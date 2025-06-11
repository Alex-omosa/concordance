use async_trait::async_trait;
use tracing::{debug, error, instrument, trace};

use crate::config::InterestDeclaration;
use crate::consumers::{RawCommand, WorkError, WorkResult, Worker};
use crate::events::publish_es_event;
use crate::natsclient::AckableMessage;
use crate::state::EntityState;
use async_nats::jetstream::Context;

use crate::eventsourcing::{Event, StateAck, StatefulCommand};

// Import aggregate implementations from separate module
use crate::aggregates::{
    AccountAggregate, InventoryAggregate, OrderAggregate, UserAggregate
};

// ============ SIMPLIFIED AGGREGATE PATTERN ============

// Aggregate trait without Box<dyn>
pub trait AggregateImpl: Send + Sync + Sized {
    const NAME: &'static str;
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError>;
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    fn apply_event(&mut self, event: Event) -> Result<StateAck, WorkError>;
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}

// Simple function to handle commands - no enum needed!
pub fn handle_aggregate_command(
    entity_name: &str,
    key: String,
    state: Option<Vec<u8>>,
    command: StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    match entity_name {
        OrderAggregate::NAME => {
            let mut agg = OrderAggregate::from_state_direct(key, state)?;
            agg.handle_command(command)
        },
        AccountAggregate::NAME => {
            let mut agg = AccountAggregate::from_state_direct(key, state)?;
            agg.handle_command(command)
        },
        UserAggregate::NAME => {
            let mut agg = UserAggregate::from_state_direct(key, state)?;
            agg.handle_command(command)
        },
        InventoryAggregate::NAME => {
            let mut agg = InventoryAggregate::from_state_direct(key, state)?;
            agg.handle_command(command)
        },
        _ => Err(WorkError::Other(format!("Unknown entity type: {}", entity_name))),
    }
}

// ============ WORKER IMPLEMENTATION ============

pub struct AggregateCommandWorker {
    pub nc: async_nats::Client,
    pub context: Context,
    pub interest: InterestDeclaration,
    pub state: EntityState,
}

impl AggregateCommandWorker {
    pub fn new(
        nc: async_nats::Client,
        context: Context,
        interest: InterestDeclaration,
        state: EntityState,
    ) -> Self {
        Self {
            nc,
            context,
            interest,
            state,
        }
    }
}

#[async_trait]
impl Worker for AggregateCommandWorker {
    type Message = RawCommand;

    #[instrument(level = "debug", skip_all, fields(entity_name = self.interest.entity_name))]
    async fn do_work(&self, mut message: AckableMessage<Self::Message>) -> WorkResult<()> {
        debug!(command = ?message.as_ref(), "Handling received command");
        
        // Step 1: Load existing state from the state store
        let state = self
            .state
            .fetch_state(
                &self.interest.role,
                &self.interest.entity_name,
                &message.key,
            )
            .await
            .map_err(|e| WorkError::Other(format!("Failed to load state: {}", e)))?;

        if let Some(ref vec) = state {
            trace!("Loaded pre-existing state - {} bytes", vec.len());
        }
        
        // Step 2: Create StatefulCommand from RawCommand
        let stateful_command = StatefulCommand {
            aggregate: self.interest.entity_name.to_string(),
            command_type: message.command_type.to_string(),
            key: message.key.to_string(),
            state: None, // We already loaded this above
            payload: serde_json::to_vec(&message.data).map_err(|e| {
                WorkError::Other(format!(
                    "Raw inbound command payload could not be converted to vec: {}",
                    e
                ))
            })?,
        };

        // Step 3: Process command directly - no container needed!
        trace!(
            "Dispatching command {} to aggregate {}",
            stateful_command.command_type,
            self.interest.entity_name
        );
        
        let outbound_events = handle_aggregate_command(
            &self.interest.entity_name,
            message.key.clone(),
            state,
            stateful_command,
        ).map_err(|e| {
            error!("Aggregate failed to handle command: {:?}", e);
            e
        })?;

        trace!("Command handler produced {} events", outbound_events.len());

        let cmd_type = message.command_type.clone();

        // Step 4: Publish all resulting events
        for evt in outbound_events {
            let evt_type = evt.event_type.clone();
            if let Err(e) = publish_es_event(&self.nc, evt).await {
                error!(
                    "Failed to publish outbound event {} in response to command {}: {:?}",
                    evt_type, cmd_type, e
                );
                // NACK the message so it can be retried
                message.nack().await;
                return Ok(());
            }
        }

        // Step 5: ACK the command only after events are published
        message.ack().await.map_err(WorkError::NatsError)?;

        Ok(())
    }
}
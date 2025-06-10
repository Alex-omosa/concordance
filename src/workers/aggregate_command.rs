use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::Arc;
use tracing::{debug, error, instrument, trace};

use crate::config::InterestDeclaration;
use crate::consumers::{RawCommand, WorkError, WorkResult, Worker};
use crate::events::publish_es_event;
use crate::natsclient::AckableMessage;
use crate::state::EntityState;
use async_nats::jetstream::Context;

// Generated types from your paste.txt
use crate::eventsourcing::{AggregateService, Event, EventList, StatefulCommand};

pub trait Aggregate: Send + Sync {
    fn from_state(key: String, state: Option<Vec<u8>>) -> Result<Box<dyn Aggregate>, WorkError>
    where
        Self: Sized;

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}

// Simple generic worker - no registry needed!
pub struct AggregateCommandWorker {
    pub nc: async_nats::Client,
    pub context: Context, // ← You have this
    pub interest: InterestDeclaration,
    pub state: EntityState, // ← Concrete type, not trait
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
        // Step 1: Load existing state from the state store (UNCHANGED)
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
        // Step 2: Create the aggregate from state - NOW GENERIC! 🎯
        let mut aggregate = match self.interest.entity_name.as_str() {
            "order" => OrderAggregate::from_state(message.key.clone(), state)?,
            "account" => AccountAggregate::from_state(message.key.clone(), state)?,
            // "user" => UserAggregate::from_state(message.key.clone(), state)?,
            // "inventory" => InventoryAggregate::from_state(message.key.clone(), state)?,
            _ => {
                return Err(WorkError::Other(format!(
                    "Unknown entity type: {}",
                    self.interest.entity_name
                )));
            }
        };

        // Step 3: Create StatefulCommand from RawCommand (UNCHANGED)
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

        // Step 4: Process command through aggregate - NOW GENERIC! 🎯
        trace!(
            "Dispatching command {} to aggregate {}",
            stateful_command.command_type,
            self.interest.entity_name
        );

        let outbound_events = aggregate.handle_command(stateful_command).map_err(|e| {
            error!("Aggregate failed to handle command: {:?}", e);
            e
        })?;

        trace!("Command handler produced {} events", outbound_events.len());

        let cmd_type = message.command_type.clone();

        // Step 5: Publish all resulting events (UNCHANGED)
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

        // Step 6: Update aggregate state in the state store - NOW GENERIC! 🎯
        let new_state = aggregate.to_state()?.unwrap_or_default();
        self.state
            .write_state(
                &self.interest.role,
                &self.interest.entity_name,
                &message.key,
                new_state,
            )
            .await
            .map_err(|e| WorkError::Other(format!("Failed to save state: {}", e)))?;

        // Step 7: ACK the command only after events are published and state is saved (UNCHANGED)
        message.ack().await.map_err(|e| WorkError::NatsError(e))?;

        Ok(())
    }
}

// Example aggregate implementations that implement the common trait

#[derive(Serialize, Deserialize)]
pub struct OrderAggregate {
    key: String,
    // ... your order-specific fields
}

impl Aggregate for OrderAggregate {
    fn from_state(key: String, state: Option<Vec<u8>>) -> Result<Box<dyn Aggregate>, WorkError> {
        // Your existing logic to create OrderAggregate from state
        let order = OrderAggregate {
            key,
            // ... deserialize state into fields
        };
        Ok(Box::new(order))
    }

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        // Your existing order command handling logic
        match command.command_type.as_str() {
            "create_order" => {
                trace!("Handling create_order command ...");
                Ok(vec![])
            } // Return empty vec for now
            "update_order" => {
                trace!("Handling update_order command ...");
                Ok(vec![])
            } // Return empty vec for now
            "cancel_order" => {
                trace!("Handling cancel_order command ...");
                Ok(vec![])
            } // Return empty vec for now
            _ => Err(WorkError::Other(format!(
                "Unknown order command: {}",
                command.command_type
            ))),
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        // Your existing logic to serialize OrderAggregate to bytes
        // serialize self into Vec<u8>
        todo!("Serialize order state")
    }
}

pub struct AccountAggregate {
    key: String,
    // ... your account-specific fields
}

impl Aggregate for AccountAggregate {
    fn from_state(key: String, state: Option<Vec<u8>>) -> Result<Box<dyn Aggregate>, WorkError> {
        let account = AccountAggregate {
            key,
            // ... deserialize state into fields
        };
        Ok(Box::new(account))
    }

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        match command.command_type.as_str() {
            "CreateAccount" => {
                println!("CreateAccount");
                Ok(vec![])
            }
            "DebitAccount" => {
                println!("DebitAccount");
                Ok(vec![])
            }
            "CreditAccount" => {
                println!("CreditAccount");
                Ok(vec![])
            }
            _ => Err(WorkError::Other(format!(
                "Unknown account command: {}",
                command.command_type
            ))),
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        // serialize account state
        todo!("Serialize account state")
    }
}

// You can add more aggregates by just implementing the Aggregate trait
pub struct UserAggregate {/* ... */}
pub struct InventoryAggregate {/* ... */}
// impl Aggregate for UserAggregate { /* ... */ }
// impl Aggregate for InventoryAggregate { /* ... */ }

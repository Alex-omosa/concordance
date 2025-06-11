// concordance/src/workers/aggregate_command.rs - Phase 3 NATS Integration

use async_trait::async_trait;
use tracing::{debug, error, info, instrument, trace, warn};
use serde::{Deserialize, Serialize};
use futures::StreamExt;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig, Stream as MessageStream},
    stream::Stream as JsStream,
    Context,
};

// Import the new dispatch system
use crate::{dispatch_command, StatefulCommand, Event, WorkError, InterestDeclaration, ActorRole};

use crate::{
    cloudevents::{ConcordanceCloudEvent, create_command_cloudevent, create_event_cloudevent},
    observability::{CorrelationContext, SpanBuilder, Metrics, OperationLogger},
};

// ============ NATS MESSAGE TYPES ============

/// Raw command as received from NATS
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawCommand {
    pub command_type: String,
    pub key: String,
    pub data: serde_json::Value,
}

/// Wrapper for messages that can be acknowledged
pub struct AckableMessage<T> {
    pub inner: T,
    pub acker: Option<async_nats::jetstream::Message>,
}

impl<T> AckableMessage<T> {
    /// Acknowledge the message
    pub async fn ack(&mut self) -> Result<(), async_nats::Error> {
        if let Some(msg) = self.acker.take() {
            msg.ack().await
        } else {
            Ok(())
        }
    }

    /// Negative acknowledge the message
    pub async fn nack(&mut self) {
        if let Some(msg) = self.acker.take() {
            if let Err(e) = msg.nak().await {
                error!("Failed to nack message: {}", e);
            }
        }
    }
}

impl<T> std::ops::Deref for AckableMessage<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for AckableMessage<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// ============ ENHANCED AGGREGATE COMMAND WORKER ============

pub struct AggregateCommandWorker {
    pub nc: async_nats::Client,
    pub context: Context,
    pub metrics: Metrics,
}

impl AggregateCommandWorker {
    pub fn new(
        nc: async_nats::Client,
        context: Context,
        metrics: Metrics,
    ) -> Self {
        AggregateCommandWorker {
            nc,
            context,
            metrics,
        }
    }

    /// Create a consumer for this aggregate's commands
    pub async fn create_consumer(&self) -> Result<MessageStream, async_nats::Error> {
        let stream_name = "CC_COMMANDS";
        let consumer_name = self.interest.consumer_name();
        let aggregate_name = &self.interest.entity_name;
        
        info!("🔧 Creating consumer '{}' for aggregate '{}'", consumer_name, aggregate_name);

        // Get the command stream
        let stream = self.context.get_stream(stream_name).await?;
        
        // Create or get the consumer
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!("Command consumer for aggregate '{}'", aggregate_name)),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(30),
                    max_deliver: 3, // Retry up to 3 times
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    filter_subject: format!("cc.commands.{}", aggregate_name),
                    ..Default::default()
                },
            )
            .await?;

        // Create the message stream
        let messages = consumer
            .stream()
            .max_messages_per_batch(10) // Process up to 10 messages at once
            .messages()
            .await?;

        info!("✅ Consumer '{}' ready for aggregate '{}'", consumer_name, aggregate_name);
        Ok(messages)
    }

    /// Start processing commands for this aggregate
    pub async fn start(&self) -> Result<(), WorkError> {
        let mut messages = self.create_consumer().await
            .map_err(|e| WorkError::Other(format!("Failed to create consumer: {}", e)))?;

        info!("🚀 Starting command processing for aggregate '{}'", self.interest.entity_name);

        while let Some(msg_result) = messages.next().await {
            match msg_result {
                Ok(msg) => {
                    // Parse the command
                    let raw_command: RawCommand = match serde_json::from_slice(&msg.payload) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            error!("Failed to parse command: {}. Acking and skipping.", e);
                            if let Err(ack_err) = msg.ack().await {
                                error!("Failed to ack invalid message: {}", ack_err);
                            }
                            continue;
                        }
                    };

                    // Create ackable message
                    let mut ackable_msg = AckableMessage {
                        inner: raw_command,
                        acker: Some(msg),
                    };

                    // Process the command
                    if let Err(e) = self.process_command(StatefulCommand {
                        aggregate: self.interest.entity_name.to_string(),
                        command_type: ackable_msg.command_type.clone(),
                        key: ackable_msg.key.clone(),
                        state: None,
                        payload: serde_json::to_vec(&ackable_msg.data).map_err(|e| {
                            WorkError::Other(format!("Failed to serialize command payload: {}", e))
                        })?,
                    }).await {
                        error!("Failed to handle command: {:?}", e);
                        ackable_msg.nack().await;
                    }
                }
                Err(e) => {
                    error!("Error receiving message: {}. Will continue...", e);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        info!("🛑 Command processing stopped for aggregate '{}'", self.interest.entity_name);
        Ok(())
    }

    #[instrument(skip(self, message), fields(
        command_type = %message.command_type,
        aggregate = %message.aggregate,
        key = %message.key
    ))]
    pub async fn process_command(&self, message: StatefulCommand) -> Result<()> {
        let start_time = std::time::Instant::now();
        
        // Create correlation context for tracing
        let correlation_ctx = CorrelationContext::new(
            message.aggregate.clone(),
            message.key.clone(),
            message.command_type.clone(),
        );

        // Create command CloudEvent with trace context
        let cloud_event = create_command_cloudevent(&message, Some(correlation_ctx.clone()))?;
        
        // Create span for command processing
        let span = SpanBuilder::new(correlation_ctx.clone()).command_processing();
        let _guard = span.enter();

        OperationLogger::command_processing(&correlation_ctx);

        // Process the command and get resulting events
        let outbound_events = self.handle_command(&message).await?;
        
        // Record metrics
        self.metrics.record_command_processed(&correlation_ctx.correlation_id);
        self.metrics.record_processing_time(
            &correlation_ctx.correlation_id,
            start_time.elapsed()
        );

        // Publish events
        for event in outbound_events {
            let event_type = event.event_type.clone();
            if let Err(e) = self.publish_event(event, &correlation_ctx).await {
                error!("Failed to publish event '{}': {:?}", event_type, e);
                self.metrics.record_error(&correlation_ctx.correlation_id, "event_publish");
                return Err(WorkError::Other(format!("Failed to publish event: {}", e)));
            }
        }

        debug!("✅ Command processing complete for key: {}", message.key);
        Ok(())
    }

    #[instrument(skip(self, command), fields(
        command_type = %command.command_type,
        aggregate = %command.aggregate,
        key = %command.key
    ))]
    async fn handle_command(&self, command: &StatefulCommand) -> Result<Vec<Event>, WorkError> {
        // Load aggregate state
        let state = self.load_state(command).await?;
        
        // Create aggregate instance
        let mut aggregate = self.create_aggregate(command, state)?;
        
        // Handle command and get events
        let events = aggregate.handle_command(command.clone())?;
        
        // Apply events to aggregate
        for event in &events {
            aggregate.apply_event(event)?;
        }
        
        // Persist new state
        if let Some(new_state) = aggregate.to_state()? {
            self.persist_state(command, new_state).await?;
        }
        
        Ok(events)
    }

    #[instrument(skip(self, event, correlation_ctx), fields(
        event_type = %event.event_type,
        aggregate = %correlation_ctx.aggregate_type,
        key = %correlation_ctx.aggregate_key
    ))]
    async fn publish_event(&self, event: Event, correlation_ctx: &CorrelationContext) -> Result<()> {
        let event_type = event.event_type.clone();
        let publish_span = SpanBuilder::new(correlation_ctx.clone()).event_publish(&event_type);
        let _guard = publish_span.enter();

        // Create event CloudEvent with trace context
        let cloud_event = create_event_cloudevent(
            &event,
            &correlation_ctx.aggregate_type,
            &correlation_ctx.aggregate_key,
            0, // Version will be updated by the event store
            Some(correlation_ctx.clone()),
        )?;

        // Serialize for NATS
        let (payload, headers) = crate::cloudevents::serialize_for_nats(&cloud_event.cloud_event)?;
        
        // Create NATS headers
        let mut nats_headers = async_nats::HeaderMap::new();
        for (key, value) in headers {
            nats_headers.insert(key, value);
        }

        // Publish to NATS
        let subject = format!("cc.events.{}", event_type);
        self.nc.publish_with_headers(subject, nats_headers, payload.into()).await?;

        // Record metrics
        self.metrics.record_event_published(&correlation_ctx.correlation_id, &event_type);
        self.metrics.record_nats_interaction(&correlation_ctx.correlation_id, "publish");

        OperationLogger::nats_interaction(
            correlation_ctx,
            "publish",
            &format!("cc.events.{}", event_type)
        );

        trace!("✅ Event '{}' published successfully", event_type);
        Ok(())
    }

    #[instrument(skip(self, command), fields(
        aggregate = %command.aggregate,
        key = %command.key
    ))]
    async fn load_state(&self, command: &StatefulCommand) -> Result<Option<Vec<u8>>, WorkError> {
        // Implementation for loading state
        Ok(None)
    }

    #[instrument(skip(self, command, state), fields(
        aggregate = %command.aggregate,
        key = %command.key
    ))]
    fn create_aggregate(&self, command: &StatefulCommand, state: Option<Vec<u8>>) -> Result<Box<dyn AggregateImpl>, WorkError> {
        // Implementation for creating aggregate
        unimplemented!()
    }

    #[instrument(skip(self, command, state), fields(
        aggregate = %command.aggregate,
        key = %command.key
    ))]
    async fn persist_state(&self, command: &StatefulCommand, state: Vec<u8>) -> Result<(), WorkError> {
        // Implementation for persisting state
        Ok(())
    }
}

// Required trait for aggregate implementation
pub trait AggregateImpl: Send + Sync {
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    fn apply_event(&mut self, event: &Event) -> Result<(), WorkError>;
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}

// ============ CONVENIENCE FUNCTIONS ============

/// Start a command worker for a specific aggregate
pub async fn start_aggregate_worker(
    nats_client: async_nats::Client,
    js_context: Context,
    aggregate_name: &str,
) -> Result<(), WorkError> {
    let interest = InterestDeclaration {
        actor_id: aggregate_name.to_string(),
        key_field: "key".to_string(), // Standard key field
        entity_name: aggregate_name.to_string(),
        role: ActorRole::Aggregate,
        interest: crate::ActorInterest::AggregateStream(aggregate_name.to_string()),
        interest_constraint: crate::InterestConstraint::Commands,
    };

    let worker = AggregateCommandWorker::new(nats_client, js_context, Metrics::new());
    worker.start().await
}

// ============ TESTING HELPERS ============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_command_parsing() {
        let json = r#"
        {
            "command_type": "create_order",
            "key": "order-123",
            "data": {
                "customer_id": "cust-456",
                "total": 99.99
            }
        }"#;

        let cmd: RawCommand = serde_json::from_str(json).unwrap();
        assert_eq!(cmd.command_type, "create_order");
        assert_eq!(cmd.key, "order-123");
        assert_eq!(cmd.data["customer_id"], "cust-456");
    }
}
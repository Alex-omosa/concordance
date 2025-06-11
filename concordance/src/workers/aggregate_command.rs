// concordance/src/workers/aggregate_command.rs - Phase 3 NATS Integration

use async_trait::async_trait;
use tracing::{debug, error, info, instrument, trace};
use serde::{Deserialize, Serialize};
use futures::StreamExt;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig, Stream as MessageStream},
    stream::Stream as JsStream,
    Context,
};

// Import the new dispatch system
use crate::{dispatch_command, StatefulCommand, Event, WorkError, InterestDeclaration, ActorRole};

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
    pub interest: InterestDeclaration,
}

impl AggregateCommandWorker {
    pub fn new(
        nc: async_nats::Client,
        context: Context,
        interest: InterestDeclaration,
    ) -> Self {
        Self {
            nc,
            context,
            interest,
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
                    if let Err(e) = self.handle_command(&mut ackable_msg).await {
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

    /// Handle a single command using the new dispatch system
    #[instrument(level = "debug", skip_all, fields(entity_name = self.interest.entity_name))]
    async fn handle_command(&self, message: &mut AckableMessage<RawCommand>) -> Result<(), WorkError> {
        debug!("📨 Handling command: {} for key: {}", message.command_type, message.key);
        
        // Step 1: Load existing state (simplified for Phase 3 - no state persistence yet)
        let state: Option<Vec<u8>> = None; // TODO: Integrate with state store in Phase 3.1
        
        // Step 2: Create StatefulCommand from RawCommand
        let stateful_command = StatefulCommand {
            aggregate: self.interest.entity_name.to_string(),
            command_type: message.command_type.to_string(),
            key: message.key.to_string(),
            state: None,
            payload: serde_json::to_vec(&message.data).map_err(|e| {
                WorkError::Other(format!("Failed to serialize command payload: {}", e))
            })?,
        };

        // Step 3: 🎉 USE THE NEW DISPATCH SYSTEM!
        trace!("🚀 Dispatching command {} to aggregate {}", 
               stateful_command.command_type, 
               self.interest.entity_name);
        
        let outbound_events = dispatch_command(
            &self.interest.entity_name,
            message.key.clone(),
            state,
            stateful_command,
        ).map_err(|e| {
            error!("Aggregate dispatch failed: {:?}", e);
            WorkError::Other(format!("Dispatch failed: {}", e))
        })?;

        trace!("✅ Command produced {} events", outbound_events.len());

        // Step 4: Publish all resulting events to NATS
        for event in outbound_events {
            let event_type = event.event_type.clone();
            if let Err(e) = self.publish_event(event).await {
                error!("Failed to publish event '{}': {:?}", event_type, e);
                // NACK the command so it can be retried
                return Err(WorkError::Other(format!("Failed to publish event: {}", e)));
            }
        }

        // Step 5: ACK the command only after all events are published
        message.ack().await.map_err(|e| {
            WorkError::Other(format!("Failed to ack message: {}", e))
        })?;

        debug!("✅ Command processing complete for key: {}", message.key);
        Ok(())
    }

    /// Publish an event to the NATS events stream
    async fn publish_event(&self, event: Event) -> Result<(), async_nats::Error> {
        let subject = format!("cc.events.{}", event.event_type);
        
        // Convert to CloudEvent format for better compatibility
        let cloud_event = serde_json::json!({
            "specversion": "1.0",
            "type": event.event_type,
            "source": "concordance",
            "id": uuid::Uuid::new_v4().to_string(),
            "time": chrono::Utc::now().to_rfc3339(),
            "datacontenttype": "application/json",
            "data": serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap_or_default(),
            "subject": event.stream,
        });

        let payload = serde_json::to_vec(&cloud_event)
            .map_err(|e| async_nats::Error::new(
                async_nats::error::ErrorKind::Other, 
                Some(format!("Failed to serialize event: {}", e))
            ))?;

        trace!("📤 Publishing event '{}' to subject '{}'", event.event_type, subject);
        
        self.nc.publish(subject, payload.into()).await?;
        
        trace!("✅ Event '{}' published successfully", event.event_type);
        Ok(())
    }
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

    let worker = AggregateCommandWorker::new(nats_client, js_context, interest);
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
// examples/user-app/src/bin/worker.rs - Fixed Standalone Command Worker

use concordance::{
    Aggregate, AggregateImpl, BaseConfiguration, Event, StatefulCommand, WorkError,
};
use serde::{Deserialize, Serialize};
use tracing::{info, error, Level};
use futures::StreamExt;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig},
    Context, AckKind,
};

// Import your business aggregates (same as before)
#[derive(Debug, Serialize, Deserialize, Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate {
    pub key: String,
    pub customer_id: Option<String>,
    pub total: f64,
    pub status: OrderStatus,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum OrderStatus {
    New,
    Created,
    Confirmed,
    Shipped,
    Delivered,
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderItem {
    pub name: String,
    pub quantity: u32,
    pub price: f64,
}

impl AggregateImpl for OrderAggregate {
    const NAME: &'static str = "order";

    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
        match state {
            Some(bytes) => {
                info!("🔄 Restoring OrderAggregate from {} bytes", bytes.len());
                serde_json::from_slice(&bytes)
                    .map_err(|e| WorkError::Other(format!("Deserialize failed: {}", e)))
            }
            None => {
                info!("✨ Creating new OrderAggregate: {}", key);
                Ok(OrderAggregate {
                    key,
                    customer_id: None,
                    total: 0.0,
                    status: OrderStatus::New,
                    items: Vec::new(),
                })
            }
        }
    }

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        info!("📨 OrderAggregate[{}] handling: {}", self.key, command.command_type);
        
        match command.command_type.as_str() {
            "create_order" => {
                let payload: CreateOrderPayload = serde_json::from_slice(&command.payload)
                    .map_err(|e| WorkError::Other(format!("Invalid payload: {}", e)))?;

                if self.status != OrderStatus::New {
                    return Err(WorkError::Other("Order already exists".to_string()));
                }

                self.customer_id = Some(payload.customer_id.clone());
                self.total = payload.total;
                self.items = payload.items.clone();
                self.status = OrderStatus::Created;

                info!("✅ Order created! Customer: {}, Total: ${:.2}", payload.customer_id, payload.total);

                Ok(vec![Event {
                    event_type: "order_created".to_string(),
                    payload: serde_json::to_vec(&OrderCreatedEvent {
                        order_id: self.key.clone(),
                        customer_id: payload.customer_id,
                        total: payload.total,
                        items: payload.items,
                    }).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            "confirm_order" => {
                if self.status != OrderStatus::Created {
                    return Err(WorkError::Other("Order must be created first".to_string()));
                }

                self.status = OrderStatus::Confirmed;
                info!("✅ Order {} confirmed!", self.key);

                Ok(vec![Event {
                    event_type: "order_confirmed".to_string(),
                    payload: serde_json::to_vec(&OrderConfirmedEvent {
                        order_id: self.key.clone(),
                    }).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            _ => Err(WorkError::Other(format!("Unknown command: {}", command.command_type))),
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        Ok(Some(serde_json::to_vec(self).unwrap()))
    }
}

// Event payloads
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderPayload {
    pub customer_id: String,
    pub total: f64,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderCreatedEvent {
    pub order_id: String,
    pub customer_id: String,
    pub total: f64,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderConfirmedEvent {
    pub order_id: String,
}

// Command from NATS
#[derive(Debug, Serialize, Deserialize)]
pub struct RawCommand {
    pub command_type: String,
    pub key: String,
    pub data: serde_json::Value,
}

// ============ STANDALONE WORKER IMPLEMENTATION ============

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        // .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    info!("🚀 Starting Concordance Command Worker");
    info!("📦 Registered aggregates: {:?}", concordance::get_registered_aggregates());

    // Get configuration from environment
    let config = BaseConfiguration::default();
    info!("📡 Connecting to NATS at: {}", config.nats_url);

    // Connect to NATS and get JetStream context
    let nc = config.get_nats_connection().await?;
    let js = config.get_jetstream_context().await?;

    // Ensure streams exist
    ensure_streams(&js).await?;

    // Start workers for all registered aggregates
    let aggregates = concordance::get_registered_aggregates();
    let mut handles = vec![];

    for aggregate_name in aggregates {
        let worker_nc = nc.clone();
        let worker_js = js.clone();
        let agg_name = aggregate_name.to_string();
        
        let handle = tokio::spawn(async move {
            info!("🏃 Starting worker for aggregate: {}", agg_name);
            if let Err(e) = run_aggregate_worker(worker_nc, worker_js, &agg_name).await {
                error!("❌ Worker for {} failed: {}", agg_name, e);
            }
        });
        
        handles.push(handle);
    }

    info!("✅ All workers started. Press Ctrl+C to stop.");

    // Wait for all workers (they should run forever)
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn ensure_streams(js: &Context) -> Result<(), Box<dyn std::error::Error>> {
    use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};

    info!("🔧 Ensuring NATS streams exist...");

    // Events stream
    js.get_or_create_stream(StreamConfig {
        name: "CC_EVENTS".to_string(),
        description: Some("Concordance events".to_string()),
        subjects: vec!["cc.events.*".to_string()],
        retention: RetentionPolicy::Limits,
        storage: StorageType::File,
        ..Default::default()
    }).await?;

    // Commands stream
    js.get_or_create_stream(StreamConfig {
        name: "CC_COMMANDS".to_string(),
        description: Some("Concordance commands".to_string()),
        subjects: vec!["cc.commands.*".to_string()],
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        ..Default::default()
    }).await?;

    info!("✅ NATS streams ready");
    Ok(())
}

async fn run_aggregate_worker(
    nc: async_nats::Client,
    js: Context,
    aggregate_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_name = format!("AGG_CMD_{}", aggregate_name);
    let filter_subject = format!("cc.commands.{}", aggregate_name);

    info!("🔧 Creating consumer '{}' for aggregate '{}'", consumer_name, aggregate_name);

    // Get command stream
    let stream = js.get_stream("CC_COMMANDS").await?;
    
    // Create consumer
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            PullConfig {
                durable_name: Some(consumer_name.clone()),
                name: Some(consumer_name.clone()),
                description: Some(format!("Commands for {}", aggregate_name)),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(30),
                max_deliver: 3,
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                filter_subject,
                ..Default::default()
            },
        )
        .await?;

    let mut messages = consumer.messages().await?;
    info!("✅ Worker for '{}' ready - waiting for commands...", aggregate_name);

    while let Some(msg) = messages.next().await {
        match msg {
            Ok(message) => {
                // Parse command
                let raw_command: RawCommand = match serde_json::from_slice(&message.payload) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        error!("❌ Failed to parse command: {}. Acking and skipping.", e);
                        let _ = message.ack().await;
                        continue;
                    }
                };

                info!("📨 Processing command: {} for key: {}", raw_command.command_type, raw_command.key);

                // Create stateful command
                let stateful_command = StatefulCommand {
                    aggregate: aggregate_name.to_string(),
                    command_type: raw_command.command_type.to_string(),
                    key: raw_command.key.to_string(),
                    state: None, // TODO: Load from state store
                    payload: serde_json::to_vec(&raw_command.data).unwrap(),
                };

                // Dispatch to aggregate using the registry system! 🎉
                match concordance::dispatch_command(
                    aggregate_name,
                    raw_command.key.clone(),
                    None, // TODO: Load state
                    stateful_command,
                ) {
                    Ok(events) => {
                        info!("✅ Command produced {} events", events.len());

                        // Publish events
                        let mut success = true;
                        for event in events {
                            if let Err(e) = publish_event(&nc, event).await {
                                error!("❌ Failed to publish event: {}", e);
                                success = false;
                                break;
                            }
                        }

                        if success {
                            // Ack the command only if all events were published successfully
                            if let Err(e) = message.ack().await {
                                error!("❌ Failed to ack command: {}", e);
                            }
                        } else {
                            // Nack the command if event publishing failed
                            if let Err(e) = message.ack_with(AckKind::Nak(None)).await {
                                error!("❌ Failed to nack command: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("❌ Command processing failed: {:?}", e);
                        // Nack the command so it can be retried
                        if let Err(nack_err) = message.ack_with(AckKind::Nak(None)).await {
                            error!("❌ Failed to nack command: {}", nack_err);
                        }
                    }
                }
            }
            Err(e) => {
                error!("❌ Error receiving message: {}. Will continue...", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    Ok(())
}

async fn publish_event(nc: &async_nats::Client, event: Event) -> Result<(), Box<dyn std::error::Error>> {
    let subject = format!("cc.events.{}", event.event_type);
    
    let cloud_event = serde_json::json!({
        "specversion": "1.0",
        "type": event.event_type,
        "source": "concordance-worker",
        "id": uuid::Uuid::new_v4().to_string(),
        "time": chrono::Utc::now().to_rfc3339(),
        "datacontenttype": "application/json",
        "data": serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap_or_default(),
    });

    let payload = serde_json::to_vec(&cloud_event)?;
    nc.publish(subject, payload.into()).await?;
    
    info!("📤 Published event: {}", event.event_type);
    Ok(())
}
// examples/user-app/src/bin/worker.rs - Enhanced Worker with State Persistence

use concordance::{
    Aggregate, AggregateImpl, BaseConfiguration, Event, StatefulCommand, WorkError, EntityState,
};
use serde::{Deserialize, Serialize};
use tracing::{info, error, warn, Level};
use futures::StreamExt;
use async_nats::jetstream::{
    consumer::pull::{Config as PullConfig},
    Context, AckKind,
};

// ============ ENHANCED BUSINESS AGGREGATES ============

#[derive(Debug, Serialize, Deserialize, Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate {
    pub key: String,
    pub customer_id: Option<String>,
    pub total: f64,
    pub status: OrderStatus,
    pub items: Vec<OrderItem>,
    pub version: u32, // For optimistic concurrency
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
                info!("Restoring OrderAggregate from {} bytes", bytes.len());
                serde_json::from_slice(&bytes)
                    .map_err(|e| WorkError::Other(format!("Deserialize failed: {}", e)))
            }
            None => {
                info!("Creating new OrderAggregate: {}", key);
                Ok(OrderAggregate {
                    key,
                    customer_id: None,
                    total: 0.0,
                    status: OrderStatus::New,
                    items: Vec::new(),
                    version: 0,
                })
            }
        }
    }

    // ENHANCED: handle_command is now READ-ONLY and doesn't modify state
    fn handle_command(&self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        info!("OrderAggregate[{}] v{} handling: {}", self.key, self.version, command.command_type);
        
        match command.command_type.as_str() {
            "create_order" => {
                // Business rule validation (read-only)
                if self.status != OrderStatus::New {
                    return Err(WorkError::Other("Order already exists".to_string()));
                }

                let payload: CreateOrderPayload = serde_json::from_slice(&command.payload)
                    .map_err(|e| WorkError::Other(format!("Invalid payload: {}", e)))?;

                // Generate event data
                let event_data = OrderCreatedEvent {
                    order_id: self.key.clone(),
                    customer_id: payload.customer_id,
                    total: payload.total,
                    items: payload.items,
                    version: self.version + 1,
                };

                Ok(vec![Event {
                    event_type: "order_created".to_string(),
                    payload: serde_json::to_vec(&event_data).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            "confirm_order" => {
                if self.status != OrderStatus::Created {
                    return Err(WorkError::Other("Order must be created first".to_string()));
                }

                let event_data = OrderConfirmedEvent {
                    order_id: self.key.clone(),
                    version: self.version + 1,
                };

                Ok(vec![Event {
                    event_type: "order_confirmed".to_string(),
                    payload: serde_json::to_vec(&event_data).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            _ => Err(WorkError::Other(format!("Unknown command: {}", command.command_type))),
        }
    }

    // NEW: apply_event modifies the aggregate state based on events
    fn apply_event(&mut self, event: &Event) -> Result<(), WorkError> {
        info!("OrderAggregate[{}] applying event: {}", self.key, event.event_type);
        
        match event.event_type.as_str() {
            "order_created" => {
                let event_data: OrderCreatedEvent = serde_json::from_slice(&event.payload)
                    .map_err(|e| WorkError::Other(format!("Invalid event payload: {}", e)))?;

                // Apply the state changes
                self.customer_id = Some(event_data.customer_id);
                self.total = event_data.total;
                self.items = event_data.items;
                self.status = OrderStatus::Created;
                self.version = event_data.version;

                info!("Order {} created with total ${:.2}", self.key, self.total);
            }
            "order_confirmed" => {
                let event_data: OrderConfirmedEvent = serde_json::from_slice(&event.payload)
                    .map_err(|e| WorkError::Other(format!("Invalid event payload: {}", e)))?;

                // Apply the state changes
                self.status = OrderStatus::Confirmed;
                self.version = event_data.version;

                info!("Order {} confirmed", self.key);
            }
            _ => {
                warn!("Unknown event type: {}", event.event_type);
                return Err(WorkError::Other(format!("Unknown event: {}", event.event_type)));
            }
        }
        
        Ok(())
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        Ok(Some(serde_json::to_vec(self).unwrap()))
    }
}

// ============ EVENT PAYLOADS ============

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
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderConfirmedEvent {
    pub order_id: String,
    pub version: u32,
}

// Command from NATS
#[derive(Debug, Serialize, Deserialize)]
pub struct RawCommand {
    pub command_type: String,
    pub key: String,
    pub data: serde_json::Value,
}

// ============ ENHANCED WORKER WITH STATE PERSISTENCE ============

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    info!("Starting Enhanced Concordance Worker with State Persistence");
    info!("Registered aggregates: {:?}", concordance::get_registered_aggregates());

    // Get configuration from environment
    let config = BaseConfiguration::default();
    info!("Connecting to NATS at: {}", config.nats_url);

    // Connect to NATS and get JetStream context
    let nc = config.get_nats_connection().await?;
    let js = config.get_jetstream_context().await?;

    // Initialize state persistence
    let state = EntityState::new_from_context(&js).await?;
    
    // Health check the state store
    state.health_check().await?;
    info!("State persistence initialized and healthy");

    // Ensure streams exist
    ensure_streams(&js).await?;

    // Start workers for all registered aggregates
    let aggregates = concordance::get_registered_aggregates();
    let mut handles = vec![];

    for aggregate_name in aggregates {
        let worker_nc = nc.clone();
        let worker_js = js.clone();
        let worker_state = state.clone();
        let agg_name = aggregate_name.to_string();
        
        let handle = tokio::spawn(async move {
            info!("Starting enhanced worker for aggregate: {}", agg_name);
            if let Err(e) = run_enhanced_worker(worker_nc, worker_js, worker_state, &agg_name).await {
                error!("Enhanced worker for {} failed: {}", agg_name, e);
            }
        });
        
        handles.push(handle);
    }

    info!("All enhanced workers started. Press Ctrl+C to stop.");

    // Wait for all workers (they should run forever)
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn ensure_streams(js: &Context) -> Result<(), Box<dyn std::error::Error>> {
    use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};

    info!("Ensuring NATS streams exist...");

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

    info!("NATS streams ready");
    Ok(())
}

async fn run_enhanced_worker(
    nc: async_nats::Client,
    js: Context,
    state: EntityState,
    aggregate_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_name = format!("AGG_CMD_{}", aggregate_name);
    let filter_subject = format!("cc.commands.{}", aggregate_name);

    info!("Creating enhanced consumer '{}' for aggregate '{}'", consumer_name, aggregate_name);

    // Get command stream
    let stream = js.get_stream("CC_COMMANDS").await?;
    
    // Create consumer
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            PullConfig {
                durable_name: Some(consumer_name.clone()),
                name: Some(consumer_name.clone()),
                description: Some(format!("Enhanced commands for {}", aggregate_name)),
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
    info!("Enhanced worker for '{}' ready - waiting for commands...", aggregate_name);

    while let Some(msg) = messages.next().await {
        match msg {
            Ok(message) => {
                // Parse command
                let raw_command: RawCommand = match serde_json::from_slice(&message.payload) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        error!("Failed to parse command: {}. Acking and skipping.", e);
                        let _ = message.ack().await;
                        continue;
                    }
                };

                info!("Processing command: {} for key: {}", raw_command.command_type, raw_command.key);

                // ENHANCED FLOW: Load state, process command with events, save state
                match process_command_with_state(&state, &nc, aggregate_name, raw_command).await {
                    Ok(_) => {
                        // Ack the command only if everything succeeded
                        if let Err(e) = message.ack().await {
                            error!("Failed to ack command: {}", e);
                        } else {
                            info!("Command processed successfully");
                        }
                    }
                    Err(e) => {
                        error!("Command processing failed: {:?}", e);
                        // Nack the command so it can be retried
                        if let Err(nack_err) = message.ack_with(AckKind::Nak(None)).await {
                            error!("Failed to nack command: {}", nack_err);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error receiving message: {}. Will continue...", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    Ok(())
}

// ENHANCED: Full command processing with state persistence
async fn process_command_with_state(
    state: &EntityState,
    nc: &async_nats::Client,
    aggregate_name: &str,
    raw_command: RawCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = &raw_command.key;
    
    // Step 1: Load current state from NATS KV
    info!("Loading state for {}.{}", aggregate_name, key);
    let current_state = state.fetch_state(aggregate_name, key).await?;
    
    if let Some(ref state_bytes) = current_state {
        info!("Found existing state ({} bytes)", state_bytes.len());
    } else {
        info!("No existing state found - will create new aggregate");
    }

    // Step 2: Create StatefulCommand
    let stateful_command = StatefulCommand {
        aggregate: aggregate_name.to_string(),
        command_type: raw_command.command_type,
        key: key.clone(),
        state: None, // Not used in enhanced flow
        payload: serde_json::to_vec(&raw_command.data)?,
    };

    // Step 3: Use enhanced dispatch that handles command → events → state update
    info!("Dispatching command using enhanced flow");
    let (events, new_state) = concordance::dispatch_command(
        aggregate_name,
        key.clone(),
        current_state,
        stateful_command,
    ).map_err(|e| format!("Enhanced dispatch failed: {}", e))?;

    info!("Enhanced dispatch produced {} events", events.len());

    // Step 4: Persist the new state to NATS KV
    if let Some(state_bytes) = new_state {
        info!("Saving updated state ({} bytes)", state_bytes.len());
        let revision = state.write_state(aggregate_name, key, state_bytes).await?;
        info!("State saved with revision: {}", revision);
    } else {
        info!("No state to persist (aggregate may have been deleted)");
    }

    // Step 5: Publish all events to NATS
    for event in events {
        publish_event(nc, event).await?;
    }

    Ok(())
}

async fn publish_event(nc: &async_nats::Client, event: Event) -> Result<(), Box<dyn std::error::Error>> {
    let subject = format!("cc.events.{}", event.event_type);
    
    let cloud_event = serde_json::json!({
        "specversion": "1.0",
        "type": event.event_type,
        "source": "concordance-enhanced-worker",
        "id": uuid::Uuid::new_v4().to_string(),
        "time": chrono::Utc::now().to_rfc3339(),
        "datacontenttype": "application/json",
        "data": serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap_or_default(),
    });

    let payload = serde_json::to_vec(&cloud_event)?;
    nc.publish(subject, payload.into()).await?;
    
    info!("Published event: {}", event.event_type);
    Ok(())
}
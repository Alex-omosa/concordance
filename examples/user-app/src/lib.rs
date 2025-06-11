// examples/user-app/src/lib.rs - Clean Aggregate with Separate Command/Event Modules and Observability

pub use concordance::*;

// ============ MODULES ============
pub mod commands;
pub mod events;

// Re-export for convenience
pub use commands::*;
pub use events::*;

// ============ BUSINESS AGGREGATES ============

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate {
    pub key: String,
    pub customer_id: Option<String>,
    pub total: f64,
    pub status: OrderStatus,
    pub items: Vec<OrderItem>,
    pub version: u32,
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

// ============ CLEAN AGGREGATE IMPLEMENTATION WITH OBSERVABILITY ============

impl AggregateImpl for OrderAggregate {
    const NAME: &'static str = "order";

    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
        let span = tracing::debug_span!(
            "aggregate.from_state",
            aggregate.type = "order",
            aggregate.key = %key,
            state.exists = state.is_some(),
            state.size_bytes = state.as_ref().map(|s| s.len())
        );
        
        let _guard = span.enter();
        
        match state {
            Some(bytes) => {
                tracing::debug!("Deserializing order aggregate from state");
                serde_json::from_slice(&bytes)
                    .map_err(|e| {
                        tracing::error!(
                            error = %e,
                            "Failed to deserialize order state"
                        );
                        WorkError::Other(format!("Deserialize failed: {}", e))
                    })
            }
            None => {
                tracing::debug!("Creating new order aggregate");
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

    /// 🎉 COMMAND HANDLING - Routes commands with match in lib.rs
    fn handle_command(&self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        let span = tracing::info_span!(
            "aggregate.handle_command",
            aggregate.type = "order",
            aggregate.key = %self.key,
            aggregate.version = %self.version,
            command.type = %command.command_type,
        );
        
        let _guard = span.enter();
        
        tracing::debug!(
            aggregate.status = ?self.status,
            "Processing command"
        );

        // Route commands to specific handlers
        let result = match command.command_type.as_str() {
            "create_order" => commands::handle_create_order(self, &command),
            "confirm_order" => commands::handle_confirm_order(self, &command),
            "ship_order" => commands::handle_ship_order(self, &command),
            "cancel_order" => commands::handle_cancel_order(self, &command),
            _ => {
                tracing::warn!(
                    command.type = %command.command_type,
                    "Unknown command type"
                );
                Err(WorkError::Other(format!(
                    "Unknown order command: {}",
                    command.command_type
                )))
            }
        };
        
        match &result {
            Ok(events) => {
                tracing::debug!(
                    events.count = events.len(),
                    events.types = ?events.iter().map(|e| &e.event_type).collect::<Vec<_>>(),
                    "Command handled successfully"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Command handling failed"
                );
            }
        }
        
        result
    }

    /// 🎉 EVENT APPLICATION - Routes events with match in lib.rs
    fn apply_event(&mut self, event: &Event) -> Result<(), WorkError> {
        let span = tracing::info_span!(
            "aggregate.apply_event",
            aggregate.type = "order",
            aggregate.key = %self.key,
            aggregate.version = %self.version,
            event.type = %event.event_type,
        );
        
        let _guard = span.enter();
        
        let previous_status = self.status.clone();
        let previous_version = self.version;
        
        tracing::debug!(
            aggregate.status = ?self.status,
            "Applying event to aggregate"
        );

        // Route events to specific handlers
        let result = match event.event_type.as_str() {
            "order_created" => events::apply_order_created(self, event),
            "order_confirmed" => events::apply_order_confirmed(self, event),
            "order_shipped" => events::apply_order_shipped(self, event),
            "order_delivered" => events::apply_order_delivered(self, event),
            "order_cancelled" => events::apply_order_cancelled(self, event),
            _ => {
                tracing::warn!(
                    event.type = %event.event_type,
                    "Unknown event type"
                );
                Err(WorkError::Other(format!(
                    "Unknown order event: {}",
                    event.event_type
                )))
            }
        };
        
        match &result {
            Ok(_) => {
                tracing::debug!(
                    state.transition.from_status = ?previous_status,
                    state.transition.to_status = ?self.status,
                    state.transition.from_version = %previous_version,
                    state.transition.to_version = %self.version,
                    "Event applied successfully"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Event application failed"
                );
            }
        }
        
        result
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        let span = tracing::debug_span!(
            "aggregate.to_state",
            aggregate.type = "order",
            aggregate.key = %self.key,
            aggregate.version = %self.version,
        );
        
        let _guard = span.enter();
        
        serde_json::to_vec(self)
            .map(|bytes| {
                tracing::debug!(
                    state.size_bytes = bytes.len(),
                    "Serialized aggregate state"
                );
                Some(bytes)
            })
            .map_err(|e| {
                tracing::error!(
                    error = %e,
                    "Failed to serialize aggregate state"
                );
                WorkError::Other(format!("Failed to serialize state: {}", e))
            })
    }
}

// ============ TEST HELPERS ============

#[cfg(test)]
pub mod test_helpers {
    use super::*;

    pub fn create_test_order_data(customer_id: &str, total: f64, item_name: &str) -> serde_json::Value {
        serde_json::json!({
            "customer_id": customer_id,
            "total": total,
            "items": [{
                "name": item_name,
                "quantity": 1,
                "price": total
            }]
        })
    }

    pub fn create_ship_order_data(tracking: &str, carrier: &str) -> serde_json::Value {
        serde_json::json!({
            "tracking_number": tracking,
            "carrier": carrier,
            "estimated_delivery": "2024-01-15"
        })
    }

    pub fn create_cancel_order_data(reason: &str, refund: Option<f64>) -> serde_json::Value {
        serde_json::json!({
            "reason": reason,
            "refund_amount": refund
        })
    }

    pub async fn create_test_provider() -> Option<ConcordanceProvider> {
        match ConcordanceProvider::new().await {
            Ok(provider) => {
                tracing::info!("✅ Test provider created successfully");
                Some(provider)
            }
            Err(e) => {
                tracing::info!("⚠️  Skipping test - NATS not available: {}", e);
                None
            }
        }
    }
}
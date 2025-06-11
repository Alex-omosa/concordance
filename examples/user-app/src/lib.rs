// examples/user-app/src/lib.rs - Clean Aggregate with Separate Command/Event Modules

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

// ============ CLEAN AGGREGATE IMPLEMENTATION ============

impl AggregateImpl for OrderAggregate {
    const NAME: &'static str = "order";

    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
        match state {
            Some(bytes) => {
                tracing::debug!("Restoring OrderAggregate from {} bytes", bytes.len());
                serde_json::from_slice(&bytes)
                    .map_err(|e| WorkError::Other(format!("Deserialize failed: {}", e)))
            }
            None => {
                tracing::debug!("Creating new OrderAggregate: {}", key);
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
        tracing::debug!(
            "OrderAggregate[{}] v{} handling command: {}",
            self.key,
            self.version,
            command.command_type
        );

        // Route commands to specific handlers
        match command.command_type.as_str() {
            "create_order" => commands::handle_create_order(self, &command),
            "confirm_order" => commands::handle_confirm_order(self, &command),
            "ship_order" => commands::handle_ship_order(self, &command),
            "cancel_order" => commands::handle_cancel_order(self, &command),
            _ => Err(WorkError::Other(format!(
                "Unknown order command: {}",
                command.command_type
            ))),
        }
    }

    /// 🎉 EVENT APPLICATION - Routes events with match in lib.rs
    fn apply_event(&mut self, event: &Event) -> Result<(), WorkError> {
        tracing::debug!(
            "OrderAggregate[{}] applying event: {}",
            self.key,
            event.event_type
        );

        // Route events to specific handlers
        match event.event_type.as_str() {
            "order_created" => events::apply_order_created(self, event),
            "order_confirmed" => events::apply_order_confirmed(self, event),
            "order_shipped" => events::apply_order_shipped(self, event),
            "order_delivered" => events::apply_order_delivered(self, event),
            "order_cancelled" => events::apply_order_cancelled(self, event),
            _ => {
                tracing::warn!("Unknown event type: {}", event.event_type);
                Err(WorkError::Other(format!(
                    "Unknown order event: {}",
                    event.event_type
                )))
            }
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        serde_json::to_vec(self)
            .map(Some)
            .map_err(|e| WorkError::Other(format!("Failed to serialize state: {}", e)))
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
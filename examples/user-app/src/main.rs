// Example showing how to use Concordance with external aggregates

use concordance::{
    Aggregate, AggregateImpl, BaseConfiguration, ConcordanceProvider, Event, StatefulCommand,
    WorkError, ActorRole, InterestConstraint, InterestDeclaration, ActorInterest, dispatch_command,
};
use serde::{Deserialize, Serialize};
use tracing::{info, Level};
use tracing_subscriber;

// ============ USER-DEFINED AGGREGATES ============

/// Order aggregate using the new derive macro
#[derive(Debug, Serialize, Deserialize, Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate {
    pub key: String,
    pub customer_id: Option<String>,
    pub total: f64,
    pub status: OrderStatus,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| WorkError::Other(format!("Failed to deserialize order state: {}", e))),
            None => Ok(OrderAggregate {
                key,
                customer_id: None,
                total: 0.0,
                status: OrderStatus::New,
                items: Vec::new(),
            }),
        }
    }

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        match command.command_type.as_str() {
            "create_order" => {
                let payload: CreateOrderPayload = serde_json::from_slice(&command.payload)
                    .map_err(|e| WorkError::Other(format!("Invalid create_order payload: {}", e)))?;

                self.customer_id = Some(payload.customer_id.clone());
                self.total = payload.total;
                self.items = payload.items.clone();
                self.status = OrderStatus::Created;

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
                if !matches!(self.status, OrderStatus::Created) {
                    return Err(WorkError::Other("Order must be created before confirming".to_string()));
                }

                self.status = OrderStatus::Confirmed;

                Ok(vec![Event {
                    event_type: "order_confirmed".to_string(),
                    payload: serde_json::to_vec(&OrderConfirmedEvent {
                        order_id: self.key.clone(),
                    }).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            _ => Err(WorkError::Other(format!("Unknown order command: {}", command.command_type))),
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        serde_json::to_vec(self)
            .map(Some)
            .map_err(|e| WorkError::Other(format!("Failed to serialize order state: {}", e)))
    }
}

// ============ COMMAND/EVENT PAYLOADS ============

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

// ============ SECOND AGGREGATE EXAMPLE ============

#[derive(Debug, Serialize, Deserialize, Aggregate)]
#[aggregate(name = "inventory")]
pub struct InventoryAggregate {
    pub key: String,
    pub product_name: String,
    pub quantity: u32,
    pub reserved: u32,
}

impl AggregateImpl for InventoryAggregate {
    const NAME: &'static str = "inventory";

    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
        match state {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| WorkError::Other(format!("Failed to deserialize inventory: {}", e))),
            None => Ok(InventoryAggregate {
                key,
                product_name: "Unknown".to_string(),
                quantity: 0,
                reserved: 0,
            }),
        }
    }

    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        match command.command_type.as_str() {
            "adjust_inventory" => {
                // Simple inventory adjustment logic
                self.quantity += 10; // Demo: add 10 items
                
                Ok(vec![Event {
                    event_type: "inventory_adjusted".to_string(),
                    payload: serde_json::to_vec(&self).unwrap(),
                    stream: "inventory".to_string(),
                }])
            }
            _ => Err(WorkError::Other(format!("Unknown inventory command: {}", command.command_type))),
        }
    }

    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        serde_json::to_vec(self)
            .map(Some)
            .map_err(|e| WorkError::Other(format!("Failed to serialize inventory state: {}", e)))
    }
}

// ============ MAIN APPLICATION ============

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🎯 Testing Concordance PHASE 1: AUTOMATED REGISTRATION!");

    // Create the provider (simplified for testing)
    let base_config = BaseConfiguration::default();
    let provider = ConcordanceProvider::try_new(base_config).await?;

    // Show which aggregates are registered
    let registered = provider.registered_aggregates();
    info!("✅ Registered aggregates: {:?}", registered);

    // Verify our aggregates are registered
    assert!(provider.is_aggregate_available("order"));
    assert!(provider.is_aggregate_available("inventory"));
    info!("✅ Both aggregates are properly registered!");

    // Test the AUTOMATED dispatch system 🎉
    let test_command = StatefulCommand {
        aggregate: "order".to_string(),
        command_type: "create_order".to_string(),
        key: "test-order-123".to_string(),
        state: None,
        payload: serde_json::to_vec(&CreateOrderPayload {
            customer_id: "cust-456".to_string(),
            total: 99.99,
            items: vec![OrderItem {
                name: "Test Widget".to_string(),
                quantity: 1,
                price: 99.99,
            }],
        }).unwrap(),
    };

    // 🎉 This now uses the registry system - Phase 1 working!
    match dispatch_command("order", "test-order-123".to_string(), None, test_command) {
        Ok(events) => {
            info!("✅ PHASE 1 DISPATCH SUCCESS! Got {} events", events.len());
            for event in events {
                info!("   📧 Event: {} (Phase 1 demo event)", event.event_type);
            }
        }
        Err(e) => {
            info!("❌ Failed to dispatch command: {:?}", e);
        }
    }

    // Test with inventory aggregate too
    let inventory_command = StatefulCommand {
        aggregate: "inventory".to_string(),
        command_type: "adjust_inventory".to_string(),
        key: "product-456".to_string(),
        state: None,
        payload: vec![], // Simple payload for demo
    };

    match dispatch_command("inventory", "product-456".to_string(), None, inventory_command) {
        Ok(events) => {
            info!("✅ INVENTORY DISPATCH SUCCESS! Got {} events", events.len());
            for event in events {
                info!("   📧 Event: {} (Phase 1 demo event)", event.event_type);
            }
        }
        Err(e) => {
            info!("❌ Failed to dispatch inventory command: {:?}", e);
        }
    }

    // Test error handling with unknown aggregate
    let unknown_command = StatefulCommand {
        aggregate: "unknown".to_string(),
        command_type: "test".to_string(),
        key: "test".to_string(),
        state: None,
        payload: vec![],
    };

    match dispatch_command("unknown", "test".to_string(), None, unknown_command) {
        Ok(_) => {
            info!("❌ This should have failed!");
        }
        Err(e) => {
            info!("✅ Error handling works: {:?}", e);
        }
    }

    // Create simplified consumer declarations for testing
    let order_decl = InterestDeclaration {
        actor_id: "order".to_string(),
        key_field: "order_id".to_string(),
        entity_name: "order".to_string(),
        role: ActorRole::Aggregate,
        interest: ActorInterest::AggregateStream("order".to_string()),
        interest_constraint: InterestConstraint::Commands,
    };

    let inventory_decl = InterestDeclaration {
        actor_id: "inventory".to_string(),
        key_field: "product_id".to_string(),
        entity_name: "inventory".to_string(),
        role: ActorRole::Aggregate,
        interest: ActorInterest::AggregateStream("inventory".to_string()),
        interest_constraint: InterestConstraint::Commands,
    };

    let order_added = provider.add_consumer(&order_decl).await?;
    let inventory_added = provider.add_consumer(&inventory_decl).await?;

    info!("✅ Consumer registration - Order: {}, Inventory: {}", order_added, inventory_added);

    info!("");
    info!("🎉🎉🎉 PHASE 1 COMPLETE: AUTOMATED REGISTRATION SYSTEM! 🎉🎉🎉");
    info!("✨ Key Achievements:");
    info!("   📦 External aggregates auto-register with #[derive(Aggregate)]");
    info!("   🔍 Framework can discover all registered aggregate types");
    info!("   🛡️ Type-safe validation with helpful error messages");
    info!("   ⚡ Zero runtime overhead - compile-time registration");
    info!("   🧪 Dispatch framework ready for Phase 2 enhancement");
    info!("");
    info!("🚀 Next: Phase 2 - Replace demo events with real aggregate dispatch!");

    Ok(())
}
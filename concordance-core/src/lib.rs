// concordance-core/src/lib.rs - Enhanced with dynamic dispatch

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::trace;

// Re-export inventory for use in derive macros
pub use inventory;

// ============ CORE TRAITS (unchanged) ============

pub trait AggregateImpl: Sized + Debug + Send + Sync {
    const NAME: &'static str;
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError>;
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}

// ============ CORE DATA TYPES (unchanged) ============

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatefulCommand {
    pub aggregate: String,
    pub command_type: String,
    pub key: String,
    pub state: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_type: String,
    pub payload: Vec<u8>,
    pub stream: String,
}

#[derive(Debug, Clone)]
pub enum WorkError {
    Other(String),
}

impl std::fmt::Display for WorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for WorkError {}

// ============ NEW: DYNAMIC DISPATCH SYSTEM ============

/// Type-erased aggregate handler - this is the magic! ✨
pub type AggregateHandler = fn(String, Option<Vec<u8>>, StatefulCommand) -> Result<Vec<Event>, WorkError>;

/// Enhanced registry that stores both names AND handlers
pub struct AggregateDescriptor {
    pub name: &'static str,
    pub handler: AggregateHandler,
}

impl Debug for AggregateDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateDescriptor")
            .field("name", &self.name)
            .finish()
    }
}

// Set up inventory collection for enhanced descriptors
inventory::collect!(AggregateDescriptor);

// ============ PHASE 2: REAL DISPATCH IMPLEMENTATION ============

/// The actual dispatch implementation that calls real aggregates! 🎉
pub fn dispatch_command(
    entity_name: &str,
    key: String,
    state: Option<Vec<u8>>,
    command: StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    trace!("Dispatching command {} to aggregate {}", command.command_type, entity_name);
    
    // Find the handler in our registry
    let handler = inventory::iter::<AggregateDescriptor>()
        .find(|desc| desc.name == entity_name)
        .map(|desc| desc.handler)
        .ok_or_else(|| {
            WorkError::Other(format!(
                "Unknown aggregate type: {}. Available types: [{}]",
                entity_name,
                get_registered_aggregates().join(", ")
            ))
        })?;
    
    // 🎉 PHASE 2: Call the actual aggregate handler!
    handler(key, state, command)
}

/// Create a handler function for a specific aggregate type
/// This is called by the derive macro
pub const fn create_aggregate_handler<T>() -> AggregateHandler 
where
    T: AggregateImpl
{
    |key: String, state: Option<Vec<u8>>, command: StatefulCommand| -> Result<Vec<Event>, WorkError> {
        // Create/restore the aggregate from state
        let mut aggregate = T::from_state_direct(key, state)?;
        
        // Let the aggregate handle the command
        let events = aggregate.handle_command(command)?;
        
        // TODO: Save the new state back (Phase 2.1)
        // let new_state = aggregate.to_state()?;
        // save_state_somewhere(new_state);
        
        Ok(events)
    }
}

/// Enhanced registration helper that includes the handler
pub const fn create_aggregate_descriptor<T>() -> AggregateDescriptor 
where
    T: AggregateImpl
{
    AggregateDescriptor {
        name: T::NAME,
        handler: create_aggregate_handler::<T>(),
    }
}

// ============ UTILITY FUNCTIONS (unchanged) ============

pub fn get_registered_aggregates() -> Vec<&'static str> {
    inventory::iter::<AggregateDescriptor>()
        .into_iter()
        .map(|desc| desc.name)
        .collect()
}

pub fn is_aggregate_registered(name: &str) -> bool {
    inventory::iter::<AggregateDescriptor>()
        .into_iter()
        .any(|desc| desc.name == name)
}

/// Updated macro for manual registration
#[macro_export]
macro_rules! register_aggregate {
    ($aggregate_type:ty) => {
        $crate::inventory::submit! {
            $crate::create_aggregate_descriptor::<$aggregate_type>()
        }
    };
}

// ============ PHASE 2 TESTING ============

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestOrder {
        key: String,
        status: String,
    }

    impl AggregateImpl for TestOrder {
        const NAME: &'static str = "test_order";

        fn from_state_direct(key: String, _state: Option<Vec<u8>>) -> Result<Self, WorkError> {
            Ok(TestOrder {
                key,
                status: "new".to_string(),
            })
        }

        fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
            match command.command_type.as_str() {
                "create_order" => {
                    self.status = "created".to_string();
                    Ok(vec![Event {
                        event_type: "order_created".to_string(),
                        payload: format!("Order {} created", self.key).into_bytes(),
                        stream: "orders".to_string(),
                    }])
                }
                _ => Err(WorkError::Other("Unknown command".to_string())),
            }
        }

        fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
            Ok(Some(self.status.as_bytes().to_vec()))
        }
    }

    // Register the test aggregate with the NEW system
    register_aggregate!(TestOrder);

    #[test]
    fn test_phase2_real_dispatch() {
        let command = StatefulCommand {
            aggregate: "test_order".to_string(),
            command_type: "create_order".to_string(),
            key: "order-123".to_string(),
            state: None,
            payload: vec![],
        };

        // 🎉 This now calls the REAL aggregate!
        let result = dispatch_command("test_order", "order-123".to_string(), None, command);
        
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "order_created");
        
        // Verify the payload contains real data!
        let payload_str = String::from_utf8(events[0].payload.clone()).unwrap();
        assert_eq!(payload_str, "Order order-123 created");
    }
}
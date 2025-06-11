// concordance-core/src/lib.rs - Enhanced with apply_event support

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{error, trace};

// Re-export inventory for use in derive macros
pub use inventory;

// ============ ENHANCED CORE TRAITS ============

/// Core trait that all aggregates must implement
pub trait AggregateImpl: Sized + Debug + Send + Sync {
    /// The aggregate's name (used for routing and registration)
    const NAME: &'static str;

    /// Create an aggregate instance from serialized state
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError>;
    
    /// Handle a command and return resulting events (READ-ONLY operation)
    /// This should NOT modify the aggregate state
    fn handle_command(&self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    
    /// Apply an event to update aggregate state (WRITE operation)
    /// This modifies the aggregate state based on the event
    fn apply_event(&mut self, event: &Event) -> Result<(), WorkError>;
    
    /// Serialize the aggregate's current state
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}

// ============ CORE DATA TYPES ============

/// Represents a command to be processed by an aggregate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatefulCommand {
    pub aggregate: String,
    pub command_type: String,
    pub key: String,
    pub state: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

/// Represents an event that occurred in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_type: String,
    pub payload: Vec<u8>,
    pub stream: String,
}

/// Error types for aggregate operations
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

// ============ ENHANCED DISPATCH SYSTEM ============

/// Type-erased aggregate handler for the new command+event flow
pub type AggregateHandler = fn(String, Option<Vec<u8>>, StatefulCommand) -> Result<(Vec<Event>, Option<Vec<u8>>), WorkError>;

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

// ============ ENHANCED DISPATCH IMPLEMENTATION ============

/// Enhanced dispatch that handles command → events → state persistence
pub fn dispatch_command(
    entity_name: &str,
    key: String,
    state: Option<Vec<u8>>,
    command: StatefulCommand,
) -> Result<(Vec<Event>, Option<Vec<u8>>), WorkError> {
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
    
    // Call the enhanced handler that returns both events and new state
    handler(key, state, command)
}

/// Create an enhanced handler function for a specific aggregate type
/// This implements the full command → events → state update flow
pub const fn create_aggregate_handler<T>() -> AggregateHandler 
where
    T: AggregateImpl
{
    |key: String, state: Option<Vec<u8>>, command: StatefulCommand| -> Result<(Vec<Event>, Option<Vec<u8>>), WorkError> {
        // Step 1: Create/restore the aggregate from state
        let mut aggregate = T::from_state_direct(key, state)?;
        
        // Step 2: Handle the command (read-only) to generate events
        let events = aggregate.handle_command(command)?;
        
        // Step 3: Apply each event to update the aggregate state
        for event in &events {
            aggregate.apply_event(event)?;
        }
        
        // Step 4: Serialize the updated state
        let new_state = aggregate.to_state()?;
        
        Ok((events, new_state))
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

// ============ UTILITY FUNCTIONS ============

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

// ============ TESTING ============

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestOrder {
        key: String,
        status: String,
        version: u32,
    }

    impl AggregateImpl for TestOrder {
        const NAME: &'static str = "test_order";

        fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
            match state {
                Some(bytes) => {
                    let data: serde_json::Value = serde_json::from_slice(&bytes)
                        .map_err(|e| WorkError::Other(e.to_string()))?;
                    Ok(TestOrder {
                        key,
                        status: data["status"].as_str().unwrap_or("new").to_string(),
                        version: data["version"].as_u64().unwrap_or(0) as u32,
                    })
                }
                None => Ok(TestOrder {
                    key,
                    status: "new".to_string(),
                    version: 0,
                }),
            }
        }

        fn handle_command(&self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
            match command.command_type.as_str() {
                "create_order" => {
                    if self.status != "new" {
                        return Err(WorkError::Other("Order already exists".to_string()));
                    }
                    
                    Ok(vec![Event {
                        event_type: "order_created".to_string(),
                        payload: format!("Order {} created", self.key).into_bytes(),
                        stream: "orders".to_string(),
                    }])
                }
                "confirm_order" => {
                    if self.status != "created" {
                        return Err(WorkError::Other("Order must be created first".to_string()));
                    }
                    
                    Ok(vec![Event {
                        event_type: "order_confirmed".to_string(),
                        payload: format!("Order {} confirmed", self.key).into_bytes(),
                        stream: "orders".to_string(),
                    }])
                }
                _ => Err(WorkError::Other("Unknown command".to_string())),
            }
        }

        fn apply_event(&mut self, event: &Event) -> Result<(), WorkError> {
            match event.event_type.as_str() {
                "order_created" => {
                    self.status = "created".to_string();
                    self.version += 1;
                }
                "order_confirmed" => {
                    self.status = "confirmed".to_string();
                    self.version += 1;
                }
                _ => return Err(WorkError::Other(format!("Unknown event: {}", event.event_type))),
            }
            Ok(())
        }

        fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
            let state = serde_json::json!({
                "status": self.status,
                "version": self.version
            });
            Ok(Some(serde_json::to_vec(&state).unwrap()))
        }
    }

    // Register the test aggregate
    register_aggregate!(TestOrder);

    #[test]
    fn test_enhanced_command_event_flow() {
        let command = StatefulCommand {
            aggregate: "test_order".to_string(),
            command_type: "create_order".to_string(),
            key: "order-123".to_string(),
            state: None,
            payload: vec![],
        };

        // Test the enhanced dispatch that returns events AND new state
        let result = dispatch_command("test_order", "order-123".to_string(), None, command);
        
        assert!(result.is_ok());
        let (events, new_state) = result.unwrap();
        
        // Verify events were generated
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "order_created");
        
        // Verify state was updated
        assert!(new_state.is_some());
        let state_data: serde_json::Value = serde_json::from_slice(&new_state.unwrap()).unwrap();
        assert_eq!(state_data["status"], "created");
        assert_eq!(state_data["version"], 1);
    }

    #[test]
    fn test_state_persistence_flow() {
        // Create order
        let create_command = StatefulCommand {
            aggregate: "test_order".to_string(),
            command_type: "create_order".to_string(),
            key: "order-456".to_string(),
            state: None,
            payload: vec![],
        };

        let (_, state_after_create) = dispatch_command("test_order", "order-456".to_string(), None, create_command).unwrap();

        // Confirm order using the state from the previous command
        let confirm_command = StatefulCommand {
            aggregate: "test_order".to_string(),
            command_type: "confirm_order".to_string(),
            key: "order-456".to_string(),
            state: None,
            payload: vec![],
        };

        let (events, final_state) = dispatch_command("test_order", "order-456".to_string(), state_after_create, confirm_command).unwrap();
        
        // Verify the flow worked
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "order_confirmed");
        
        let final_state_data: serde_json::Value = serde_json::from_slice(&final_state.unwrap()).unwrap();
        assert_eq!(final_state_data["status"], "confirmed");
        assert_eq!(final_state_data["version"], 2);
    }
}
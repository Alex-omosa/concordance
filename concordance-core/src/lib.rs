// Core types and traits for the Concordance event sourcing framework

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{error, trace};

// Re-export inventory for use in derive macros
pub use inventory;

// ============ CORE TRAITS ============

/// Core trait that all aggregates must implement
pub trait AggregateImpl: Sized + Debug + Send + Sync {
    /// The aggregate's name (used for routing and registration)
    const NAME: &'static str;

    /// Create an aggregate instance from serialized state
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError>;
    
    /// Handle a command and return resulting events
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    
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

// ============ REGISTRY SYSTEM ============

/// Registry entry for an aggregate type - stores just the name for compile-time registration
pub struct AggregateDescriptor {
    /// The aggregate's name (used for command routing)
    pub name: &'static str,
}

impl Debug for AggregateDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateDescriptor")
            .field("name", &self.name)
            .finish()
    }
}

// Set up inventory collection
inventory::collect!(AggregateDescriptor);

// ============ DISPATCH SYSTEM ============

/// Simple dispatch that will be enhanced in Phase 2
/// For now, this demonstrates the registry working and provides a hook for later enhancement
pub fn dispatch_command(
    entity_name: &str,
    key: String,
    state: Option<Vec<u8>>,
    command: StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    trace!("Dispatching command {} to aggregate {}", command.command_type, entity_name);
    
    // Verify the aggregate type is registered
    if !is_aggregate_registered(entity_name) {
        error!("No handler registered for aggregate type: {}", entity_name);
        return Err(WorkError::Other(format!(
            "Unknown aggregate type: {}. Available types: [{}]",
            entity_name,
            get_registered_aggregates().join(", ")
        )));
    }
    
    // For Phase 1: Return a placeholder response showing the system works
    // In Phase 2, this will be replaced with actual aggregate dispatch
    tracing::info!("Phase 1: Would dispatch to {}, but dispatch not yet implemented", entity_name);
    
    // Return a demo event to show the system works
    Ok(vec![Event {
        event_type: format!("{}_demo_event", entity_name),
        payload: vec![],
        stream: entity_name.to_string(),
    }])
}

/// Get a list of all registered aggregate types (useful for debugging)
pub fn get_registered_aggregates() -> Vec<&'static str> {
    inventory::iter::<AggregateDescriptor>()
        .into_iter()
        .map(|desc| desc.name)
        .collect()
}

/// Check if an aggregate type is registered
pub fn is_aggregate_registered(name: &str) -> bool {
    inventory::iter::<AggregateDescriptor>()
        .into_iter()
        .any(|desc| desc.name == name)
}

// ============ REGISTRATION HELPER ============

/// Manual registration helper - used by the derive macro
/// This must be const to work with inventory's static submission
pub const fn create_aggregate_descriptor(name: &'static str) -> AggregateDescriptor {
    AggregateDescriptor { name }
}

/// Macro for manual aggregate registration (for testing or non-derive usage)
#[macro_export]
macro_rules! register_aggregate {
    ($aggregate_type:ty) => {
        $crate::inventory::submit! {
            $crate::create_aggregate_descriptor(<$aggregate_type as $crate::AggregateImpl>::NAME)
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
    }

    impl AggregateImpl for TestOrder {
        const NAME: &'static str = "test_order";

        fn from_state_direct(key: String, _state: Option<Vec<u8>>) -> Result<Self, WorkError> {
            Ok(TestOrder {
                key,
                status: "new".to_string(),
            })
        }

        fn handle_command(&mut self, _command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
            self.status = "created".to_string();
            Ok(vec![Event {
                event_type: "order_created".to_string(),
                payload: vec![],
                stream: "orders".to_string(),
            }])
        }

        fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
            Ok(Some(self.status.as_bytes().to_vec()))
        }
    }

    // Register the test aggregate
    register_aggregate!(TestOrder);

    #[test]
    fn test_aggregate_registration() {
        let registered = get_registered_aggregates();
        assert!(registered.contains(&"test_order"));
        assert!(is_aggregate_registered("test_order"));
        assert!(!is_aggregate_registered("nonexistent"));
    }

    #[test]
    fn test_phase1_dispatch() {
        // Test the Phase 1 dispatch system (placeholder implementation)
        let command = StatefulCommand {
            aggregate: "test_order".to_string(),
            command_type: "create_order".to_string(),
            key: "order-123".to_string(),
            state: None,
            payload: vec![],
        };

        // This should work with Phase 1 placeholder dispatch
        let result = dispatch_command("test_order", "order-123".to_string(), None, command);
        
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "test_order_demo_event");
    }

    #[test]
    fn test_unknown_aggregate_error() {
        let command = StatefulCommand {
            aggregate: "unknown".to_string(),
            command_type: "test".to_string(),
            key: "key".to_string(),
            state: None,
            payload: vec![],
        };

        let result = dispatch_command("unknown", "key".to_string(), None, command);
        assert!(result.is_err());
        
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("Unknown aggregate type"));
        assert!(error_msg.contains("test_order"));
    }
}
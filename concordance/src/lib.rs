// Main concordance library - combines core functionality with user-friendly API

// ============ RE-EXPORTS FOR NEW AGGREGATE SYSTEM ============

// Re-export core types and traits
pub use concordance_core::{
    AggregateImpl, Event, StatefulCommand, WorkError,
    dispatch_command, get_registered_aggregates, is_aggregate_registered,
};

// Re-export derive macro
pub use concordance_derive::Aggregate;

// Re-export the entire concordance_core crate for derive macro usage
pub use concordance_core;

// ============ MINIMAL CONFIGURATION FOR TESTING ============

use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Clone, Serialize, Deserialize)]
pub struct BaseConfiguration {
    pub nats_url: String,
    pub user_jwt: Option<String>,
    pub user_seed: Option<String>,
    pub js_domain: Option<String>,
}

impl Default for BaseConfiguration {
    fn default() -> Self {
        Self {
            nats_url: "127.0.0.1:4222".to_string(),
            user_jwt: None,
            user_seed: None,
            js_domain: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorRole {
    Aggregate,
    Projector,
    ProcessManager,
    Notifier,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterestConstraint {
    Commands,
    Events,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorInterest {
    AggregateStream(String),
    EventList(Vec<String>),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterestDeclaration {
    pub actor_id: String,
    pub key_field: String,
    pub entity_name: String,
    pub role: ActorRole,
    pub interest: ActorInterest,
    pub interest_constraint: InterestConstraint,
}

// ============ SIMPLIFIED PROVIDER FOR TESTING ============

#[derive(Clone, Debug)]
pub struct ConcordanceProvider {
    // Simplified for now - we'll add the full implementation later
}

impl ConcordanceProvider {
    pub async fn try_new(_base_config: BaseConfiguration) -> Result<ConcordanceProvider> {
        // Log registered aggregates for debugging
        let registered = get_registered_aggregates();
        tracing::info!("Concordance initialized with {} registered aggregates: [{}]", 
            registered.len(), 
            registered.join(", ")
        );

        Ok(ConcordanceProvider {
            // Simplified for now
        })
    }

    /// Simplified for testing - always returns true for now
    pub async fn add_consumer(&self, decl: &InterestDeclaration) -> Result<bool> {
        // Verify the aggregate is actually registered
        if !is_aggregate_registered(&decl.entity_name) {
            tracing::error!(
                "Cannot add consumer for unregistered aggregate '{}'. Available aggregates: [{}]",
                decl.entity_name,
                get_registered_aggregates().join(", ")
            );
            return Ok(false);
        }

        tracing::info!("Would add consumer for aggregate: {}", decl.entity_name);
        Ok(true)
    }

    /// Get list of registered aggregate types (useful for debugging)
    pub fn registered_aggregates(&self) -> Vec<&'static str> {
        get_registered_aggregates()
    }

    /// Check if a specific aggregate type is registered
    pub fn is_aggregate_available(&self, name: &str) -> bool {
        is_aggregate_registered(name)
    }
}
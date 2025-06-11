// concordance/src/lib.rs - Enhanced BaseConfiguration for Phase 3

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

// ============ ENHANCED CONFIGURATION WITH ENV SUPPORT ============

use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BaseConfiguration {
    pub nats_url: String,
    pub user_jwt: Option<String>,
    pub user_seed: Option<String>,
    pub js_domain: Option<String>,
}

impl Default for BaseConfiguration {
    fn default() -> Self {
        Self::from_env()
    }
}

impl BaseConfiguration {
    /// Create configuration from environment variables with sensible defaults
    pub fn from_env() -> Self {
        let nats_url = std::env::var("NATS_URL")
            .or_else(|_| std::env::var("NATS_SERVER"))
            .unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());

        let user_jwt = std::env::var("NATS_JWT").ok();
        let user_seed = std::env::var("NATS_SEED").ok();
        let js_domain = std::env::var("NATS_JS_DOMAIN").ok();

        tracing::info!("📡 NATS Configuration:");
        tracing::info!("   URL: {}", nats_url);
        tracing::info!("   JWT: {}", if user_jwt.is_some() { "***provided***" } else { "not set" });
        tracing::info!("   Seed: {}", if user_seed.is_some() { "***provided***" } else { "not set" });
        tracing::info!("   JS Domain: {}", js_domain.as_deref().unwrap_or("default"));

        Self {
            nats_url,
            user_jwt,
            user_seed,
            js_domain,
        }
    }

    /// Create configuration with explicit NATS URL
    pub fn with_nats_url(nats_url: impl Into<String>) -> Self {
        Self {
            nats_url: nats_url.into(),
            user_jwt: None,
            user_seed: None,
            js_domain: None,
        }
    }

    /// Get NATS connection using the configuration
    pub async fn get_nats_connection(&self) -> Result<async_nats::Client> {
        tracing::info!("🔌 Connecting to NATS at: {}", self.nats_url);
        
        let opts = async_nats::ConnectOptions::default()
            .name("Concordance Event Sourcing Framework");

        let client = opts
            .connect(&self.nats_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to NATS at {}: {}", self.nats_url, e))?;

        tracing::info!("✅ Successfully connected to NATS!");
        Ok(client)
    }

    /// Get JetStream context
    pub async fn get_jetstream_context(&self) -> Result<async_nats::jetstream::Context> {
        let nc = self.get_nats_connection().await?;
        
        let js = if let Some(ref domain) = self.js_domain {
            tracing::info!("🚀 Creating JetStream context with domain: {}", domain);
            async_nats::jetstream::with_domain(nc, domain)
        } else {
            tracing::info!("🚀 Creating JetStream context with default domain");
            async_nats::jetstream::new(nc)
        };

        Ok(js)
    }
}

// ============ REST OF THE API (unchanged) ============

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

impl InterestDeclaration {
    /// Create an aggregate interest declaration for commands
    pub fn aggregate_for_commands(actor_id: &str, entity_name: &str, key_field: &str) -> Self {
        Self {
            actor_id: actor_id.into(),
            entity_name: entity_name.into(),
            key_field: key_field.into(),
            role: ActorRole::Aggregate,
            interest: ActorInterest::AggregateStream(entity_name.to_string()),
            interest_constraint: InterestConstraint::Commands,
        }
    }

    /// Generate consumer name for this interest
    pub fn consumer_name(&self) -> String {
        let name = self.entity_name.clone();
        match self.role {
            ActorRole::Aggregate => {
                if let InterestConstraint::Commands = self.interest_constraint {
                    format!("AGG_CMD_{name}")
                } else {
                    format!("AGG_EVT_{name}")
                }
            }
            ActorRole::ProcessManager => format!("PM_{name}"),
            ActorRole::Notifier => format!("NOTIFIER_{name}"),
            ActorRole::Projector => format!("PROJ_{name}"),
            ActorRole::Unknown => "".to_string(),
        }
    }
}

// ============ SIMPLIFIED PROVIDER FOR PHASE 3 ============

#[derive(Clone, Debug)]
pub struct ConcordanceProvider {
    nc: async_nats::Client,
    js: async_nats::jetstream::Context,
}

impl ConcordanceProvider {
    /// Create a new Concordance provider with NATS integration
    pub async fn try_new(base_config: BaseConfiguration) -> Result<ConcordanceProvider> {
        // Log registered aggregates for debugging
        let registered = get_registered_aggregates();
        tracing::info!("🎯 Concordance initializing with {} registered aggregates: [{}]", 
            registered.len(), 
            registered.join(", ")
        );

        // Get NATS connection and JetStream context
        let nc = base_config.get_nats_connection().await?;
        let js = if let Some(ref domain) = base_config.js_domain {
            async_nats::jetstream::with_domain(nc.clone(), domain)
        } else {
            async_nats::jetstream::new(nc.clone())
        };

        // Ensure required streams exist
        let provider = ConcordanceProvider { nc, js };
        provider.ensure_streams().await?;

        tracing::info!("✅ Concordance provider initialized successfully!");
        Ok(provider)
    }

    /// Create with default configuration (reads from environment)
    pub async fn new() -> Result<ConcordanceProvider> {
        Self::try_new(BaseConfiguration::default()).await
    }

    /// Create with explicit NATS URL
    pub async fn with_nats_url(nats_url: impl Into<String>) -> Result<ConcordanceProvider> {
        Self::try_new(BaseConfiguration::with_nats_url(nats_url)).await
    }

    /// Ensure required NATS streams exist
    async fn ensure_streams(&self) -> Result<()> {
        use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};

        tracing::info!("🔧 Ensuring required NATS streams exist...");

        // Events stream
        let _event_stream = self.js
            .get_or_create_stream(StreamConfig {
                name: "CC_EVENTS".to_string(),
                description: Some("Concordance event stream for event sourcing".to_string()),
                num_replicas: 1,
                retention: RetentionPolicy::Limits,
                subjects: vec!["cc.events.*".to_string()],
                storage: StorageType::File,
                allow_rollup: false,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create events stream: {}", e))?;

        // Commands stream  
        let _command_stream = self.js
            .get_or_create_stream(StreamConfig {
                name: "CC_COMMANDS".to_string(),
                description: Some("Concordance command stream for event sourcing".to_string()),
                num_replicas: 1,
                retention: RetentionPolicy::WorkQueue,
                subjects: vec!["cc.commands.*".to_string()],
                storage: StorageType::File,
                allow_rollup: false,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create commands stream: {}", e))?;

        tracing::info!("✅ All required streams are ready!");
        Ok(())
    }

    /// Publish a command to the command stream
    pub async fn publish_command(
        &self,
        aggregate_name: &str,
        command: &serde_json::Value,
    ) -> Result<()> {
        let subject = format!("cc.commands.{}", aggregate_name);
        let payload = serde_json::to_vec(command)?;

        tracing::debug!("📤 Publishing command to subject: {}", subject);
        
        self.nc
            .publish(subject, payload.into())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to publish command: {}", e))?;

        Ok(())
    }

    /// Get list of registered aggregate types (useful for debugging)
    pub fn registered_aggregates(&self) -> Vec<&'static str> {
        get_registered_aggregates()
    }

    /// Check if a specific aggregate type is registered
    pub fn is_aggregate_available(&self, name: &str) -> bool {
        is_aggregate_registered(name)
    }

    /// Get access to the underlying NATS client
    pub fn nats_client(&self) -> &async_nats::Client {
        &self.nc
    }

    /// Get access to the JetStream context
    pub fn jetstream(&self) -> &async_nats::jetstream::Context {
        &self.js
    }
}

// ============ TESTING HELPERS ============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        // Clear any existing env vars
        std::env::remove_var("NATS_URL");
        std::env::remove_var("NATS_SERVER");
        
        let config = BaseConfiguration::from_env();
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        
        // Test override
        std::env::set_var("NATS_URL", "nats://custom:4222");
        let config = BaseConfiguration::from_env();
        assert_eq!(config.nats_url, "nats://custom:4222");
        
        // Cleanup
        std::env::remove_var("NATS_URL");
    }

    #[test]
    fn test_config_explicit_url() {
        let config = BaseConfiguration::with_nats_url("nats://example.com:4222");
        assert_eq!(config.nats_url, "nats://example.com:4222");
    }
}
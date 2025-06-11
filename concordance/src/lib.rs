// concordance/src/lib.rs - Enhanced with Auto-Setup and Worker Management

// ============ MODULES ============
pub mod persistence;

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

// Re-export persistence types
pub use persistence::EntityState;

// ============ ENHANCED CONFIGURATION WITH ENV SUPPORT ============

use serde::{Deserialize, Serialize};
use anyhow::Result;
use futures::StreamExt;

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

// ============ ENHANCED PROVIDER WITH AUTO-SETUP ============

#[derive(Clone, Debug)]
pub struct ConcordanceProvider {
    nc: async_nats::Client,
    js: async_nats::jetstream::Context,
}

impl ConcordanceProvider {
    /// Create with automatic NATS discovery and setup
    /// This is the main constructor users should use
    pub async fn new() -> Result<ConcordanceProvider> {
        tracing::info!("🚀 Initializing Concordance Event Sourcing Framework");
        
        // Use environment-based configuration
        let config = BaseConfiguration::from_env();
        
        Self::try_new(config).await
    }

    /// Create with explicit NATS URL (for simple cases)
    pub async fn with_nats_url(nats_url: impl Into<String>) -> Result<ConcordanceProvider> {
        let config = BaseConfiguration::with_nats_url(nats_url);
        Self::try_new(config).await
    }

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

    /// Start all workers for registered aggregates
    /// This replaces the manual worker setup
    pub async fn start_workers(&self) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let registered = get_registered_aggregates();
        tracing::info!("🎯 Starting workers for {} registered aggregates: [{}]", 
            registered.len(), 
            registered.join(", ")
        );

        let mut handles = Vec::new();

        for aggregate_name in registered {
            let handle = self.start_worker_for_aggregate(aggregate_name).await?;
            handles.push(handle);
        }

        tracing::info!("✅ All workers started successfully!");
        Ok(handles)
    }

    /// Start worker for a specific aggregate
    async fn start_worker_for_aggregate(&self, aggregate_name: &'static str) -> Result<tokio::task::JoinHandle<()>> {
        let nc = self.nc.clone();
        let js = self.js.clone();
        let agg_name = aggregate_name.to_string();
        
        let handle = tokio::spawn(async move {
            tracing::info!("🔧 Starting worker for aggregate: {}", agg_name);
            
            if let Err(e) = run_aggregate_worker(nc, js, &agg_name).await {
                tracing::error!("❌ Worker for {} failed: {}", agg_name, e);
            }
        });

        tracing::info!("✅ Worker started for aggregate: {}", aggregate_name);
        Ok(handle)
    }

    /// Process a single command (for testing or API endpoints)
    pub async fn handle_command(
        &self,
        aggregate_name: &str,
        key: &str,
        command_type: &str,
        data: serde_json::Value,
    ) -> Result<Vec<Event>> {
        tracing::debug!("📨 Processing command {} for {}.{}", command_type, aggregate_name, key);

        // Create the command
        let raw_command = RawCommand {
            command_type: command_type.to_string(),
            key: key.to_string(),
            data,
        };

        // Process using the same logic as the worker
        self.process_single_command(aggregate_name, raw_command).await
    }

    /// Internal method to process a command (shared with workers)
    async fn process_single_command(
        &self,
        aggregate_name: &str,
        raw_command: RawCommand,
    ) -> Result<Vec<Event>> {
        // Initialize state store if needed
        let state = EntityState::new_from_context(&self.js).await?;
        
        // Load current state
        let current_state = state.fetch_state(aggregate_name, &raw_command.key).await?;
        
        // Create StatefulCommand
        let stateful_command = StatefulCommand {
            aggregate: aggregate_name.to_string(),
            command_type: raw_command.command_type.clone(),
            key: raw_command.key.clone(),
            state: None,
            payload: serde_json::to_vec(&raw_command.data)?,
        };

        // Use enhanced dispatch
        let (events, new_state) = dispatch_command(
            aggregate_name,
            raw_command.key.clone(),
            current_state,
            stateful_command,
        )?;

        // Persist state if needed
        if let Some(state_bytes) = new_state {
            state.write_state(aggregate_name, &raw_command.key, state_bytes).await?;
        }

        // Publish events
        for event in &events {
            self.publish_event(event.clone()).await?;
        }

        tracing::debug!("✅ Command processed successfully, generated {} events", events.len());
        Ok(events)
    }

    /// Publish an event to NATS
    async fn publish_event(&self, event: Event) -> Result<()> {
        let subject = format!("cc.events.{}", event.event_type);
        
        let cloud_event = serde_json::json!({
            "specversion": "1.0",
            "type": event.event_type,
            "source": "concordance",
            "id": uuid::Uuid::new_v4().to_string(),
            "time": chrono::Utc::now().to_rfc3339(),
            "datacontenttype": "application/json",
            "data": serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap_or_default(),
        });

        let payload = serde_json::to_vec(&cloud_event)?;
        self.nc.publish(subject, payload.into()).await?;
        
        Ok(())
    }

    /// Health check for the entire system
    pub async fn health_check(&self) -> Result<HealthStatus> {
        let mut status = HealthStatus::new();

        // Check NATS connection
        match self.nc.connection_state() {
            async_nats::connection::State::Connected => {
                status.nats = ServiceHealth::Healthy;
            }
            _ => {
                status.nats = ServiceHealth::Unhealthy("NATS connection lost".to_string());
            }
        }

        // Check JetStream
        match self.js.get_stream("CC_COMMANDS").await {
            Ok(_) => status.jetstream = ServiceHealth::Healthy,
            Err(e) => status.jetstream = ServiceHealth::Unhealthy(e.to_string()),
        }

        // Check state store
        match EntityState::new_from_context(&self.js).await {
            Ok(state) => match state.health_check().await {
                Ok(_) => status.state_store = ServiceHealth::Healthy,
                Err(e) => status.state_store = ServiceHealth::Unhealthy(e.to_string()),
            }
            Err(e) => status.state_store = ServiceHealth::Unhealthy(e.to_string()),
        }

        status.overall = if status.is_healthy() {
            ServiceHealth::Healthy
        } else {
            ServiceHealth::Unhealthy("One or more services unhealthy".to_string())
        };

        Ok(status)
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

// ============ SIMPLIFIED WORKER FUNCTION ============

/// Simplified worker function that can be called from the provider
pub async fn run_aggregate_worker(
    nc: async_nats::Client,
    js: async_nats::jetstream::Context,
    aggregate_name: &str,
) -> Result<()> {
    let state = EntityState::new_from_context(&js).await?;
    let consumer_name = format!("AGG_CMD_{}", aggregate_name);
    let filter_subject = format!("cc.commands.{}", aggregate_name);

    tracing::info!("Creating consumer '{}' for aggregate '{}'", consumer_name, aggregate_name);

    let stream = js.get_stream("CC_COMMANDS").await?;
    
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            async_nats::jetstream::consumer::pull::Config {
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
    tracing::info!("Worker for '{}' ready - waiting for commands...", aggregate_name);

    while let Some(msg) = messages.next().await {
        match msg {
            Ok(message) => {
                let raw_command: RawCommand = match serde_json::from_slice(&message.payload) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        tracing::error!("Failed to parse command: {}. Acking and skipping.", e);
                        let _ = message.ack().await;
                        continue;
                    }
                };

                tracing::info!("Processing command: {} for key: {}", raw_command.command_type, raw_command.key);

                // Process the command using enhanced dispatch
                match process_command_with_state(&state, &nc, aggregate_name, raw_command).await {
                    Ok(_) => {
                        if let Err(e) = message.ack().await {
                            tracing::error!("Failed to ack command: {}", e);
                        } else {
                            tracing::info!("Command processed successfully");
                        }
                    }
                    Err(e) => {
                        tracing::error!("Command processing failed: {}", e);
                        if let Err(nack_err) = message.ack_with(async_nats::jetstream::AckKind::Nak(None)).await {
                            tracing::error!("Failed to nack command: {}", nack_err);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error receiving message: {}. Will continue...", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    Ok(())
}

/// Enhanced command processing with state persistence
async fn process_command_with_state(
    state: &EntityState,
    nc: &async_nats::Client,
    aggregate_name: &str,
    raw_command: RawCommand,
) -> Result<()> {
    let key = &raw_command.key;
    
    // Step 1: Load current state from NATS KV
    tracing::debug!("Loading state for {}.{}", aggregate_name, key);
    let current_state = state.fetch_state(aggregate_name, key).await?;

    // Step 2: Create StatefulCommand
    let stateful_command = StatefulCommand {
        aggregate: aggregate_name.to_string(),
        command_type: raw_command.command_type,
        key: key.clone(),
        state: None, // Not used in enhanced flow
        payload: serde_json::to_vec(&raw_command.data)?,
    };

    // Step 3: Use enhanced dispatch that handles command → events → state update
    tracing::debug!("Dispatching command using enhanced flow");
    let (events, new_state) = dispatch_command(
        aggregate_name,
        key.clone(),
        current_state,
        stateful_command,
    )?;

    tracing::debug!("Enhanced dispatch produced {} events", events.len());

    // Step 4: Persist the new state to NATS KV
    if let Some(state_bytes) = new_state {
        tracing::debug!("Saving updated state ({} bytes)", state_bytes.len());
        let _revision = state.write_state(aggregate_name, key, state_bytes).await?;
    }

    // Step 5: Publish all events to NATS
    for event in events {
        publish_event_to_nats(nc, event).await?;
    }

    Ok(())
}

/// Publish an event to NATS events stream
async fn publish_event_to_nats(nc: &async_nats::Client, event: Event) -> Result<()> {
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
    
    tracing::debug!("Published event: {}", event.event_type);
    Ok(())
}

// ============ HEALTH CHECK TYPES ============

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub overall: ServiceHealth,
    pub nats: ServiceHealth,
    pub jetstream: ServiceHealth,
    pub state_store: ServiceHealth,
    pub registered_aggregates: Vec<String>,
}

impl HealthStatus {
    fn new() -> Self {
        Self {
            overall: ServiceHealth::Unknown,
            nats: ServiceHealth::Unknown,
            jetstream: ServiceHealth::Unknown,
            state_store: ServiceHealth::Unknown,
            registered_aggregates: get_registered_aggregates().into_iter().map(String::from).collect(),
        }
    }

    fn is_healthy(&self) -> bool {
        matches!(self.nats, ServiceHealth::Healthy)
            && matches!(self.jetstream, ServiceHealth::Healthy)
            && matches!(self.state_store, ServiceHealth::Healthy)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceHealth {
    Healthy,
    Unhealthy(String),
    Unknown,
}

// ============ HELPER TYPES ============

#[derive(Debug, Serialize, Deserialize)]
pub struct RawCommand {
    pub command_type: String,
    pub key: String,
    pub data: serde_json::Value,
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
// concordance/src/lib.rs - Enhanced with Auto-Setup, Fixed Worker Management, and Observability

// ============ MODULES ============
pub mod persistence;
pub mod observability;

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
use crate::observability::{CorrelationContext, SpanBuilder, OperationLogger, OperationMetrics};
use tracing::Instrument;

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
            .name("Concordance Event Sourcing Framework")
            .max_reconnects(Some(60)) // Increased reconnect attempts
            .reconnect_delay_callback(|attempts| {
                std::time::Duration::from_millis(std::cmp::min((attempts as u64) * 100, 5000))
            });

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
        let correlation_ctx = CorrelationContext::new(
            aggregate_name.to_string(),
            key.to_string(),
            command_type.to_string(),
        );
        
        let root_span = tracing::info_span!(
            "api.handle_command",
            correlation_id = %correlation_ctx.correlation_id,
            aggregate.type = %aggregate_name,
            aggregate.key = %key,
            command.type = %command_type,
        );
        
        async move {
            OperationLogger::operation_started(&correlation_ctx);
            
            let raw_command = RawCommand {
                command_type: command_type.to_string(),
                key: key.to_string(),
                data,
            };

            let result = self.process_single_command(aggregate_name, raw_command).await;
            
            match &result {
                Ok(events) => {
                    OperationLogger::operation_completed(&correlation_ctx, 0, events.len());
                }
                Err(e) => {
                    OperationLogger::operation_failed(&correlation_ctx, &e.to_string(), 0);
                }
            }
            
            result
        }
        .instrument(root_span)
        .await
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

// ============ ENHANCED WORKER FUNCTION WITH ROBUST ERROR HANDLING ============

/// Enhanced worker function with robust heartbeat and connection handling
pub async fn run_aggregate_worker(
    nc: async_nats::Client,
    js: async_nats::jetstream::Context,
    aggregate_name: &str,
) -> Result<()> {
    let state = EntityState::new_from_context(&js).await?;
    let consumer_name = format!("AGG_CMD_{}", aggregate_name);
    let filter_subject = format!("cc.commands.{}", aggregate_name);

    tracing::info!(
        worker.aggregate_name = %aggregate_name,
        worker.consumer_name = %consumer_name,
        worker.filter_subject = %filter_subject,
        "Starting aggregate worker"
    );

    loop {
        // Wrap the entire worker loop in error handling to allow restarts
        match run_worker_loop(&nc, &js, &state, aggregate_name, &consumer_name, &filter_subject).await {
            Ok(_) => {
                tracing::warn!(
                    worker.aggregate_name = %aggregate_name,
                    "Worker loop ended normally"
                );
                break;
            }
            Err(e) => {
                tracing::error!(
                    worker.aggregate_name = %aggregate_name,
                    error = %e,
                    "Worker loop failed, will restart in 5 seconds"
                );
                
                // Wait before restarting to avoid tight loops
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                tracing::info!(
                    worker.aggregate_name = %aggregate_name,
                    "Restarting worker loop"
                );
            }
        }
    }

    Ok(())
}

// Replace the message processing loop in concordance/src/lib.rs around line 850

/// Enhanced worker loop with improved heartbeat handling
async fn run_worker_loop(
    nc: &async_nats::Client,
    js: &async_nats::jetstream::Context,
    state: &EntityState,
    aggregate_name: &str,
    consumer_name: &str,
    filter_subject: &str,
) -> Result<()> {
    let stream = js.get_stream("CC_COMMANDS").await
        .map_err(|e| anyhow::anyhow!("Failed to get commands stream: {}", e))?;
    
    let consumer = stream
        .get_or_create_consumer(
            consumer_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.to_string()),
                name: Some(consumer_name.to_string()),
                description: Some(format!("Commands for {}", aggregate_name)),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(90), // 🔧 INCREASED: More time for processing
                max_deliver: 3,
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                filter_subject: filter_subject.to_string(),
                max_waiting: 1, // 🔧 REDUCED: Only 1 concurrent pull to avoid heartbeat issues
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create consumer: {}", e))?;

    let mut messages = consumer
        .stream()
        .max_messages_per_batch(1) // Process one message at a time
        .messages()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create message stream: {}", e))?;
    
    tracing::info!(
        worker.aggregate_name = %aggregate_name,
        worker.status = "ready",
        "Worker ready to process commands"
    );

    // 🔧 IMPROVED: Better heartbeat error tracking
    let mut last_heartbeat_error = std::time::Instant::now();
    let mut consecutive_errors = 0;
    
    loop {
        // 🔧 REDUCED: Shorter timeout to handle heartbeats better
        let message_result = tokio::time::timeout(
            std::time::Duration::from_secs(15), // Reduced from 30s to 15s
            messages.next()
        ).await;

        match message_result {
            Ok(Some(msg_result)) => {
                consecutive_errors = 0; // Reset on successful message reception
                
                match msg_result {
                    Ok(message) => {
                        // Process message normally
                        let nats_message_id = match message.info() {
                            Ok(info) => Some(format!("{}", info.stream_sequence)),
                            Err(_) => None,
                        };
                        let nats_subject = message.subject.clone();
                        
                        let raw_command: RawCommand = match serde_json::from_slice(&message.payload) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    nats.subject = %message.subject,
                                    "Failed to parse command"
                                );
                                let _ = message.ack().await;
                                continue;
                            }
                        };

                        let correlation_ctx = CorrelationContext::new(
                            aggregate_name.to_string(),
                            raw_command.key.clone(),
                            raw_command.command_type.clone(),
                        )
                        .with_nats_info(nats_message_id, Some(nats_subject.to_string()));

                        let operation_start = std::time::Instant::now();
                        
                        match process_command_with_observability(
                            state,
                            nc,
                            aggregate_name,
                            raw_command,
                            correlation_ctx.clone(),
                        )
                        .await {
                            Ok(_) => {
                                let span_builder = SpanBuilder::new(correlation_ctx.clone());
                                let ack_span = span_builder.message_ack(true);
                                
                                let _guard = ack_span.enter();
                                
                                if let Err(e) = message.ack().await {
                                    tracing::error!(
                                        correlation_id = %correlation_ctx.correlation_id,
                                        error = %e,
                                        "Failed to acknowledge message"
                                    );
                                } else {
                                    let duration_ms = operation_start.elapsed().as_millis() as u64;
                                    OperationLogger::operation_completed(&correlation_ctx, duration_ms, 0);
                                }
                            }
                            Err(e) => {
                                let duration_ms = operation_start.elapsed().as_millis() as u64;
                                OperationLogger::operation_failed(&correlation_ctx, &e.to_string(), duration_ms);
                                
                                let span_builder = SpanBuilder::new(correlation_ctx.clone());
                                let ack_span = span_builder.message_ack(false);
                                let _guard = ack_span.enter();
                                
                                if let Err(nack_err) = message.ack_with(async_nats::jetstream::AckKind::Nak(None)).await {
                                    tracing::error!(
                                        correlation_id = %correlation_ctx.correlation_id,
                                        error = %nack_err,
                                        "Failed to nack message"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        
                        // 🔧 IMPROVED: Better heartbeat error handling
                        if error_msg.contains("missed idle heartbeat") {
                            let now = std::time::Instant::now();
                            let time_since_last = now.duration_since(last_heartbeat_error);
                            
                            // Only log if it's been a while since the last error
                            if time_since_last > std::time::Duration::from_secs(60) {
                                tracing::debug!( // 🔧 CHANGED: From error to debug
                                    worker.aggregate_name = %aggregate_name,
                                    "Heartbeat timeout during idle period (this is normal)"
                                );
                                last_heartbeat_error = now;
                            }
                            
                            consecutive_errors += 1;
                            
                            // 🔧 ONLY restart after many consecutive errors over a long period
                            if consecutive_errors > 50 && time_since_last < std::time::Duration::from_secs(10) {
                                tracing::error!(
                                    worker.aggregate_name = %aggregate_name,
                                    consecutive_errors = %consecutive_errors,
                                    "Too many rapid heartbeat errors, restarting worker"
                                );
                                return Err(anyhow::anyhow!("Excessive heartbeat timeouts"));
                            }
                            
                            // Short delay before continuing
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        } else {
                            // For non-heartbeat errors, log normally
                            tracing::warn!(
                                worker.aggregate_name = %aggregate_name,
                                error = %e,
                                "NATS message error"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                    }
                }
            }
            Ok(None) => {
                tracing::info!(
                    worker.aggregate_name = %aggregate_name,
                    "Message stream ended"
                );
                return Ok(());
            }
            Err(_timeout) => {
                // 🔧 IMPROVED: Timeout is normal during idle periods
                tracing::trace!( // 🔧 CHANGED: From debug to trace
                    worker.aggregate_name = %aggregate_name,
                    "No messages in 15s (normal during idle periods)"
                );
                
                // Check NATS connection health less frequently
                if nc.connection_state() != async_nats::connection::State::Connected {
                    tracing::error!(
                        worker.aggregate_name = %aggregate_name,
                        connection_state = ?nc.connection_state(),
                        "NATS connection lost, restarting worker"
                    );
                    return Err(anyhow::anyhow!("NATS connection lost"));
                }
                
                continue;
            }
        }
    }
}
/// Enhanced command processing with full observability
async fn process_command_with_observability(
    state: &EntityState,
    nc: &async_nats::Client,
    aggregate_name: &str,
    raw_command: RawCommand,
    correlation_ctx: CorrelationContext,
) -> Result<()> {
    // Create root span for entire operation
    let span_builder = SpanBuilder::new(correlation_ctx.clone());
    let root_span = span_builder.worker_processing();
    
    async move {
        let mut metrics = OperationMetrics::new();
        metrics.mark_command_received();
        
        OperationLogger::operation_started(&correlation_ctx);
        
        let key = &raw_command.key;
        
        // Step 1: Load state with observability
        let span_builder = SpanBuilder::new(correlation_ctx.clone());
        let state_span = span_builder.state_load();
        let current_state = {
            let _guard = state_span.enter();
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                state.key = %key,
                "Loading aggregate state"
            );
            
            let state_result = state.fetch_state(aggregate_name, key).await?;
            let state_size = state_result.as_ref().map(|s| s.len());
            
            metrics.mark_state_loaded(state_size);
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                state.exists = %state_result.is_some(),
                state.size_bytes = ?state_size,
                "State loaded"
            );
            
            state_result
        };

        // Step 2: Create StatefulCommand
        let stateful_command = StatefulCommand {
            aggregate: aggregate_name.to_string(),
            command_type: raw_command.command_type,
            key: key.clone(),
            state: None,
            payload: serde_json::to_vec(&raw_command.data)?,
        };

        // Step 3: Dispatch command with domain layer observability
        let span_builder = SpanBuilder::new(correlation_ctx.clone());
        let dispatch_span = span_builder.domain_command_handling();
        let (events, new_state) = {
            let _guard = dispatch_span.enter();
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                command.type = %stateful_command.command_type,
                "Dispatching command to aggregate"
            );
            
            let dispatch_result = dispatch_command(
                aggregate_name,
                key.clone(),
                current_state,
                stateful_command,
            )?;
            
            metrics.mark_command_handled(dispatch_result.0.len());
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                events.count = %dispatch_result.0.len(),
                state.updated = %dispatch_result.1.is_some(),
                "Command handled by aggregate"
            );
            
            dispatch_result
        };

        // Log each generated event
        for event in &events {
            let event_size = event.payload.len();
            metrics.add_event_size(event_size);
            OperationLogger::event_processing(&correlation_ctx);
        }

        metrics.mark_events_applied();

        // Step 4: Persist state with observability
        if let Some(state_bytes) = new_state {
            let span_builder = SpanBuilder::new(correlation_ctx.clone());
            let persist_span = span_builder.state_persist();
            let _guard = persist_span.enter();
            
            let state_size = state_bytes.len();
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                state.size_bytes = %state_size,
                "Persisting aggregate state"
            );
            
            let revision = state.write_state(aggregate_name, key, state_bytes).await?;
            
            metrics.mark_state_persisted();
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                state.revision = %revision,
                "State persisted"
            );
        }

        // Step 5: Publish events with observability
        for event in events {
            let event_type = event.event_type.clone();
            let span_builder = SpanBuilder::new(correlation_ctx.clone());
            let publish_span = span_builder.event_publish(&event_type);
            
            let _guard = publish_span.enter();
            
            tracing::debug!(
                correlation_id = %correlation_ctx.correlation_id,
                event.type = %event_type,
                "Publishing event"
            );
            
            publish_event_to_nats(nc, event).await?;
            
            OperationLogger::nats_interaction(
                &correlation_ctx,
                "publish",
                &format!("cc.events.{}", &event_type)
            );
        }

        metrics.mark_events_published();
        metrics.mark_acknowledged();
        
        // Log final metrics
        metrics.log_timings(&correlation_ctx);
        
        Ok(())
    }
    .instrument(root_span)
    .await
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

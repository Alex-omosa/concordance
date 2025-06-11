// concordance/src/observability.rs - OpenTelemetry Observability

use std::sync::Arc;
use tracing::{info_span, Instrument, Span};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use chrono::Utc;
use anyhow::Result;

// ============ CORRELATION CONTEXT ============

/// Correlation context that flows through the entire request lifecycle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationContext {
    /// Unique identifier for the entire operation flow
    pub correlation_id: String,
    /// The originating NATS message ID (if available)
    pub nats_message_id: Option<String>,
    /// Aggregate type being processed
    pub aggregate_type: String,
    /// Aggregate instance key
    pub aggregate_key: String,
    /// Command or event type
    pub operation_type: String,
    /// When this operation started
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// NATS subject the message came from
    pub source_subject: Option<String>,
}

impl CorrelationContext {
    pub fn new(aggregate_type: String, aggregate_key: String, operation_type: String) -> Self {
        Self {
            correlation_id: Uuid::new_v4().to_string(),
            nats_message_id: None,
            aggregate_type,
            aggregate_key,
            operation_type,
            started_at: Utc::now(),
            source_subject: None,
        }
    }

    pub fn with_nats_info(mut self, message_id: Option<String>, subject: Option<String>) -> Self {
        self.nats_message_id = message_id;
        self.source_subject = subject;
        self
    }
}

// ============ SPAN BUILDER ============

/// Builder for creating consistent tracing spans across layers with OpenTelemetry attributes
pub struct SpanBuilder;

impl SpanBuilder {
    /// Create a span for NATS message ingestion
    pub fn nats_ingestion(ctx: &CorrelationContext) -> Span {
        info_span!(
            "nats.message.ingestion",
            // OpenTelemetry semantic conventions
            "messaging.system" = "nats",
            "messaging.operation" = "receive",
            "messaging.destination" = ?ctx.source_subject,
            "messaging.message_id" = ?ctx.nats_message_id,
            
            // Concordance-specific attributes
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.operation.type" = %ctx.operation_type,
            
            // Standard attributes
            "service.name" = "concordance-worker",
            "operation.name" = "nats_message_receive",
        )
    }

    /// Create a span for command processing in the worker layer
    pub fn worker_processing(ctx: &CorrelationContext) -> Span {
        info_span!(
            "worker.command.processing",
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.command.type" = %ctx.operation_type,
            "service.name" = "concordance-worker",
            "operation.name" = "command_processing",
            "layer" = "worker"
        )
    }

    /// Create a span for state loading operations
    pub fn state_load(ctx: &CorrelationContext) -> Span {
        info_span!(
            "state.load",
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "service.name" = "concordance-worker",
            "operation.name" = "state_load",
            "layer" = "persistence",
            "db.operation" = "select",
            "db.system" = "nats-kv"
        )
    }

    /// Create a span for aggregate command handling (domain layer)
    pub fn domain_command_handling(ctx: &CorrelationContext) -> Span {
        info_span!(
            "domain.command.handle",
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.command.type" = %ctx.operation_type,
            "service.name" = "concordance-worker",
            "operation.name" = "domain_command_handle",
            "layer" = "domain"
        )
    }

    /// Create a span for event application in domain
    pub fn domain_event_apply(ctx: &CorrelationContext, event_type: &str) -> Span {
        info_span!(
            "domain.event.apply",
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.event.type" = %event_type,
            "service.name" = "concordance-worker",
            "operation.name" = "domain_event_apply",
            "layer" = "domain"
        )
    }

    /// Create a span for state persistence
    pub fn state_persist(ctx: &CorrelationContext) -> Span {
        info_span!(
            "state.persist",
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "service.name" = "concordance-worker",
            "operation.name" = "state_persist",
            "layer" = "persistence",
            "db.operation" = "update",
            "db.system" = "nats-kv"
        )
    }

    /// Create a span for event publishing
    pub fn event_publish(ctx: &CorrelationContext, event_type: &str) -> Span {
        info_span!(
            "event.publish",
            "messaging.system" = "nats",
            "messaging.operation" = "publish",
            "messaging.destination" = format!("cc.events.{}", event_type),
            
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.event.type" = %event_type,
            
            "service.name" = "concordance-worker",
            "operation.name" = "event_publish",
            "layer" = "infrastructure"
        )
    }

    /// Create a span for message acknowledgment
    pub fn message_ack(ctx: &CorrelationContext, success: bool) -> Span {
        info_span!(
            "nats.message.ack",
            "messaging.system" = "nats",
            "messaging.operation" = if success { "ack" } else { "nack" },
            
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.ack.success" = %success,
            
            "service.name" = "concordance-worker",
            "operation.name" = "message_ack",
            "layer" = "infrastructure"
        )
    }
}

// ============ OPERATION LOGGER ============

/// Structured logger for operation flow with OpenTelemetry attributes
pub struct OperationLogger;

impl OperationLogger {
    /// Log the start of an operation
    pub fn operation_started(ctx: &CorrelationContext) {
        tracing::info!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.operation.type" = %ctx.operation_type,
            "operation.started" = true,
            "Operation started"
        );
    }

    /// Log successful operation completion
    pub fn operation_completed(ctx: &CorrelationContext, duration_ms: u64, events_generated: usize) {
        tracing::info!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.operation.type" = %ctx.operation_type,
            "concordance.operation.duration_ms" = %duration_ms,
            "concordance.events.generated" = %events_generated,
            "operation.completed" = true,
            "Operation completed successfully"
        );
    }

    /// Log operation failure
    pub fn operation_failed(ctx: &CorrelationContext, error: &str, duration_ms: u64) {
        tracing::error!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.operation.type" = %ctx.operation_type,
            "concordance.operation.duration_ms" = %duration_ms,
            "concordance.error.message" = %error,
            "operation.failed" = true,
            "Operation failed"
        );
    }

    /// Log state transition
    pub fn state_transition(ctx: &CorrelationContext, from_version: u32, to_version: u32) {
        tracing::info!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.state.version.from" = %from_version,
            "concordance.state.version.to" = %to_version,
            "state.transition" = true,
            "State transition"
        );
    }

    /// Log event generation
    pub fn event_generated(ctx: &CorrelationContext, event_type: &str, event_size: usize) {
        tracing::debug!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "concordance.aggregate.type" = %ctx.aggregate_type,
            "concordance.aggregate.key" = %ctx.aggregate_key,
            "concordance.event.type" = %event_type,
            "concordance.event.size_bytes" = %event_size,
            "event.generated" = true,
            "Event generated"
        );
    }

    /// Log NATS interaction
    pub fn nats_interaction(ctx: &CorrelationContext, operation: &str, subject: &str) {
        tracing::debug!(
            "concordance.correlation_id" = %ctx.correlation_id,
            "messaging.system" = "nats",
            "messaging.operation" = %operation,
            "messaging.destination" = %subject,
            "nats.interaction" = true,
            "NATS interaction"
        );
    }
}

// ============ METRICS COLLECTOR ============

/// Metrics that can be collected during operation processing
#[derive(Debug, Default)]
pub struct OperationMetrics {
    pub command_received_at: Option<chrono::DateTime<chrono::Utc>>,
    pub state_loaded_at: Option<chrono::DateTime<chrono::Utc>>,
    pub command_handled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub events_applied_at: Option<chrono::DateTime<chrono::Utc>>,
    pub state_persisted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub events_published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub acknowledged_at: Option<chrono::DateTime<chrono::Utc>>,
    
    pub state_size_bytes: Option<usize>,
    pub events_generated: usize,
    pub total_event_size_bytes: usize,
}

impl OperationMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_command_received(&mut self) {
        self.command_received_at = Some(Utc::now());
    }

    pub fn mark_state_loaded(&mut self, size_bytes: Option<usize>) {
        self.state_loaded_at = Some(Utc::now());
        self.state_size_bytes = size_bytes;
    }

    pub fn mark_command_handled(&mut self, events_count: usize) {
        self.command_handled_at = Some(Utc::now());
        self.events_generated = events_count;
    }

    pub fn mark_events_applied(&mut self) {
        self.events_applied_at = Some(Utc::now());
    }

    pub fn mark_state_persisted(&mut self) {
        self.state_persisted_at = Some(Utc::now());
    }

    pub fn mark_events_published(&mut self) {
        self.events_published_at = Some(Utc::now());
    }

    pub fn mark_acknowledged(&mut self) {
        self.acknowledged_at = Some(Utc::now());
    }

    pub fn add_event_size(&mut self, size_bytes: usize) {
        self.total_event_size_bytes += size_bytes;
    }

    /// Log timing metrics with OpenTelemetry-friendly attributes
    pub fn log_timings(&self, ctx: &CorrelationContext) {
        if let (Some(start), Some(end)) = (self.command_received_at, self.acknowledged_at) {
            let total_duration = (end - start).num_milliseconds();
            
            tracing::info!(
                "concordance.correlation_id" = %ctx.correlation_id,
                "concordance.metrics.total_duration_ms" = %total_duration,
                "concordance.metrics.state_size_bytes" = ?self.state_size_bytes,
                "concordance.metrics.events_generated" = %self.events_generated,
                "concordance.metrics.total_event_size_bytes" = %self.total_event_size_bytes,
                "operation.metrics" = true,
                "Operation metrics"
            );

            // Log phase durations
            if let Some(state_loaded) = self.state_loaded_at {
                let state_load_duration = (state_loaded - start).num_milliseconds();
                tracing::debug!(
                    "concordance.correlation_id" = %ctx.correlation_id,
                    "concordance.metrics.phase" = "state_load",
                    "concordance.metrics.duration_ms" = %state_load_duration,
                    "phase.timing" = true,
                    "Phase timing"
                );
            }

            if let (Some(handled), Some(loaded)) = (self.command_handled_at, self.state_loaded_at) {
                let command_handle_duration = (handled - loaded).num_milliseconds();
                tracing::debug!(
                    "concordance.correlation_id" = %ctx.correlation_id,
                    "concordance.metrics.phase" = "command_handle",
                    "concordance.metrics.duration_ms" = %command_handle_duration,
                    "phase.timing" = true,
                    "Phase timing"
                );
            }
        }
    }
}

// ============ OPENTELEMETRY INITIALIZATION ============

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize tracing with OpenTelemetry support
pub fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::new("concordance=debug,user_app=debug,info")
        });

    #[cfg(feature = "opentelemetry")]
    {
        init_tracing_with_otel(filter)
    }

    #[cfg(not(feature = "opentelemetry"))]
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_target(true)
            .with_level(true)
            .json();

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
        
        tracing::info!("Tracing initialized (without OpenTelemetry)");
        Ok(())
    }
}

/// Initialize tracing with OpenTelemetry and Jaeger
#[cfg(feature = "opentelemetry")]
pub fn init_tracing_with_otel(filter: EnvFilter) -> Result<()> {
    use opentelemetry_sdk::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::layer::SubscriberExt;
    use opentelemetry_sdk::Resource;
    use opentelemetry::KeyValue;
    
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());
    
    tracing::info!("Initializing OpenTelemetry with endpoint: {}", otlp_endpoint);
    
    // Create resource with service information
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "concordance"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("service.namespace", "event-sourcing"),
    ]);
    
    // Configure OTLP exporter for Jaeger
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(otlp_endpoint)
        )
        .with_trace_config(
            opentelemetry_sdk::trace::config()
                .with_resource(resource)
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .map_err(|e| anyhow::anyhow!("Failed to initialize OTLP trace pipeline: {}", e))?;
    
    // Create layers
    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer);
    
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .with_level(true)
        .json();
    
    // Compose all layers
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();
    
    tracing::info!("OpenTelemetry tracing initialized successfully");
    Ok(())
}

/// Shutdown OpenTelemetry gracefully
#[cfg(feature = "opentelemetry")]
pub fn shutdown_telemetry() {
    opentelemetry::global::shutdown_tracer_provider();
}

#[cfg(not(feature = "opentelemetry"))]
pub fn shutdown_telemetry() {
    // No-op when OpenTelemetry is not enabled
}
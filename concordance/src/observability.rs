// concordance/src/observability.rs - Observability infrastructure for Concordance

use std::sync::Arc;
use tracing::{info_span, Instrument, Span};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use chrono::Utc;

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

/// Builder for creating consistent tracing spans across layers
pub struct SpanBuilder;

impl SpanBuilder {
    /// Create a span for NATS message ingestion
    pub fn nats_ingestion(ctx: &CorrelationContext) -> Span {
        info_span!(
            "nats.message.ingestion",
            correlation_id = %ctx.correlation_id,
            nats.message_id = ?ctx.nats_message_id,
            nats.subject = ?ctx.source_subject,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            operation.type = %ctx.operation_type,
            layer = "infrastructure"
        )
    }

    /// Create a span for command processing in the worker layer
    pub fn worker_processing(ctx: &CorrelationContext) -> Span {
        info_span!(
            "worker.command.processing",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            command.type = %ctx.operation_type,
            layer = "worker"
        )
    }

    /// Create a span for state loading operations
    pub fn state_load(ctx: &CorrelationContext) -> Span {
        info_span!(
            "state.load",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            layer = "persistence"
        )
    }

    /// Create a span for aggregate command handling (domain layer)
    pub fn domain_command_handling(ctx: &CorrelationContext) -> Span {
        info_span!(
            "domain.command.handle",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            command.type = %ctx.operation_type,
            layer = "domain"
        )
    }

    /// Create a span for event application in domain
    pub fn domain_event_apply(ctx: &CorrelationContext, event_type: &str) -> Span {
        info_span!(
            "domain.event.apply",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            event.type = %event_type,
            layer = "domain"
        )
    }

    /// Create a span for state persistence
    pub fn state_persist(ctx: &CorrelationContext) -> Span {
        info_span!(
            "state.persist",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            layer = "persistence"
        )
    }

    /// Create a span for event publishing
    pub fn event_publish(ctx: &CorrelationContext, event_type: &str) -> Span {
        info_span!(
            "event.publish",
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            event.type = %event_type,
            layer = "infrastructure"
        )
    }

    /// Create a span for message acknowledgment
    pub fn message_ack(ctx: &CorrelationContext, success: bool) -> Span {
        info_span!(
            "nats.message.ack",
            correlation_id = %ctx.correlation_id,
            ack.success = %success,
            layer = "infrastructure"
        )
    }
}

// ============ OPERATION LOGGER ============

/// Structured logger for operation flow
pub struct OperationLogger;

impl OperationLogger {
    /// Log the start of an operation
    pub fn operation_started(ctx: &CorrelationContext) {
        tracing::info!(
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            operation.type = %ctx.operation_type,
            "Operation started"
        );
    }

    /// Log successful operation completion
    pub fn operation_completed(ctx: &CorrelationContext, duration_ms: u64, events_generated: usize) {
        tracing::info!(
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            operation.type = %ctx.operation_type,
            duration_ms = %duration_ms,
            events.generated = %events_generated,
            "Operation completed successfully"
        );
    }

    /// Log operation failure
    pub fn operation_failed(ctx: &CorrelationContext, error: &str, duration_ms: u64) {
        tracing::error!(
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            operation.type = %ctx.operation_type,
            error = %error,
            duration_ms = %duration_ms,
            "Operation failed"
        );
    }

    /// Log state transition
    pub fn state_transition(ctx: &CorrelationContext, from_version: u32, to_version: u32) {
        tracing::info!(
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            state.version.from = %from_version,
            state.version.to = %to_version,
            "State transition"
        );
    }

    /// Log event generation
    pub fn event_generated(ctx: &CorrelationContext, event_type: &str, event_size: usize) {
        tracing::debug!(
            correlation_id = %ctx.correlation_id,
            aggregate.type = %ctx.aggregate_type,
            aggregate.key = %ctx.aggregate_key,
            event.type = %event_type,
            event.size_bytes = %event_size,
            "Event generated"
        );
    }

    /// Log NATS interaction
    pub fn nats_interaction(ctx: &CorrelationContext, operation: &str, subject: &str) {
        tracing::debug!(
            correlation_id = %ctx.correlation_id,
            nats.operation = %operation,
            nats.subject = %subject,
            "NATS interaction"
        );
    }
}

// ============ INSTRUMENTATION HELPERS ============

/// Extension trait to add correlation context to futures
pub trait InstrumentWithContext: Sized {
    type Output;
    fn instrument_with_context(self, ctx: Arc<CorrelationContext>) -> Self::Output;
}

impl<T: std::future::Future> InstrumentWithContext for T {
    type Output = tracing::instrument::Instrumented<T>;
    
    fn instrument_with_context(self, ctx: Arc<CorrelationContext>) -> Self::Output {
        self.instrument(tracing::info_span!(
            "operation",
            correlation_id = %ctx.correlation_id
        ))
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

    /// Log timing metrics
    pub fn log_timings(&self, ctx: &CorrelationContext) {
        if let (Some(start), Some(end)) = (self.command_received_at, self.acknowledged_at) {
            let total_duration = (end - start).num_milliseconds();
            
            tracing::info!(
                correlation_id = %ctx.correlation_id,
                metrics.total_duration_ms = %total_duration,
                metrics.state_size_bytes = ?self.state_size_bytes,
                metrics.events_generated = %self.events_generated,
                metrics.total_event_size_bytes = %self.total_event_size_bytes,
                "Operation metrics"
            );

            // Log phase durations
            if let Some(state_loaded) = self.state_loaded_at {
                let state_load_duration = (state_loaded - start).num_milliseconds();
                tracing::debug!(
                    correlation_id = %ctx.correlation_id,
                    metrics.phase = "state_load",
                    metrics.duration_ms = %state_load_duration,
                    "Phase timing"
                );
            }

            if let (Some(handled), Some(loaded)) = (self.command_handled_at, self.state_loaded_at) {
                let command_handle_duration = (handled - loaded).num_milliseconds();
                tracing::debug!(
                    correlation_id = %ctx.correlation_id,
                    metrics.phase = "command_handle",
                    metrics.duration_ms = %command_handle_duration,
                    "Phase timing"
                );
            }
        }
    }
}

// ============ TRACING SETUP ============

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize tracing with structured output
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            // Default filter - adjust as needed
            EnvFilter::new(
                "concordance=debug,user_app=debug,warn"
            )
        });

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .with_level(true)
        .json(); // JSON output for structured logging

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}

// ============ OPENTELEMETRY SETUP (for future use with Tempo) ============

#[cfg(feature = "opentelemetry")]
pub fn init_tracing_with_otel() {
    use opentelemetry::sdk::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::layer::SubscriberExt;
    
    // Configure OTLP exporter for Tempo
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");
    
    let trace_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_resource(opentelemetry::sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "concordance"),
                ]))
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Failed to initialize OTLP trace pipeline");
    
    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(trace_provider.tracer("concordance"));
    
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("concordance=debug,user_app=debug,warn"));
    
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json();
    
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();
}
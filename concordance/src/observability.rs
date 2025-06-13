// concordance/src/observability.rs - OpenTelemetry Observability

use tracing::Span;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use chrono::Utc;
use anyhow::Result;
use std::time::Duration;
use opentelemetry::{
    metrics::{Counter, Histogram, Meter},
    KeyValue,
};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter};

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
    /// Parent span ID for distributed tracing
    pub parent_span_id: Option<String>,
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
            parent_span_id: None,
        }
    }

    pub fn with_nats_info(mut self, message_id: Option<String>, subject: Option<String>) -> Self {
        self.nats_message_id = message_id;
        self.source_subject = subject;
        self
    }

    pub fn with_parent_span(mut self, span_id: String) -> Self {
        self.parent_span_id = Some(span_id);
        self
    }
}

// ============ SPAN BUILDER ============

/// Builder for creating consistent tracing spans across layers with OpenTelemetry attributes
pub struct SpanBuilder {
    pub correlation_ctx: CorrelationContext,
}

impl SpanBuilder {
    pub fn new(correlation_ctx: CorrelationContext) -> Self {
        Self { correlation_ctx }
    }

    pub fn command_processing(&self) -> Span {
        let span = tracing::info_span!(
            "command_processing",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }

    pub fn event_processing(&self) -> Span {
        let span = tracing::info_span!(
            "event_processing",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }

    pub fn state_persist(&self) -> Span {
        let span = tracing::info_span!(
            "state_persist",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }

    pub fn state_load(&self) -> Span {
        let span = tracing::info_span!(
            "state_load",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }

    pub fn domain_command_handling(&self) -> Span {
        let span = tracing::info_span!(
            "domain_command_handling",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }

    pub fn event_publish(&self, event_type: &str) -> Span {
        let span = tracing::info_span!(
            "event_publish",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id,
            event_type = %event_type
        );
        span
    }

    pub fn message_ack(&self, success: bool) -> Span {
        let span = tracing::info_span!(
            "message_ack",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id,
            success = %success
        );
        span
    }

    pub fn worker_processing(&self) -> Span {
        let span = tracing::info_span!(
            "worker_processing",
            correlation_id = %self.correlation_ctx.correlation_id,
            aggregate_type = %self.correlation_ctx.aggregate_type,
            aggregate_key = %self.correlation_ctx.aggregate_key,
            operation_type = %self.correlation_ctx.operation_type,
            source_subject = ?self.correlation_ctx.source_subject,
            parent_span_id = ?self.correlation_ctx.parent_span_id
        );
        span
    }
}

// ============ METRICS ============

/// Metrics for monitoring system performance
#[derive(Clone)]
pub struct Metrics {
    commands_processed: Counter<u64>,
    events_published: Counter<u64>,
    processing_time: Histogram<f64>,
    error_count: Counter<u64>,
    state_operations: Counter<u64>,
    nats_interactions: Counter<u64>,
}

impl Metrics {
    pub fn new(meter: Meter) -> Self {
        Self {
            commands_processed: meter.u64_counter("concordance.commands.processed")
                .with_description("Number of commands processed")
                .init(),
            events_published: meter.u64_counter("concordance.events.published")
                .with_description("Number of events published")
                .init(),
            processing_time: meter.f64_histogram("concordance.processing.time")
                .with_description("Time taken to process operations")
                .init(),
            error_count: meter.u64_counter("concordance.errors")
                .with_description("Number of errors encountered")
                .init(),
            state_operations: meter.u64_counter("concordance.state.operations")
                .with_description("Number of state operations performed")
                .init(),
            nats_interactions: meter.u64_counter("concordance.nats.interactions")
                .with_description("Number of NATS interactions")
                .init(),
        }
    }

    pub fn record_command_processed(&self, correlation_id: &str) {
        self.commands_processed.add(1, &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
        ]);
    }

    pub fn record_event_published(&self, correlation_id: &str, event_type: &str) {
        self.events_published.add(1, &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
            KeyValue::new("event_type", event_type.to_string()),
        ]);
    }

    pub fn record_processing_time(&self, correlation_id: &str, duration: Duration) {
        self.processing_time.record(duration.as_secs_f64(), &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
        ]);
    }

    pub fn record_error(&self, correlation_id: &str, error_type: &str) {
        self.error_count.add(1, &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
            KeyValue::new("error_type", error_type.to_string()),
        ]);
    }

    pub fn record_state_operation(&self, correlation_id: &str, operation_type: &str) {
        self.state_operations.add(1, &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
            KeyValue::new("operation_type", operation_type.to_string()),
        ]);
    }

    pub fn record_nats_interaction(&self, correlation_id: &str, interaction_type: &str) {
        self.nats_interactions.add(1, &[
            KeyValue::new("correlation_id", correlation_id.to_string()),
            KeyValue::new("interaction_type", interaction_type.to_string()),
        ]);
    }
}

// ============ OPERATION LOGGER ============

/// Helper for logging operations with consistent format
pub struct OperationLogger;

impl OperationLogger {
    pub fn command_processing(correlation_ctx: &CorrelationContext) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            "Processing command"
        );
    }

    pub fn event_processing(correlation_ctx: &CorrelationContext) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            "Processing event"
        );
    }

    pub fn state_operation(correlation_ctx: &CorrelationContext, operation: &str) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            operation = %operation,
            "State operation"
        );
    }

    pub fn nats_interaction(correlation_ctx: &CorrelationContext, interaction: &str, subject: &str) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            interaction = %interaction,
            subject = %subject,
            "NATS interaction"
        );
    }

    pub fn error(correlation_ctx: &CorrelationContext, error: &str) {
        tracing::error!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            error = %error,
            "Error occurred"
        );
    }

    pub fn operation_started(correlation_ctx: &CorrelationContext) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            "Operation started"
        );
    }

    pub fn operation_completed(correlation_ctx: &CorrelationContext, duration_ms: u64, event_count: usize) {
        tracing::info!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            duration_ms = %duration_ms,
            event_count = %event_count,
            "Operation completed"
        );
    }

    pub fn operation_failed(correlation_ctx: &CorrelationContext, error: &str, duration_ms: u64) {
        tracing::error!(
            correlation_id = %correlation_ctx.correlation_id,
            aggregate_type = %correlation_ctx.aggregate_type,
            aggregate_key = %correlation_ctx.aggregate_key,
            operation_type = %correlation_ctx.operation_type,
            error = %error,
            duration_ms = %duration_ms,
            "Operation failed"
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

use tracing_subscriber::layer::SubscriberExt;

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
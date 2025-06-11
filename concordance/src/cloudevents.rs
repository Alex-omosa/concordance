// concordance/src/cloudevents.rs - CloudEvents Integration with OpenTelemetry

use cloudevents_sdk::{Event as CloudEvent, EventBuilder, EventBuilderV10};
use serde::{Serialize, Deserialize};
use anyhow::Result;
use uuid::Uuid;
use chrono::Utc;
use std::collections::HashMap;
use tracing::{debug, error, instrument};

use crate::{Event, StatefulCommand, WorkError};
use crate::observability::{CorrelationContext, SpanBuilder};

// ============ CLOUDEVENTS WRAPPER TYPES ============

/// CloudEvents wrapper for Concordance events with built-in observability
#[derive(Debug, Clone)]
pub struct ConcordanceCloudEvent {
    pub cloud_event: CloudEvent,
    pub correlation_ctx: CorrelationContext,
    pub metadata: EventMetadata,
}

/// Command metadata for CloudEvents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMetadata {
    pub aggregate_type: String,
    pub aggregate_key: String,
    pub command_type: String,
    pub correlation_id: String,
    pub version: Option<u32>,
}

/// Event metadata for CloudEvents  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub aggregate_type: String,
    pub aggregate_key: String, 
    pub stream: String,
    pub version: u32,
    pub correlation_id: String,
    pub event_type: String,
}

impl EventMetadata {
    pub fn is_command(&self) -> bool {
        false
    }
}

impl CommandMetadata {
    pub fn is_command(&self) -> bool {
        true
    }
}

// ============ CLOUDEVENTS BUILDER ============

pub struct ConcordanceEventBuilder {
    source: String,
    correlation_ctx: Option<CorrelationContext>,
}

impl ConcordanceEventBuilder {
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            correlation_ctx: None,
        }
    }

    pub fn with_correlation_context(mut self, ctx: CorrelationContext) -> Self {
        self.correlation_ctx = Some(ctx);
        self
    }

    /// Create a CloudEvent from a Concordance Command
    #[instrument(skip(self, command), fields(command_type = %command.command_type))]
    pub fn from_command(
        self,
        command: &StatefulCommand,
    ) -> Result<ConcordanceCloudEvent, WorkError> {
        let correlation_ctx = self.correlation_ctx.unwrap_or_else(|| {
            CorrelationContext::new(
                command.aggregate.clone(),
                command.key.clone(),
                command.command_type.clone(),
            )
        });

        let metadata = CommandMetadata {
            aggregate_type: command.aggregate.clone(),
            aggregate_key: command.key.clone(),
            command_type: command.command_type.clone(),
            correlation_id: correlation_ctx.correlation_id.clone(),
            version: None,
        };

        let cloud_event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .source(&self.source)
            .ty(format!("com.concordance.command.{}.{}", 
                command.aggregate, command.command_type))
            .time(Utc::now())
            .subject(format!("{}/{}", command.aggregate, command.key))
            .data("application/json", &command.payload)
            // Add Concordance-specific extensions
            .extension("concordance_type", "command")
            .extension("concordance_aggregate", &command.aggregate)
            .extension("concordance_key", &command.key)
            .extension("concordance_command", &command.command_type)
            .extension("concordance_correlation_id", &correlation_ctx.correlation_id)
            .build()
            .map_err(|e| WorkError::Other(format!("Failed to build CloudEvent: {}", e)))?;

        Ok(ConcordanceCloudEvent {
            cloud_event,
            correlation_ctx,
            metadata: EventMetadata {
                aggregate_type: metadata.aggregate_type,
                aggregate_key: metadata.aggregate_key,
                stream: format!("commands.{}", command.aggregate),
                version: 0,
                correlation_id: metadata.correlation_id,
                event_type: command.command_type,
            },
        })
    }

    /// Create a CloudEvent from a Concordance Event
    #[instrument(skip(self, event), fields(event_type = %event.event_type))]
    pub fn from_event(
        self,
        event: &Event,
        aggregate_type: &str,
        aggregate_key: &str,
        version: u32,
    ) -> Result<ConcordanceCloudEvent, WorkError> {
        let correlation_ctx = self.correlation_ctx.unwrap_or_else(|| {
            CorrelationContext::new(
                aggregate_type.to_string(),
                aggregate_key.to_string(),
                event.event_type.clone(),
            )
        });

        let metadata = EventMetadata {
            aggregate_type: aggregate_type.to_string(),
            aggregate_key: aggregate_key.to_string(),
            stream: event.stream.clone(),
            version,
            correlation_id: correlation_ctx.correlation_id.clone(),
            event_type: event.event_type.clone(),
        };

        let cloud_event = EventBuilderV10::new()
            .id(Uuid::new_v4().to_string())
            .source(&self.source)
            .ty(format!("com.concordance.event.{}.{}", 
                aggregate_type, event.event_type))
            .time(Utc::now())
            .subject(format!("{}/{}", aggregate_type, aggregate_key))
            .data("application/json", &event.payload)
            // Add Concordance-specific extensions
            .extension("concordance_type", "event")
            .extension("concordance_aggregate", aggregate_type)
            .extension("concordance_key", aggregate_key)
            .extension("concordance_event", &event.event_type)
            .extension("concordance_stream", &event.stream)
            .extension("concordance_version", version.to_string())
            .extension("concordance_correlation_id", &correlation_ctx.correlation_id)
            .build()
            .map_err(|e| WorkError::Other(format!("Failed to build CloudEvent: {}", e)))?;

        Ok(ConcordanceCloudEvent {
            cloud_event,
            correlation_ctx,
            metadata,
        })
    }
}

// ============ TRACE CONTEXT INTEGRATION ============

impl ConcordanceCloudEvent {
    /// Inject OpenTelemetry trace context into CloudEvent
    #[instrument(skip(self))]
    pub fn inject_trace_context(&mut self) -> Result<(), WorkError> {
        use opentelemetry::{global, propagation::Injector};
        
        let mut carrier = HashMap::new();
        
        // Extract current OpenTelemetry context
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &opentelemetry::Context::current(), 
                &mut carrier
            );
        });

        // Add W3C TraceContext to CloudEvent extensions
        if let Some(traceparent) = carrier.get("traceparent") {
            self.cloud_event.set_extension("traceparent", traceparent.clone())
                .map_err(|e| WorkError::Other(format!("Failed to set traceparent: {}", e)))?;
        }

        if let Some(tracestate) = carrier.get("tracestate") {
            self.cloud_event.set_extension("tracestate", tracestate.clone())
                .map_err(|e| WorkError::Other(format!("Failed to set tracestate: {}", e)))?;
        }

        Ok(())
    }

    /// Extract OpenTelemetry trace context from CloudEvent
    #[instrument(skip(self))]
    pub fn extract_trace_context(&self) -> opentelemetry::Context {
        use opentelemetry::{global, propagation::Extractor};
        
        let mut carrier = HashMap::new();
        
        // Extract W3C TraceContext from CloudEvent extensions
        if let Some(traceparent) = self.cloud_event.extension("traceparent") {
            if let Some(traceparent_str) = traceparent.as_str() {
                carrier.insert("traceparent".to_string(), traceparent_str.to_string());
            }
        }

        if let Some(tracestate) = self.cloud_event.extension("tracestate") {
            if let Some(tracestate_str) = tracestate.as_str() {
                carrier.insert("tracestate".to_string(), tracestate_str.to_string());
            }
        }

        // Extract context using OpenTelemetry propagator
        global::get_text_map_propagator(|propagator| {
            propagator.extract(&carrier)
        })
    }
}

// ============ NATS INTEGRATION ============

/// Serialize CloudEvent for NATS transport (structured mode)
#[instrument(skip(cloud_event))]
pub fn serialize_for_nats(cloud_event: &CloudEvent) -> Result<(Vec<u8>, HashMap<String, String>), WorkError> {
    // Structured mode: CloudEvent as JSON with headers
    let mut headers = HashMap::new();
    
    // Add CloudEvents headers
    headers.insert("ce-specversion".to_string(), cloud_event.specversion().to_string());
    headers.insert("ce-id".to_string(), cloud_event.id().to_string());
    headers.insert("ce-source".to_string(), cloud_event.source().to_string());
    headers.insert("ce-type".to_string(), cloud_event.ty().to_string());

    // Optional CloudEvents attributes
    if let Some(datacontenttype) = cloud_event.datacontenttype() {
        headers.insert("ce-datacontenttype".to_string(), datacontenttype.to_string());
    }
    if let Some(subject) = cloud_event.subject() {
        headers.insert("ce-subject".to_string(), subject.to_string());
    }
    if let Some(time) = cloud_event.time() {
        headers.insert("ce-time".to_string(), time.to_rfc3339());
    }

    // CloudEvents extensions
    for (name, value) in cloud_event.extensions() {
        headers.insert(
            format!("ce-{}", name), 
            value.to_string()
        );
    }

    // Event data as payload
    let payload = cloud_event.data()
        .map(|data| data.to_vec())
        .unwrap_or_default();

    Ok((payload, headers))
}

/// Deserialize CloudEvent from NATS message (structured mode)
#[instrument(skip(payload, headers))]
pub fn deserialize_from_nats(
    payload: &[u8], 
    headers: &HashMap<String, String>
) -> Result<CloudEvent, WorkError> {
    let mut builder = EventBuilderV10::new();

    // Extract required attributes
    let id = headers.get("ce-id")
        .ok_or_else(|| WorkError::Other("Missing ce-id header".to_string()))?;
    let source = headers.get("ce-source")
        .ok_or_else(|| WorkError::Other("Missing ce-source header".to_string()))?;
    let event_type = headers.get("ce-type")
        .ok_or_else(|| WorkError::Other("Missing ce-type header".to_string()))?;

    builder = builder.id(id).source(source).ty(event_type);

    // Extract optional attributes
    if let Some(subject) = headers.get("ce-subject") {
        builder = builder.subject(subject);
    }
    if let Some(time_str) = headers.get("ce-time") {
        if let Ok(time) = chrono::DateTime::parse_from_rfc3339(time_str) {
            builder = builder.time(time.with_timezone(&Utc));
        }
    }

    // Add payload if present
    if !payload.is_empty() {
        let datacontenttype = headers.get("ce-datacontenttype")
            .map(|s| s.as_str())
            .unwrap_or("application/json");
        builder = builder.data(datacontenttype, payload);
    }

    // Add extensions
    for (key, value) in headers {
        if let Some(ext_name) = key.strip_prefix("ce-") {
            if !["specversion", "id", "source", "type", "datacontenttype", "subject", "time"].contains(&ext_name) {
                builder = builder.extension(ext_name, value);
            }
        }
    }

    builder.build()
        .map_err(|e| WorkError::Other(format!("Failed to build CloudEvent: {}", e)))
}

// ============ CONVENIENCE FUNCTIONS ============

/// Create a CloudEvent builder with Concordance defaults
pub fn concordance_event_builder() -> ConcordanceEventBuilder {
    ConcordanceEventBuilder::new("concordance://event-sourcing")
}

/// Create a command CloudEvent with trace context
#[instrument(skip(command, correlation_ctx))]
pub fn create_command_cloudevent(
    command: &StatefulCommand,
    correlation_ctx: Option<CorrelationContext>,
) -> Result<ConcordanceCloudEvent, WorkError> {
    let mut cloud_event = if let Some(ctx) = correlation_ctx {
        concordance_event_builder()
            .with_correlation_context(ctx)
            .from_command(command)?
    } else {
        concordance_event_builder()
            .from_command(command)?
    };

    // Always inject current trace context
    cloud_event.inject_trace_context()?;
    
    Ok(cloud_event)
}

/// Create an event CloudEvent with trace context
#[instrument(skip(event, correlation_ctx))]
pub fn create_event_cloudevent(
    event: &Event,
    aggregate_type: &str,
    aggregate_key: &str,
    version: u32,
    correlation_ctx: Option<CorrelationContext>,
) -> Result<ConcordanceCloudEvent, WorkError> {
    let mut cloud_event = if let Some(ctx) = correlation_ctx {
        concordance_event_builder()
            .with_correlation_context(ctx)
            .from_event(event, aggregate_type, aggregate_key, version)?
    } else {
        concordance_event_builder()
            .from_event(event, aggregate_type, aggregate_key, version)?
    };

    // Always inject current trace context
    cloud_event.inject_trace_context()?;
    
    Ok(cloud_event)
}
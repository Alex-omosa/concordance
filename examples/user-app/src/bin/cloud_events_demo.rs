use anyhow::Result;
use async_nats::Client;
use chrono::Utc;
use concordance::{
    cloudevents::{create_command_cloudevent, create_event_cloudevent},
    observability::{CorrelationContext, Metrics, OperationLogger, SpanBuilder},
    Event, StatefulCommand,
};
use opentelemetry::{
    global,
    sdk::{
        trace::{self, IdGenerator, Sampler},
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// ============ USER AGGREGATE ============

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    email: String,
    name: String,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateUserCommand {
    email: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserCreatedEvent {
    user_id: String,
    email: String,
    name: String,
    created_at: chrono::DateTime<Utc>,
}

// ============ MAIN ============

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with OpenTelemetry
    init_tracing()?;

    // Connect to NATS
    let nc = async_nats::connect("localhost:4222").await?;
    
    // Initialize metrics
    let metrics = init_metrics()?;

    // Create a test user
    let user_id = uuid::Uuid::new_v4().to_string();
    let command = CreateUserCommand {
        email: "test@example.com".to_string(),
        name: "Test User".to_string(),
    };

    // Create correlation context
    let correlation_ctx = CorrelationContext::new(
        "user".to_string(),
        user_id.clone(),
        "create_user".to_string(),
    );

    // Create command CloudEvent
    let stateful_command = StatefulCommand {
        aggregate: "user".to_string(),
        command_type: "create_user".to_string(),
        key: user_id.clone(),
        state: None,
        payload: serde_json::to_vec(&command)?,
    };

    let cloud_event = create_command_cloudevent(&stateful_command, Some(correlation_ctx.clone()))?;

    // Create span for command processing
    let span_builder = SpanBuilder::new(correlation_ctx.clone());
    let command_span = span_builder.command_processing();
    let _guard = command_span.enter();

    OperationLogger::command_processing(&correlation_ctx);

    // Simulate command processing
    info!("Processing create user command");
    sleep(Duration::from_millis(100)).await;

    // Create and publish event
    let event = UserCreatedEvent {
        user_id: user_id.clone(),
        email: command.email,
        name: command.name,
        created_at: Utc::now(),
    };

    let event = Event {
        event_type: "user_created".to_string(),
        payload: serde_json::to_vec(&event)?,
        stream: format!("users.{}", user_id),
    };

    publish_event(&nc, event, &correlation_ctx, &metrics).await?;

    // Record metrics
    metrics.record_command_processed(&correlation_ctx.correlation_id);
    metrics.record_processing_time(
        &correlation_ctx.correlation_id,
        Duration::from_millis(100),
    );

    info!("✅ User created successfully");
    Ok(())
}

// ============ HELPER FUNCTIONS ============

fn init_tracing() -> Result<()> {
    // Initialize OpenTelemetry
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(IdGenerator::default())
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    "user-app",
                )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Initialize the subscriber
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(opentelemetry)
        .init();

    Ok(())
}

fn init_metrics() -> Result<Metrics> {
    let meter = global::meter("user-app");
    Ok(Metrics::new(meter))
}

#[instrument(skip(nc, event, correlation_ctx, metrics), fields(
    event_type = %event.event_type,
    aggregate = %correlation_ctx.aggregate_type,
    key = %correlation_ctx.aggregate_key
))]
async fn publish_event(
    nc: &Client,
    event: Event,
    correlation_ctx: &CorrelationContext,
    metrics: &Metrics,
) -> Result<()> {
    let event_type = event.event_type.clone();
    let span_builder = SpanBuilder::new(correlation_ctx.clone());
    let publish_span = span_builder.event_publish("user.created");
    let _guard = publish_span.enter();

    // Create event CloudEvent
    let cloud_event = create_event_cloudevent(
        &event,
        &correlation_ctx.aggregate_type,
        &correlation_ctx.aggregate_key,
        0,
        Some(correlation_ctx.clone()),
    )?;

    // Serialize for NATS
    let (payload, headers) = concordance::cloudevents::serialize_for_nats(&cloud_event.cloud_event)?;
    
    // Create NATS headers
    let mut nats_headers = async_nats::HeaderMap::new();
    for (key, value) in headers {
        nats_headers.insert(key, value);
    }

    // Publish to NATS
    let subject = format!("cc.events.{}", event_type);
    nc.publish_with_headers(subject, nats_headers, payload.into()).await?;

    // Record metrics
    metrics.record_event_published(&correlation_ctx.correlation_id, &event_type);
    metrics.record_nats_interaction(&correlation_ctx.correlation_id, "publish");

    OperationLogger::nats_interaction(
        correlation_ctx,
        "publish",
        &format!("cc.events.{}", event_type),
    );

    debug!("✅ Event '{}' published successfully", event_type);
    Ok(())
} 
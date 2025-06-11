// examples/user-app/src/bin/worker.rs - Updated with OpenTelemetry observability

use user_app::*;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize OpenTelemetry tracing
    concordance::observability::init_tracing()?;

    tracing::info!(
        service.name = "concordance-worker",
        service.version = env!("CARGO_PKG_VERSION"),
        otel.enabled = cfg!(feature = "opentelemetry"),
        "Starting Concordance Production Worker with OpenTelemetry"
    );

    tracing::info!(
        aggregates.registered = ?get_registered_aggregates(),
        aggregates.count = get_registered_aggregates().len(),
        "Registered aggregates discovered"
    );

    // Create provider
    let app = match ConcordanceProvider::new().await {
        Ok(provider) => {
            tracing::info!(
                nats.connected = true,
                jetstream.enabled = true,
                "Concordance provider initialized successfully"
            );
            provider
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                initialization.failed = true,
                "Failed to initialize Concordance provider"
            );
            return Err(e);
        }
    };

    // Health check with structured logging
    match app.health_check().await {
        Ok(health) => {
            tracing::info!(
                health.nats = ?health.nats,
                health.jetstream = ?health.jetstream,
                health.state_store = ?health.state_store,
                health.aggregates = ?health.registered_aggregates,
                health.overall = ?health.overall,
                system.health_check = "passed",
                "System health check completed"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                system.health_check = "failed",
                "Health check encountered issues"
            );
        }
    }

    // Start workers
    let worker_handles = app.start_workers().await?;
    
    tracing::info!(
        workers.count = worker_handles.len(),
        nats.subjects.commands = "cc.commands.*",
        nats.subjects.events = "cc.events.*",
        nats.kv.bucket = "CC_STATE",
        workers.status = "started",
        "All workers started successfully"
    );

    // Create a shutdown handler
    let shutdown = tokio::signal::ctrl_c();
    
    // Wait for either all workers to stop or shutdown signal
    tokio::select! {
        result = futures::future::try_join_all(worker_handles) => {
            match result {
                Ok(_) => {
                    tracing::info!(
                        workers.status = "completed",
                        "All workers have completed normally"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        workers.status = "failed",
                        "Worker error occurred"
                    );
                }
            }
        }
        _ = shutdown => {
            tracing::info!(
                shutdown.signal = "received",
                workers.status = "stopping",
                "Received shutdown signal"
            );
        }
    }

    tracing::info!(
        service.shutdown = "starting",
        "Worker shutting down gracefully"
    );

    // Shutdown telemetry gracefully
    concordance::observability::shutdown_telemetry();

    tracing::info!(
        service.shutdown = "complete",
        "Worker shutdown complete"
    );

    Ok(())
}
// examples/user-app/src/bin/worker.rs - Clean Worker Using Shared Code

use user_app::*; // Import everything from lib.rs
use tracing::{info, Level};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        // .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    info!("🚀 Starting Concordance Production Worker");
    info!("Registered aggregates: {:?}", get_registered_aggregates());

    // 🎉 ALL SETUP IS HIDDEN IN THE FRAMEWORK!
    let app = ConcordanceProvider::new().await?;

    // Health check
    match app.health_check().await {
        Ok(health) => {
            info!("✅ Health check passed!");
            info!("   NATS: {:?}", health.nats);
            info!("   JetStream: {:?}", health.jetstream);
            info!("   State Store: {:?}", health.state_store);
        }
        Err(e) => {
            info!("⚠️  Health check issues: {}", e);
            info!("   Will continue anyway...");
        }
    }

    // 🎉 WORKER MANAGEMENT IS AUTOMATED!
    let worker_handles = app.start_workers().await?;

    info!("✅ All workers started! Ready to process commands.");
    info!("   📥 Listening: cc.commands.*");
    info!("   📤 Publishing: cc.events.*");
    info!("   💾 State: NATS KV (CC_STATE)");
    info!("");
    info!("Press Ctrl+C to stop.");

    // Wait for all workers
    futures::future::try_join_all(worker_handles).await?;

    info!("👋 Shutting down gracefully");
    Ok(())
}
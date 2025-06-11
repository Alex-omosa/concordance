// concordance/src/persistence.rs - NATS KV State Management

use async_nats::jetstream::{kv::Config as KvConfig, kv::Store, Context};
use tracing::{debug, error, instrument, trace, warn};
use anyhow::{Result, Error};

const STATE_BUCKET_NAME: &str = "CC_STATE";

/// NATS KV-backed state persistence for aggregates
#[derive(Clone, Debug)]
pub struct EntityState {
    bucket: Store,
}

impl EntityState {
    /// Create a new EntityState instance from JetStream context
    pub async fn new_from_context(context: &Context) -> Result<EntityState> {
        let bucket = get_or_create_bucket(context).await?;
        
        debug!("EntityState initialized with NATS KV bucket: {}", STATE_BUCKET_NAME);
        Ok(EntityState { bucket })
    }

    /// Write aggregate state to NATS KV
    #[instrument(level = "debug", skip(self, state))]
    pub async fn write_state(
        &self,
        entity_name: &str,
        key: &str,
        state: Vec<u8>,
    ) -> Result<u64> {
        trace!("Writing state for {}.{}", entity_name, key);

        let state_key = format!("agg.{}.{}", entity_name, key);
        
        let revision = self.bucket
            .put(&state_key, state.into())
            .await
            .map_err(|err| {
                let err_msg = format!("Failed to write state @ {}: {:?}", state_key, err);
                error!(error = %err, message = err_msg);
                Error::msg(err_msg)
            })?;

        debug!("State written for {} with revision {}", state_key, revision);
        Ok(revision)
    }

    /// Fetch aggregate state from NATS KV
    #[instrument(level = "debug", skip(self))]
    pub async fn fetch_state(
        &self,
        entity_name: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        trace!("Fetching state for {}.{}", entity_name, key);
        
        let state_key = format!("agg.{}.{}", entity_name, key);

        match self.bucket.get(&state_key).await {
            Ok(Some(entry)) => {
                let state = entry.to_vec();
                debug!("Found state for {} ({} bytes)", state_key, state.len());
                Ok(Some(state))
            }
            Ok(None) => {
                debug!("No state found for {}", state_key);
                Ok(None)
            }
            Err(err) => {
                let err_msg = format!("Failed to fetch state @ {}: {:?}", state_key, err);
                error!(error = %err, message = err_msg);
                Err(Error::msg(err_msg))
            }
        }
    }

    /// Remove aggregate state from NATS KV
    #[instrument(level = "debug", skip(self))]
    pub async fn remove_state(
        &self,
        entity_name: &str,
        key: &str,
    ) -> Result<()> {
        let state_key = format!("agg.{}.{}", entity_name, key);
        
        self.bucket
            .purge(&state_key)
            .await
            .map_err(|e| {
                let err_msg = format!("Failed to delete state @ {}: {:?}", state_key, e);
                error!(error = %e, message = err_msg);
                Error::msg(err_msg)
            })?;

        debug!("State removed for {}", state_key);
        Ok(())
    }

    /// Health check for the KV store
    pub async fn health_check(&self) -> Result<()> {
        let health_key = "health_check";
        let health_value = b"ok".to_vec();
        
        self.bucket.put(health_key, health_value.clone().into()).await
            .map_err(|e| Error::msg(format!("Health check write failed: {:?}", e)))?;
        
        let retrieved = self.bucket.get(health_key).await
            .map_err(|e| Error::msg(format!("Health check read failed: {:?}", e)))?;
        
        match retrieved {
            Some(entry) if entry.to_vec() == health_value => {
                debug!("EntityState health check passed");
                Ok(())
            }
            Some(_) => Err(Error::msg("Health check: value mismatch")),
            None => Err(Error::msg("Health check: value not found")),
        }
    }
}

/// Create or get the NATS KV bucket for state storage
async fn get_or_create_bucket(js: &Context) -> Result<Store> {
    debug!("Initializing NATS KV bucket: {}", STATE_BUCKET_NAME);

    // Try to get existing bucket first
    match js.get_key_value(STATE_BUCKET_NAME).await {
        Ok(store) => {
            debug!("Using existing KV bucket: {}", STATE_BUCKET_NAME);
            Ok(store)
        }
        Err(_) => {
            // Create new bucket if it doesn't exist
            debug!("Creating new KV bucket: {}", STATE_BUCKET_NAME);
            
            let store = js.create_key_value(KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                description: "Concordance aggregate state storage".to_string(),
                history: 5, // Keep last 5 versions for each key
                max_value_size: 1024 * 1024, // 1MB max per aggregate state
                max_age: std::time::Duration::from_secs(0), // No expiration
                storage: async_nats::jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::msg(format!("Failed to create KV bucket: {:?}", e)))?;

            debug!("Created KV bucket: {}", STATE_BUCKET_NAME);
            Ok(store)
        }
    }
}
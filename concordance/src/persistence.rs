// concordance/src/persistence.rs - Fixed NATS KV State Management with enhanced compatibility

use async_nats::jetstream::{kv::Config as KvConfig, kv::Store, Context};
use tracing::{debug, error, instrument, trace, warn, info};
use anyhow::{Result, Error};

const STATE_BUCKET_NAME: &str = "CC_STATE";

/// NATS KV-backed state persistence for aggregates with improved compatibility
#[derive(Clone, Debug)]
pub struct EntityState {
    bucket: Store,
}

impl EntityState {
    /// Create a new EntityState instance from JetStream context with enhanced retry logic
    pub async fn new_from_context(context: &Context) -> Result<EntityState> {
        let bucket = get_or_create_bucket_with_enhanced_retry(context, 5).await?;
        
        info!("✅ EntityState initialized with NATS KV bucket: {}", STATE_BUCKET_NAME);
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

/// Enhanced bucket creation with better error handling and compatibility
async fn get_or_create_bucket_with_enhanced_retry(js: &Context, max_retries: u32) -> Result<Store> {
    info!("🔧 Initializing NATS KV bucket: {} (with {} retries)", STATE_BUCKET_NAME, max_retries);

    for attempt in 1..=max_retries {
        debug!("Attempt {} of {} to get/create KV bucket", attempt, max_retries);
        
        // Try to get existing bucket first
        match js.get_key_value(STATE_BUCKET_NAME).await {
            Ok(store) => {
                info!("✅ Using existing KV bucket: {}", STATE_BUCKET_NAME);
                return Ok(store);
            }
            Err(e) => {
                debug!("Bucket doesn't exist yet, will try to create: {:?}", e);
            }
        }

        // Try multiple creation strategies
        match create_bucket_with_fallback_strategy(js, attempt).await {
            Ok(store) => {
                info!("✅ Created new KV bucket: {} on attempt {}", STATE_BUCKET_NAME, attempt);
                return Ok(store);
            }
            Err(e) => {
                warn!("Attempt {} failed to create bucket: {:?}", attempt, e);
                
                if attempt == max_retries {
                    return Err(Error::msg(format!(
                        "Failed to create KV bucket after {} attempts. Last error: {:?}", 
                        max_retries, e
                    )));
                }
                
                // Progressive backoff
                let delay_ms = 500 * attempt as u64;
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }

    Err(Error::msg("Unexpected end of retry loop"))
}

/// Create bucket with multiple fallback strategies to handle version compatibility
async fn create_bucket_with_fallback_strategy(js: &Context, attempt: u32) -> Result<Store> {
    debug!("Creating KV bucket with fallback strategy, attempt {}", attempt);
    
    match attempt {
        1 => {
            // Strategy 1: Absolutely minimal config (most compatible)
            debug!("Trying minimal config strategy");
            let minimal_config = KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                ..Default::default()
            };
            
            js.create_key_value(minimal_config)
                .await
                .map_err(|e| Error::msg(format!("Minimal config failed: {:?}", e)))
        }
        2 => {
            // Strategy 2: Basic config with essential settings only
            debug!("Trying basic config strategy");
            let basic_config = KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                description: "Concordance state storage".to_string(),
                history: 1,
                ..Default::default()
            };
            
            js.create_key_value(basic_config)
                .await
                .map_err(|e| Error::msg(format!("Basic config failed: {:?}", e)))
        }
        3 => {
            // Strategy 3: Explicit storage settings
            debug!("Trying explicit storage config strategy");
            let storage_config = KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                description: "Concordance aggregate state storage".to_string(),
                history: 1,
                max_value_size: 1024 * 1024, // 1MB max
                storage: async_nats::jetstream::stream::StorageType::File,
                num_replicas: 1,
                ..Default::default()
            };
            
            js.create_key_value(storage_config)
                .await
                .map_err(|e| Error::msg(format!("Storage config failed: {:?}", e)))
        }
        4 => {
            // Strategy 4: Memory storage (for compatibility)
            debug!("Trying memory storage config strategy");
            let memory_config = KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                description: "Concordance state storage (memory)".to_string(),
                history: 1,
                storage: async_nats::jetstream::stream::StorageType::Memory,
                num_replicas: 1,
                ..Default::default()
            };
            
            js.create_key_value(memory_config)
                .await
                .map_err(|e| Error::msg(format!("Memory config failed: {:?}", e)))
        }
        _ => {
            // Strategy 5+: Last resort with no optional fields
            debug!("Trying last resort config strategy");
            
            // Use the underlying stream creation approach as fallback
            create_kv_via_stream_creation(js).await
        }
    }
}

/// Last resort: Create KV bucket by manually creating the underlying stream
async fn create_kv_via_stream_creation(js: &Context) -> Result<Store> {
    use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
    
    debug!("Attempting to create KV bucket via manual stream creation");
    
    let stream_name = format!("KV_{}", STATE_BUCKET_NAME);
    let subject = format!("$KV.{}.>", STATE_BUCKET_NAME);
    
    let stream_config = StreamConfig {
        name: stream_name.clone(),
        description: Some("Concordance KV state store".to_string()),
        subjects: vec![subject],
        retention: RetentionPolicy::Limits,
        storage: StorageType::File,
        num_replicas: 1,
        max_age: std::time::Duration::ZERO, // No expiration
        discard: async_nats::jetstream::stream::DiscardPolicy::New,
        allow_rollup: true,
        ..Default::default()
    };
    
    // Create the stream first
    js.create_stream(stream_config)
        .await
        .map_err(|e| Error::msg(format!("Failed to create KV stream: {:?}", e)))?;
    
    // Now try to get the KV bucket
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    js.get_key_value(STATE_BUCKET_NAME)
        .await
        .map_err(|e| Error::msg(format!("Failed to get KV bucket after stream creation: {:?}", e)))
}
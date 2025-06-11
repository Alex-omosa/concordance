use async_nats::jetstream::{kv::Config as KvConfig, kv::Store, Context};
use tracing::{error, instrument, trace};
use anyhow::Error;

use crate::{config::ActorRole, Result};

pub(crate) const STATE_BUCKET_NAME: &str = "CC_STATE";

#[derive(Clone, Debug)]
pub struct EntityState {
    bucket: Store,
}

impl EntityState {
    pub async fn new_from_context(context: &async_nats::jetstream::Context) -> Result<EntityState> {
        Ok(EntityState {
            bucket: get_or_create_bucket(context).await?,
        })
    }

    #[instrument(level = "debug", skip(self, state))]
    pub async fn write_state(
        &self,
        actor_role: &ActorRole,
        entity_name: &str,
        key: &str,
        state: Vec<u8>,
    ) -> Result<()> {
        trace!("Writing state");

        let key = state_key(actor_role, entity_name, key);

        self.bucket
            .put(&key, state.into())
            .await
            .map_err(|err| {
                let err_msg = format!("Failed to write state @ {key}: {err:?}");
                error!(error = %err, message = err_msg);
                Error::msg(err_msg)
            })
            .map(|_| ())
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn fetch_state(
        &self,
        actor_role: &ActorRole,
        entity_name: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        trace!("Fetching state");
        let key = state_key(actor_role, entity_name, key);

        self.bucket
            .get(&key)
            .await
            .map_err(|err| {
                let err_msg = format!("Failed to fetch state @ {key}: {err:?}");
                error!(error = %err, message = err_msg);
                Error::msg(err_msg)
            })
            .map(|b| b.map(|v| v.to_vec()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn remove_state(
        &self,
        actor_role: &ActorRole,
        entity_name: &str,
        key: &str,
    ) -> Result<()> {
        let key = state_key(actor_role, entity_name, key);
        // We use a purge here instead of delete because we don't care about maintaining history (that comes from the event log)
        self.bucket
            .purge(&key)
            .await
            .map_err(|e| {
                let err_msg = format!("Failed to delete state @ {key}: {e:?}");
                error!(error = %e, message = err_msg);
                Error::msg(err_msg)
            })
            .map(|_| ())
    }
}

fn state_key(role: &ActorRole, entity_name: &str, key: &str) -> String {
    match role {
        ActorRole::Aggregate => format!("agg.{entity_name}.{key}"),
        ActorRole::ProcessManager => format!("pm.{entity_name}.{key}"),
        _ => {
            error!("Attempted to get a state key for an unsupported actor role: {role:?}");
            "".to_string()
        }
    }
}

async fn get_or_create_bucket(js: &Context) -> Result<Store> {
    if let Ok(store) = js
        .get_key_value(STATE_BUCKET_NAME)
        .await
        .map_err(|e| Error::msg(e.to_string()))
    {
        Ok(store)
    } else {
        Ok(js
            .create_key_value(KvConfig {
                bucket: STATE_BUCKET_NAME.to_string(),
                description: "Concordance state for aggregates and process managers".to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::msg(e.to_string()))?)
    }
}
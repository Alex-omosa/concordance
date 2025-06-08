use crate::{
    config::BaseConfiguration, consumers::ConsumerManager, natsclient::NatsClient,
    state::EntityState,
};
use anyhow::Result;

pub struct ConcordanceProvider {
    nc: async_nats::Client,
    consumer_manager: ConsumerManager,
    js: async_nats::jetstream::Context,
    state: EntityState,
}

impl ConcordanceProvider {
    pub async fn try_new(base_config: BaseConfiguration) -> Result<ConcordanceProvider> {
        let nc = base_config.get_nats_connection().await?;
        let js = if let Some(ref domain) = base_config.js_domain {
            async_nats::jetstream::with_domain(nc.clone(), domain)
        } else {
            async_nats::jetstream::new(nc.clone())
        };

        let client = NatsClient::new(js.clone());
        let (e, c) = client.ensure_streams().await.unwrap();
        let cm = ConsumerManager::new(e, c);
        let state = EntityState::new_from_context(&js).await?;

        Ok(ConcordanceProvider {
            nc,
            consumer_manager: cm,
            state,
            js,
        })
    }
}


mod config;
mod consumers;
mod events;
mod eventsourcing;
mod natsclient;
mod provider;
mod state;
mod workers;
mod aggregates;

use crate::natsclient::NatsClient;
use anyhow::Result;
use tracing::{error, warn};

use crate::{
    consumers::{CommandConsumer, ConsumerManager},
    state::EntityState,
    workers::AggregateCommandWorker,
};

pub use config::ActorInterest;
pub use config::ActorRole;
pub use config::BaseConfiguration;
pub use config::InterestConstraint;
pub use config::InterestDeclaration;

#[derive(Clone, Debug)]
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

    /// Adds a consumer and the appropriate worker to the provider's consumer manager, which will in turn create or
    /// bind to an existing NATS consumer
    pub async fn add_consumer(&self, decl: &InterestDeclaration) -> Result<bool> {
        
        use InterestConstraint::Commands;
        Ok(match (&decl.interest_constraint, &decl.role) {
            (Commands, _) => self.add_aggregate_cmd_consumer(decl).await,
            // (Events, ProcessManager) => self.add_process_manager_consumer(decl).await,
            // (Events, Projector) | (Events, Notifier) => self.add_general_event_consumer(decl).await,
            // (Events, Aggregate) => self.add_aggregate_event_consumer(decl).await,
            (a, b) => {
                warn!("Unsupported combination of consumer and worker: {a:?} {b:?}. Ignoring.");
                false
            }
        })
    }

    /// This is a command consumer that uses an aggregate-specific worker. This worker
    /// knows how to supply the aggregate with its internal state when applying
    /// the command. Note that aggregates can't change state during command
    /// application.
    async fn add_aggregate_cmd_consumer(&self, decl: &InterestDeclaration) -> bool {
        if let Err(e) = self
            .consumer_manager
            .add_consumer::<AggregateCommandWorker, CommandConsumer>(
                decl.to_owned(),
                AggregateCommandWorker::new(
                    self.nc.clone(),
                    self.js.clone(),
                    decl.clone(),
                    self.state.clone(),
                ),
            )
            .await
        {
            error!(
                "Failed to add command consumer for {} ({}): {}",
                decl.entity_name, decl.actor_id, e
            );
            return false;
        }
        true
    }
}

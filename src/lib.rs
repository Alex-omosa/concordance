mod config;
mod consumers;
mod natsclient;
mod workers;
mod state;
mod provider;
mod events;
mod eventsourcing;

use tracing::{error, instrument, warn};
use async_trait::async_trait;
use crate::natsclient::NatsClient;

use anyhow::Result;
use crate::config::{ActorRole, BaseConfiguration, InterestConstraint, InterestDeclaration};

use crate::consumers::{CommandConsumer, ConsumerManager, EventConsumer};


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntityState;
#[derive(Clone)]
pub struct ConcordanceProvider {
    nc: async_nats::Client,
    consumer_manager: ConsumerManager,
    js: async_nats::jetstream::Context,
    state: EntityState,
}


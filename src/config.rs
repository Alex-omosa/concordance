// config.rs
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use anyhow::{
    Result,
    Error
};
const DEFAULT_BATCH_MAX: usize = 200;



#[derive(Clone, Serialize, Deserialize)]
pub struct BaseConfiguration {
    /// Address of the NATS server
    pub nats_url: String,
    /// User JWT for connecting to the core NATS
    pub user_jwt: Option<String>,
    /// User seed for connecting to the core NATS
    pub user_seed: Option<String>,
    /// JetStream domain for the JS context used by this provider
    pub js_domain: Option<String>,
}

impl Default for BaseConfiguration {
    fn default() -> Self {
        Self {
            nats_url: "127.0.0.1:4222".to_string(),
            user_jwt: None,
            user_seed: None,
            js_domain: None,
        }
    }
}

impl BaseConfiguration {
    pub async fn get_nats_connection(&self) -> Result<async_nats::Client> {
        let base_opts = async_nats::ConnectOptions::default();
        Ok(base_opts
            .name("Concordance Event Sourcing")
            .connect(&self.nats_url)
            .await
            .map_err(|e| Error::msg(format!("failed to make NATS connection to {}: {}", self.nats_url, e)))?)
    }
}


/// All entities participating in an event sourced system must declare their interest.
/// Aggregates declare interest in the stream that corresponds to their name, notifiers and process managers declare interest
/// in an explicit list of event types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterestDeclaration {
    pub actor_id: String,
    pub key_field: String,
    pub entity_name: String,
    pub role: ActorRole,
    pub interest: ActorInterest,
    pub interest_constraint: InterestConstraint,
}

impl InterestDeclaration {
    pub fn consumer_name(&self) -> String {
        let name = self.entity_name.clone();
        match self.role {
            ActorRole::Aggregate => {
                if let InterestConstraint::Commands = self.interest_constraint {
                    format!("AGG_CMD_{name}")
                } else {
                    format!("AGG_EVT_{name}")
                }
            }
            ActorRole::ProcessManager => {
                format!("PM_{name}")
            }
            ActorRole::Notifier => {
                format!("NOTIFIER_{name}")
            }
            ActorRole::Projector => {
                format!("PROJ_{name}")
            }
            ActorRole::Unknown => {
                "".to_string() // unknown decls can be used for publish-only entities or are filtered out via error early
            }
        }
    }

    pub fn extract_max_messages_per_batch(&self) -> usize {
        // Simple implementation - could be enhanced to read from configuration
        DEFAULT_BATCH_MAX
    }

    /// Create an aggregate interest declaration for commands
    pub fn aggregate_for_commands(actor_id: &str, entity_name: &str, key_field: &str) -> Self {
        Self {
            actor_id: actor_id.into(),
            entity_name: entity_name.into(),
            key_field: key_field.into(),
            role: ActorRole::Aggregate,
            interest: ActorInterest::AggregateStream(entity_name.to_string()),
            interest_constraint: InterestConstraint::Commands,
        }
    }
}

impl fmt::Display for InterestDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.entity_name.clone();
        let roledesc = self.role.to_string();
        let constraint = self.interest_constraint.to_string();
        write!(f, "{} ({}) - source type: {}", name, roledesc, constraint)
    }
}

impl Hash for InterestDeclaration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state);
        self.entity_name.hash(state);
        self.role.hash(state);
        self.interest.hash(state);
        self.interest_constraint.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum InterestConstraint {
    Commands,
    Events,
}

impl fmt::Display for InterestConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InterestConstraint::Commands => write!(f, "commands"),
            InterestConstraint::Events => write!(f, "events"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum ActorRole {
    Aggregate,
    Projector,
    ProcessManager,
    Notifier,
    Unknown,
}

impl fmt::Display for ActorRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ActorRole::*;
        match self {
            Aggregate => {
                write!(f, "aggregate")
            }
            Projector => {
                write!(f, "projector")
            }
            ProcessManager => {
                write!(f, "process manager")
            }
            Notifier => {
                write!(f, "notifier")
            }
            Unknown => {
                write!(f, "unknown")
            }
        }
    }
}

impl From<String> for ActorRole {
    fn from(source: String) -> Self {
        use ActorRole::*;
        match source.trim().to_lowercase().as_ref() {
            "aggregate" => Aggregate,
            "notifier" => Notifier,
            "process_manager" => ProcessManager,
            "projector" => Projector,
            u => {
                eprintln!("Unknown role declared: {u}");
                Unknown
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum ActorInterest {
    AggregateStream(String),
    EventList(Vec<String>),
    ProcessManager(ProcessManagerLifetime),
    None,
}
/// A process manager lifetime defines the life cycle of a long running process. A long running process in this case is any
/// process that occurs over the span of more than one event, and does not necessarily correspond to a length of elapsed
/// time
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Hash, Eq)]
pub struct ProcessManagerLifetime {
    pub start: String,
    pub advance: Vec<String>,
    pub stop: Vec<String>,
}

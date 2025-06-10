use serde::{Deserialize, Serialize};
use async_nats::jetstream::Context;
use async_trait::async_trait;
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub stream: String,
}

#[async_trait]
pub trait AggregateService: Send + Sync {
    async fn handle_command(&self, ctx: &Context, cmd: &StatefulCommand) -> Result<EventList, String>;
    async fn apply_event(&self, ctx: &Context, event_with_state: &EventWithState) -> Result<StateAck, String>;
}

#[derive(Debug)]
pub struct StatefulCommand {
    pub aggregate: String,
    pub command_type: String,
    pub key: String,
    pub state: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventWithState {
    pub event: Event,
    #[serde(with = "serde_bytes")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Vec<u8>>,
}

pub type EventList = Vec<Event>;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct StateAck {
    /// Optional error message
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(with = "serde_bytes")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Vec<u8>>,
    #[serde(default)]
    pub succeeded: bool,
}
use anyhow::{Result, Error};
use crate::{consumers::RawCommand, eventsourcing::Event as ConcordanceEvent};
use case::CaseExt;
use chrono::Utc; // only using chrono because cloudevents SDK needs it
use cloudevents::AttributesReader;
use tracing::{error, instrument};
// use wasmbus_rpc::error::Error;

use cloudevents::{Event as CloudEvent, EventBuilder, EventBuilderV10};

pub(crate) const EVENT_TOPIC_PREFIX: &str = "cc.events";

impl From<ConcordanceEvent> for CloudEvent {
    fn from(event: ConcordanceEvent) -> Self {
        EventBuilderV10::new()
            .id(uuid::Uuid::new_v4().to_string())
            .source("concordance")
            .ty(event.event_type)
            .time(Utc::now())
            .build()
            .unwrap()
    }
}
pub(crate) const COMMAND_TOPIC_PREFIX: &str = "cc.commands";

pub(crate) const EXT_CONCORDANCE_STREAM: &str = "x-concordance-stream";


#[instrument(level = "debug", skip(nc))]
pub(crate) async fn publish_es_event(
    nc: &async_nats::Client,
    event: ConcordanceEvent,
) -> Result<()> {
    let evt_type = event.event_type.to_snake();
    let topic = format!("{EVENT_TOPIC_PREFIX}.{evt_type}"); // e.g. cc.events.amount_withdrawn

    let cloud_event: CloudEvent = event.into();
    let Ok(raw) = serde_json::to_vec(&cloud_event) else {
        error!("Failed to serialize a stock cloudevent. Something is very wrong.");
        return Err(Error::msg("Fatal serialization failure - could not serialize a cloud event".to_string()));
    };

    nc.request(topic, raw.into())
        .await
        .map_err(|e| Error::msg(e.to_string()))?;

    Ok(())
}

use crate::config::InterestDeclaration;
use crate::natsclient::{AckableMessage, DEFAULT_ACK_TIME};
use cloudevents::{Event as CloudEvent, AttributesWriter, AttributesReader};
use case::CaseExt;

use async_nats::{
    jetstream::{
        consumer::pull::{Config as PullConfig, Stream as MessageStream},
        stream::Stream as JsStream,
    },
    Error as NatsError,
};
use super::{impl_Stream, CreateConsumer};

#[allow(dead_code)]
pub struct EventConsumer {
    stream: MessageStream,
    interest: InterestDeclaration,
    name: String,
}

impl EventConsumer {
    pub fn sanitize_type_name(evt: CloudEvent) -> CloudEvent {
        let mut event = evt.clone();
        event.set_type(evt.ty().to_string().to_snake());
        event
    }

    pub async fn try_new(
        stream: JsStream,
        interest: InterestDeclaration,
    ) -> ::std::result::Result<EventConsumer, NatsError> {
        let consumer_name = interest.consumer_name();
        let friendly_name = interest.to_string();

        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!("Durable event consumer for {friendly_name}")),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: DEFAULT_ACK_TIME,
                    // poison pill identified after 3 nacks
                    max_deliver: 3,
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    // TODO: when NATS server and async nats client support it, convert this
                    // to declare explicit per-event interest rather than subscribing to all
                    //filter_subject: "cc.events.a,cc.events.b,etc".to_string(),
                    ..Default::default()
                },
            )
            .await?;

        let info = consumer.cached_info();
        let messages = consumer
            .stream()
            .max_messages_per_batch(interest.extract_max_messages_per_batch())
            .messages()
            .await?;
        Ok(EventConsumer {
            stream: messages,
            interest,
            name: info.name.to_string(),
        })
    }
}


// Creates a futures::Stream for EventConsumer, pulling items of type CloudEvent
impl_Stream!(EventConsumer; CloudEvent);

#[async_trait::async_trait]
impl CreateConsumer for EventConsumer {
    type Output = EventConsumer;

    async fn create(
        stream: async_nats::jetstream::stream::Stream,
        interest: InterestDeclaration,
    ) -> Result<Self::Output, NatsError> {
        EventConsumer::try_new(stream, interest).await
    }
}
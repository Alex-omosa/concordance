use tracing::{debug, instrument};
use async_nats::{
    jetstream::stream::{Config as StreamConfig, Stream},
    Error as NatsError,
};

use crate::{
    natsclient::{
        COMMANDS_STREAM_NAME, COMMANDS_STREAM_TOPIC, EVENTS_STREAM_TOPIC, EVENT_STREAM_NAME,
    },
  
};

pub(crate) struct NatsClient {
    context: async_nats::jetstream::Context,
    
}

impl NatsClient {
    pub fn new(js: async_nats::jetstream::Context) -> NatsClient {
       NatsClient { context: js }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn ensure_streams(&self) -> Result<(Stream, Stream), NatsError> {
       let event_stream = self
          .context
          .get_or_create_stream(StreamConfig {
             name: EVENT_STREAM_NAME.to_string(),
             description: Some(
                "Concordance event stream for event sourcing capability provider".to_string(),
             ),
             num_replicas: 1,
             retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
             subjects: vec![EVENTS_STREAM_TOPIC.to_owned()],
             storage: async_nats::jetstream::stream::StorageType::File,
             allow_rollup: false,
             ..Default::default()
          })
          .await?;

       let command_stream = self
          .context
          .get_or_create_stream(StreamConfig {
             name: COMMANDS_STREAM_NAME.to_string(),
             description: Some(
                "Concordance command stream for event sourcing capability provider".to_string(),
             ),
             num_replicas: 1,
             retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
             subjects: vec![COMMANDS_STREAM_TOPIC.to_owned()],
             storage: async_nats::jetstream::stream::StorageType::File,
             allow_rollup: false,
             ..Default::default()
          })
          .await?;

       debug!("Detected or created both CC_EVENTS and CC_COMMANDS");

       Ok((event_stream, command_stream))
    }
}

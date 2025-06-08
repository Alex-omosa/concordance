use crate::{
    natsclient::{AckableMessage, DEFAULT_ACK_TIME,},
    config::{ActorRole, InterestDeclaration}
};
use async_nats::{
    jetstream::{
        consumer::pull::{Config as PullConfig, Stream as MessageStream},
        stream::Stream as JsStream,
    },
    Error as NatsError,
};
// use futures::Stream;
use serde::{Deserialize, Serialize};
use super::{impl_Stream, CreateConsumer};

/// The JSON command as pulled off of the stream by way of a command consumer
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawCommand {
    pub command_type: String,
    pub key: String,
    pub data: serde_json::Value,
}

pub struct CommandConsumer {
    stream: MessageStream,
}

impl CommandConsumer {
    pub fn sanitize_type_name(cmd: RawCommand) -> RawCommand {
        RawCommand {
            command_type: Self::to_snake_case(&cmd.command_type),
            ..cmd
        }
    }

    // Simple snake_case conversion
    fn to_snake_case(s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c.is_uppercase() && !result.is_empty() {
                if let Some(&next_char) = chars.peek() {
                    if next_char.is_lowercase() || result.chars().last().unwrap().is_lowercase() {
                        result.push('_');
                    }
                }
            }
            result.push(c.to_lowercase().next().unwrap());
        }

        result
    }

    pub async fn try_new(
        stream: JsStream,
        interest: InterestDeclaration,
    ) -> Result<CommandConsumer, NatsError> {
        let consumer_name = interest.consumer_name();
        if interest.role != ActorRole::Aggregate {
            return Err(format!(
                "Only aggregates are allowed to receive commands, supplied role was {}",
                interest.role
            )
            .into());
        }
        let friendly_name = interest.to_string();
        let agg_name = interest.entity_name.clone();

        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!("Durable command consumer for {friendly_name}")),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: DEFAULT_ACK_TIME,
                    // poison pill identified after 3 nacks
                    max_deliver: 3,
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    filter_subject: format!("cc.commands.{agg_name}"),
                    ..Default::default()
                },
            )
            .await?;

        let messages = consumer
            .stream()
            .max_messages_per_batch(interest.extract_max_messages_per_batch())
            .messages()
            .await?;
        Ok(CommandConsumer { stream: messages })
    }
}


// Creates a futures::Stream for CommandConsumer, pulling items of type RawCommand
impl_Stream!(CommandConsumer; RawCommand);

#[async_trait::async_trait]
impl CreateConsumer for CommandConsumer {
    type Output = CommandConsumer;

    async fn create(
        stream: async_nats::jetstream::stream::Stream,
        interest: InterestDeclaration,
    ) -> Result<Self::Output, NatsError> {
        CommandConsumer::try_new(stream, interest).await
    }
}


// // Implement Stream for CommandConsumer following the inspiration framework pattern
// impl Stream for CommandConsumer {
//     type Item = Result<AckableMessage<RawCommand>, NatsError>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         match Pin::new(&mut self.stream).poll_next(cx) {
//             Poll::Ready(None) => Poll::Ready(None),
//             Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
//             Poll::Ready(Some(Ok(msg))) => {
//                 // Convert to RawCommand, skipping if we can't decode it
//                 let item: RawCommand = match serde_json::from_slice(&msg.payload) {
//                     Ok(item) => item,
//                     Err(e) => {
//                         eprintln!(
//                             "Unable to decode as RawCommand. Skipping message. Error: {:?}",
//                             e
//                         );
//                         eprintln!("Raw payload: {}", String::from_utf8_lossy(&msg.payload));

//                         // Async ack the invalid message to prevent redelivery loops
//                         let waker = cx.waker().clone();
//                         tokio::spawn(async move {
//                             if let Err(e) = msg.ack().await {
//                                 eprintln!("Error when trying to ack skipped message, message will be redelivered: {}", e);
//                             }
//                             waker.wake();
//                         });

//                         // Return Poll::Pending - it will wake up and try again after acking
//                         return Poll::Pending;
//                     }
//                 };

//                 // Sanitize the command type name following the framework pattern
//                 let sanitized_item = CommandConsumer::sanitize_type_name(item);

//                 // Wrap in AckableMessage
//                 Poll::Ready(Some(Ok(AckableMessage {
//                     inner: sanitized_item,
//                     acker: Some(msg),
//                 })))
//             }
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    config::{InterestConstraint, InterestDeclaration},
    natsclient::AckableMessage,
};
use async_nats::jetstream::stream::Stream as NatsStream;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use tracing::{error, trace, Instrument};

use super::{CreateConsumer, WorkError, WorkHandles, WorkResult, Worker};

#[derive(Clone, Debug)]
pub struct ConsumerManager {
    handles: WorkHandles,
    evt_stream: NatsStream,
    cmd_stream: NatsStream,
}

impl ConsumerManager {
    pub fn new(evt_stream: NatsStream, cmd_stream: NatsStream) -> ConsumerManager {
        ConsumerManager {
            handles: Arc::new(RwLock::new(HashMap::default())),
            evt_stream,
            cmd_stream,
        }
    }

    #[allow(unused)]
    pub async fn consumers(&self) -> Vec<InterestDeclaration> {
        let keys = {
            let lock = self.handles.read().await;
            lock.keys().cloned().collect()
        };
        keys
    }

    pub async fn add_consumer<W, C>(
        &self,
        interest: InterestDeclaration,
        worker: W,
    ) -> Result<(), async_nats::Error>
    where
        W: Worker + Send + Sync + 'static,
        C: Stream<Item = Result<AckableMessage<W::Message>, async_nats::Error>>
            + CreateConsumer<Output = C>
            + Send
            + Unpin
            + 'static,
    {
        let i = interest.clone();
        if !self.has_consumer(&interest).await {
            let consumer = if interest.interest_constraint == InterestConstraint::Commands {
                C::create(self.cmd_stream.clone(), interest.clone()).await?
            } else {
                C::create(self.evt_stream.clone(), interest.clone()).await?
            };

            let handle = tokio::spawn(
                work_fn(consumer, worker, interest)
                    .instrument(tracing::info_span!("consumer_worker", %i)),
            );
            let mut handles = self.handles.write().await;
            handles.insert(i.clone(), handle);
        }
        Ok(())
    }

    /// Checks if this manager has a consumer for the given interest declaration. Returns `false` if it doesn't
    /// exist or has stopped
    pub async fn has_consumer(&self, interest: &InterestDeclaration) -> bool {
        self.handles
            .read()
            .await
            .get(interest)
            .map(|handle| !handle.is_finished())
            .unwrap_or(false)
    }
}

async fn work_fn<C, W>(mut consumer: C, worker: W, _interest: InterestDeclaration) -> WorkResult<()>
where
    W: Worker + Send,
    C: Stream<Item = Result<AckableMessage<W::Message>, async_nats::Error>> + Unpin,
{
    loop {
        // Get next value from stream, returning error if the consumer stopped
        let res = consumer.next().await.ok_or(WorkError::ConsumerStopped)?;
        let res = match res {
            Ok(msg) => {
                trace!(message = ?msg, "Got message from consumer");
                worker.do_work(msg).await
            }
            Err(e) => {
                error!(error = %e, "Got error from stream when reading from consumer. Will try again");
                continue;
            }
        };
        match res {
            // Return fatal errors if they occur
            Err(e) if matches!(e, WorkError::Fatal(_)) => return Err(e),
            // For the rest of the errors, right now we just log. Could do nicer retry behavior as this evolves
            Err(e) => error!(error = ?e, "Got error from worker"),
            _ => (),
        }
    }
}

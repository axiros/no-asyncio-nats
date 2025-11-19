use crate::task::TaskQueueReceiver;
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;
use tokio_stream::StreamExt;

pub enum SubscriberCmd {
    Drain,
    Unsubscribe,
    UnsubscribeAfter(u64),
    RecvMsg(Option<Duration>)
}

#[derive(Debug)]
pub enum SubscriberCmdResponse {
    NoResponse,
    Message(Option<async_nats::Message>),
}

pub(crate) async fn main_loop(
    mut subscriber: async_nats::Subscriber,
    mut task_receiver: TaskQueueReceiver<SubscriberCmd, SubscriberCmdResponse>,
) {
    run_task_loop!(task_receiver, {
        SubscriberCmd::Drain => subscriber
            .drain()
            .await
            .map(|_| SubscriberCmdResponse::NoResponse)
            .map_err(|err| err.into()),
        SubscriberCmd::Unsubscribe => subscriber
            .unsubscribe()
            .await
            .map(|_| SubscriberCmdResponse::NoResponse)
            .map_err(|err| err.into()),
        SubscriberCmd::UnsubscribeAfter(count) => subscriber
            .unsubscribe_after(count)
            .await
            .map(|_| SubscriberCmdResponse::NoResponse)
            .map_err(|err| err.into()),
        SubscriberCmd::RecvMsg(timeout) => {
            run_recv_msg(&mut subscriber, timeout).await
        }
    });
}

async fn run_recv_msg(
    subscriber: &mut async_nats::Subscriber,
    timeout: Option<Duration>,
) -> anyhow::Result<SubscriberCmdResponse> {
    let msg = match timeout {
        Some(timeout) => match tokio_timeout(timeout, subscriber.next()).await {
            Ok(msg) => msg,
            Err(_) => None,
        },
        None => subscriber.next().await,
    };
    Ok(SubscriberCmdResponse::Message(msg))
}

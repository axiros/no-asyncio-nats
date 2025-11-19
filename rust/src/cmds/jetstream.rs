use std::time::Duration;
use tokio_stream::StreamExt;
use crate::task::TaskQueueReceiver;

pub(crate) enum JetStreamCmd {
    Publish {
        subject: String,
        payload: bytes::Bytes,
        headers: Option<async_nats::HeaderMap>,
    },
    GetOrCreateStream {
        config: async_nats::jetstream::stream::Config,
    },
    DeleteStream { stream: String },
    SetTimeout {timeout: Duration }
}

#[derive(Debug)]
pub(crate) enum JetStreamCmdResponse {
    NoResponse,
    PublishAck{future: async_nats::jetstream::context::PublishAckFuture},
    Stream{stream: async_nats::jetstream::stream::Stream},
    DeleteStatus{success: bool},
}

pub(crate) async fn main_loop(
    mut jetstream: async_nats::jetstream::Context,
    mut task_receiver: TaskQueueReceiver<JetStreamCmd, JetStreamCmdResponse>,
) {
    run_task_loop!(task_receiver, {
        JetStreamCmd::Publish{subject, payload, headers} =>
            run_publish(&jetstream, subject, payload, headers).await,
        JetStreamCmd::GetOrCreateStream { config } =>
            run_get_or_create_stream(&jetstream, config).await,
        JetStreamCmd::DeleteStream { stream } =>
            run_delete_stream(&jetstream, stream).await,
        JetStreamCmd::SetTimeout { timeout } =>
            run_set_timeout(&mut jetstream, timeout).await
    });
}

async fn run_publish(
    jetstream: &async_nats::jetstream::Context,
    subject: String,
    payload: bytes::Bytes,
    headers: Option<async_nats::HeaderMap>
) -> Result<JetStreamCmdResponse, anyhow::Error> {
    let result = match headers {
        Some(headers) => {
            jetstream
                .publish_with_headers(subject, headers, payload)
                .await?
        }
        None => {
            jetstream.publish(subject, payload).await?
        }
    };

    Ok(JetStreamCmdResponse::PublishAck{future: result})
}

async fn run_get_or_create_stream(
    jetstream: &async_nats::jetstream::Context,
    config: async_nats::jetstream::stream::Config,
) -> Result<JetStreamCmdResponse, anyhow::Error> {
    let stream = jetstream.get_or_create_stream(config).await?;
    Ok(JetStreamCmdResponse::Stream{stream})
}

async fn run_delete_stream(
    jetstream: &async_nats::jetstream::Context,
    stream: String,
) -> Result<JetStreamCmdResponse, anyhow::Error> {
    let status = jetstream.delete_stream(&stream).await?;
    Ok(JetStreamCmdResponse::DeleteStatus{success: status.success})
}

async fn run_set_timeout(
    jetstream: &mut async_nats::jetstream::Context,
    timeout: Duration,
) -> Result<JetStreamCmdResponse, anyhow::Error> {
    jetstream.set_timeout(timeout);
    Ok(JetStreamCmdResponse::NoResponse)
}

pub enum JetStreamStreamCmd{
    GetPullConsumer{name: String},
    GetOrCreatePullConsumer{
        name: String,
        config: async_nats::jetstream::consumer::pull::Config
    }
}

#[derive(Debug)]
pub enum JetStreamStreamCmdResponse {
    PullConsumer(async_nats::jetstream::consumer::Consumer<
        async_nats::jetstream::consumer::pull::Config>
    )
}


pub async fn stream_loop(
    stream: async_nats::jetstream::stream::Stream,
    mut task_receiver: TaskQueueReceiver<JetStreamStreamCmd, JetStreamStreamCmdResponse>
) {
    run_task_loop!(task_receiver, {
        JetStreamStreamCmd::GetPullConsumer { name } => {
            stream
                .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&name)
                .await
                .map_err(|err| anyhow::anyhow!("{err}"))
                .map(|ob| JetStreamStreamCmdResponse::PullConsumer(ob))
        },
        JetStreamStreamCmd::GetOrCreatePullConsumer { name, config } => {
            stream.get_or_create_consumer(&name, config)
                .await
                .map_err(|err| anyhow::anyhow!("{err}"))
                .map(|ob| JetStreamStreamCmdResponse::PullConsumer(ob))
        }
    });
}

pub type PullConsumerMessagesResult = Option<async_nats::jetstream::Message>;

pub async fn pull_consumer_messages_loop(
    mut stream: async_nats::jetstream::consumer::pull::Stream,
    mut task_receiver: TaskQueueReceiver<(), PullConsumerMessagesResult>,
) {
    run_task_loop!(task_receiver, {
        () => stream
            .next()
            .await
            .transpose()
            .map_err(|err| err.into())
    });
}

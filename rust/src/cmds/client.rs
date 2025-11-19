use crate::task::TaskQueueReceiver;

pub(crate) enum ClientCmd {
    Publish(ClientCmdPublish),
    Subscribe(String),
    QueueSubscribe(ClientCmdQueueSubscribe),
    Flush,
    Request(ClientCmdRequest),
    NewInbox,
    CreateJetStream,
}

pub(crate) struct ClientCmdPublish {
    pub(crate) subject: String,
    pub(crate) payload: bytes::Bytes,
    pub(crate) reply: Option<String>,
    pub(crate) headers: Option<async_nats::HeaderMap>,
}

pub(crate) struct ClientCmdQueueSubscribe {
    pub(crate) subject: String,
    pub(crate) queue_group: String,
}

pub(crate) struct ClientCmdRequest {
    pub(crate) subject: String,
    pub(crate) payload: bytes::Bytes,
    pub(crate) headers: Option<async_nats::HeaderMap>,
}

#[derive(Debug)]
pub(crate) enum ClientCmdResponse {
    NoResponse,
    Subscribe(async_nats::Subscriber),
    Message(async_nats::Message),
    Inbox(String),
    JetStream(async_nats::jetstream::Context),
}

pub(crate) async fn main_loop(
    client: async_nats::Client,
    mut task_receiver: TaskQueueReceiver<ClientCmd, ClientCmdResponse>,
) {
    run_task_loop!(task_receiver, {
        ClientCmd::Publish(publish) => run_publish(&client, publish).await,
        ClientCmd::Flush => run_flush(&client).await,
        ClientCmd::Subscribe(subject) => run_subscribe(&client, subject).await,
        ClientCmd::QueueSubscribe(queue_subscribe) => {
            run_queue_subscribe(&client, queue_subscribe).await
        }
        ClientCmd::Request(request) => run_request(&client, request).await,
        ClientCmd::NewInbox => run_new_inbox(&client).await,
        ClientCmd::CreateJetStream => run_create_jetstream(&client).await,
    });
}

async fn run_publish(
    client: &async_nats::Client,
    publish: ClientCmdPublish,
) -> anyhow::Result<ClientCmdResponse> {
    match (publish.reply, publish.headers) {
        (Some(reply), Some(headers)) => {
            client
                .publish_with_reply_and_headers(
                    publish.subject,
                    reply,
                    headers,
                    publish.payload,
                )
                .await
        }
        (Some(reply), None) => {
            client
                .publish_with_reply(publish.subject, reply, publish.payload)
                .await
        }
        (None, Some(headers)) => {
            client
                .publish_with_headers(publish.subject, headers, publish.payload)
                .await
        }
        (None, None) => client.publish(publish.subject, publish.payload).await,
    }?;
    Ok(ClientCmdResponse::NoResponse)
}

async fn run_flush(client: &async_nats::Client) -> anyhow::Result<ClientCmdResponse> {
    client.flush().await?;
    Ok(ClientCmdResponse::NoResponse)
}

async fn run_subscribe(
    client: &async_nats::Client,
    subject: String,
) -> anyhow::Result<ClientCmdResponse> {
    Ok(ClientCmdResponse::Subscribe(
        client.subscribe(subject).await?,
    ))
}

async fn run_request(
    client: &async_nats::Client,
    request: ClientCmdRequest,
) -> anyhow::Result<ClientCmdResponse> {
    let msg = match request.headers {
        Some(headers) => {
            client
                .request_with_headers(request.subject, headers, request.payload)
                .await?
        }
        None => client.request(request.subject, request.payload).await?,
    };
    Ok(ClientCmdResponse::Message(msg))
}

async fn run_new_inbox(client: &async_nats::Client) -> anyhow::Result<ClientCmdResponse> {
    let inbox = client.new_inbox();
    Ok(ClientCmdResponse::Inbox(inbox))
}

async fn run_queue_subscribe(
    client: &async_nats::Client,
    queue_subscribe: ClientCmdQueueSubscribe,
) -> anyhow::Result<ClientCmdResponse> {
    let subscriber = client
        .queue_subscribe(queue_subscribe.subject, queue_subscribe.queue_group)
        .await?;
    Ok(ClientCmdResponse::Subscribe(subscriber))
}

async fn run_create_jetstream(client: &async_nats::Client
) -> anyhow::Result<ClientCmdResponse> {
    let jetstream = async_nats::jetstream::new(client.clone());
    Ok(ClientCmdResponse::JetStream(jetstream))
}

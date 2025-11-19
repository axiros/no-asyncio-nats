use pyo3::prelude::*;

use crate::bindings::converters::headermap::HeaderMap;
use crate::bindings::converters::message::Message;

use crate::cmds::client::ClientCmd;
use crate::cmds::client::ClientCmdPublish;
use crate::cmds::client::ClientCmdQueueSubscribe;
use crate::cmds::client::ClientCmdRequest;
use crate::cmds::client::ClientCmdResponse;

use crate::bindings::subscriber::Subscriber;
use crate::bindings::jetstream::JetStream;

#[pyclass]
pub(crate) struct Client {
    pub(crate) task_caller: crate::task::TaskCaller<ClientCmd, ClientCmdResponse>,
    pub(crate) task_spawner: crate::task::TaskSpawner
}


#[pymethods]
impl Client {
    #[pyo3(signature = (subject, data, reply=None, headers=None))]
    fn publish(
        &self,
        py: Python,
        subject: String,
        data: &[u8],
        reply: Option<String>,
        headers: Option<HeaderMap>,
    ) -> anyhow::Result<Py<PyAny>> {
        let payload = bytes::Bytes::copy_from_slice(data);
        let command = ClientCmdPublish {
            subject,
            payload,
            reply,
            headers: headers.map(Into::into)
        };
        self.send_command(py, ClientCmd::Publish(command))
    }

    fn flush(
        &self,
        py: Python,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(py, ClientCmd::Flush)
    }

    fn subscribe(
        &self,
        py: Python,
        subject: String,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(py, ClientCmd::Subscribe(subject))
    }

    fn queue_subscribe(
        &self,
        py: Python,
        subject: String,
        queue_group: String,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(
            py,
            ClientCmd::QueueSubscribe(ClientCmdQueueSubscribe {
                subject,
                queue_group,
            }),
        )
    }

    #[pyo3(signature = (subject, data, headers=None))]
    fn request(
        &self,
        py: Python,
        subject: String,
        data: &[u8],
        headers: Option<HeaderMap>,
    ) -> anyhow::Result<Py<PyAny>> {
        let payload = bytes::Bytes::copy_from_slice(data);
        let command = ClientCmdRequest {
            subject,
            payload,
            headers: headers.map(Into::into)
        };
        self.send_command(py, ClientCmd::Request(command))
    }

    fn new_inbox(
        &self,
        py: Python,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(py, ClientCmd::NewInbox)
    }

    fn jetstream(
        &self,
        py: Python,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(py, ClientCmd::CreateJetStream)

    }
}

impl Client {
    fn send_command(
        &self,
        py: Python,
        command: ClientCmd,
    ) -> anyhow::Result<Py<PyAny>> {
        let response = self.task_caller.req_response(py, command)?;
        self.response_to_py(py, response?)
    }

    fn response_to_py(
        &self,
        py: Python,
        response: ClientCmdResponse,
    ) -> anyhow::Result<Py<PyAny>> {
        match response {
            ClientCmdResponse::NoResponse => Ok(py.None().into_bound(py).into()),
            ClientCmdResponse::Subscribe(nats_subscriber) => {
                let (task_caller, queue) =
                    crate::task::TaskCaller::from_other(py, &self.task_caller);

                self.task_spawner.spawn(async move {
                    crate::cmds::subscriber::main_loop(nats_subscriber, queue)
                        .await
                });

                Ok(Py::new(py, Subscriber { task_caller })?.into())
            }
            ClientCmdResponse::Message(msg) => Ok(Message::new(msg).into_pyobject(py)?.into()),
            ClientCmdResponse::Inbox(inbox) => Ok(inbox.into_pyobject(py)?.into()),
            ClientCmdResponse::JetStream(nats_jetstream) => {
                let (task_caller, queue) =
                    crate::task::TaskCaller::from_other(py, &self.task_caller);

                self.task_spawner.spawn(async move {
                    crate::cmds::jetstream::main_loop(nats_jetstream, queue)
                        .await
                });

                let jetstream = JetStream {
                    task_caller,
                    task_spawner: self.task_spawner.make_clone(py)
                };

                Ok(Py::new(py, jetstream)?.into())
            }
        }
    }
}


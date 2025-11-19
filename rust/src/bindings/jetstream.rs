use pyo3::prelude::*;
use std::time::Duration;

use crate::bindings::converters;
use crate::bindings::converters::headermap::HeaderMap;

use crate::cmds::jetstream::JetStreamCmd;
use crate::cmds::jetstream::JetStreamCmdResponse;

pub(crate) mod publish_ack;
pub(crate) mod stream;

#[pyclass]
pub(crate) struct JetStream {
    pub(crate) task_caller: crate::task::TaskCaller<JetStreamCmd, JetStreamCmdResponse>,
    pub(crate) task_spawner: crate::task::TaskSpawner
}

#[pymethods]
impl JetStream {
    fn set_timeout(&self, py: Python, timeout: Duration) -> anyhow::Result<Py<PyAny>> {
        let command = JetStreamCmd::SetTimeout { timeout };
        self.send_command(py, command)
    }

    #[pyo3(signature = (subject, data, headers=None))]
    fn publish(
        &self,
        py: Python,
        subject: String,
        data: &[u8],
        headers: Option<HeaderMap>
    ) -> anyhow::Result<Py<PyAny>> {
        let payload_bytes = bytes::Bytes::copy_from_slice(data);
        let command = JetStreamCmd::Publish {
            subject,
            payload: payload_bytes,
            headers: headers.map(Into::into)
        };
        self.send_command(py, command)
    }

    fn get_or_create_stream(
        &self,
        py: Python,
        stream_config: &Bound<'_, pyo3::types::PyDict>,
    ) -> anyhow::Result<Py<PyAny>> {
        let config = converters::jetstream::stream::py_to_stream_config(stream_config)?;
        let command = JetStreamCmd::GetOrCreateStream { config };
        self.send_command(py, command)
    }

    fn delete_stream(&self, py: Python, stream: String) -> anyhow::Result<Py<PyAny>> {
        let command = JetStreamCmd::DeleteStream { stream };
        self.send_command(py, command)
    }
}

impl JetStream {
    fn send_command(
        &self,
        py: Python,
        command: JetStreamCmd,
    ) -> anyhow::Result<Py<PyAny>> {
        let response = self.task_caller.req_response(py, command)?;
        self.response_to_py(py, response?)
    }

    fn response_to_py(
        &self,
        py: Python,
        response: JetStreamCmdResponse,
    ) -> anyhow::Result<Py<PyAny>> {
        match response {
            JetStreamCmdResponse::NoResponse => Ok(py.None().into_bound(py).into()),
            JetStreamCmdResponse::DeleteStatus { success } => {
                Ok(success.into_pyobject(py)?.to_owned().into())
            },
            JetStreamCmdResponse::PublishAck { future } => {
                let future = publish_ack::PublishAckFuture{
                    future: Some(future),
                    task_spawner: self.task_spawner.make_clone(py)
                };
                Ok(Py::new(py, future)?.into())
            },
            JetStreamCmdResponse::Stream{stream} => {
                let (task_caller, queue) =
                    crate::task::TaskCaller::from_other(py, &self.task_caller);

                self.task_spawner.spawn(async move {
                    crate::cmds::jetstream::stream_loop(stream, queue)
                        .await
                });

                let stream = stream::JetStreamStream{
                    task_caller: task_caller,
                    task_spawner: self.task_spawner.make_clone(py)
                };

                Ok(Py::new(py, stream)?.into())
            }
        }
    }
}

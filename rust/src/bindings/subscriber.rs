use pyo3::prelude::*;
use std::time::Duration;

use crate::bindings::converters::message::Message;
use crate::cmds::subscriber::SubscriberCmd;
use crate::cmds::subscriber::SubscriberCmdResponse;


#[pyclass]
pub(crate) struct Subscriber {
    pub(crate) task_caller: crate::task::TaskCaller<SubscriberCmd, SubscriberCmdResponse>
}

#[pymethods]
impl Subscriber {
    fn drain(
        &self,
        py: Python,
    ) -> anyhow::Result<Py<PyAny>> {
        let command = SubscriberCmd::Drain;
        self.send_command(py, command)
    }

    fn unsubscribe(
        &self,
        py: Python,
    ) -> anyhow::Result<Py<PyAny>> {
        let command = SubscriberCmd::Unsubscribe;
        self.send_command(py, command)
    }

    fn unsubscribe_after(
        &self,
        py: Python,
        count: u64,
    ) -> anyhow::Result<Py<PyAny>> {
        let command = SubscriberCmd::UnsubscribeAfter(count);
        self.send_command(py, command)
    }

    fn recv_msg(
        &self,
        py: Python,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Py<PyAny>> {
        self.send_command(py, SubscriberCmd::RecvMsg(timeout))
    }
}

impl Subscriber {
    fn send_command(
        &self,
        py: Python,
        command: SubscriberCmd,
    ) -> anyhow::Result<Py<PyAny>> {
        let response = self.task_caller.req_response(py, command)?;
        self.response_to_py(py, response?)
    }

    fn response_to_py(
        &self,
        py: Python,
        response: SubscriberCmdResponse,
    ) -> anyhow::Result<Py<PyAny>> {
        match response {
            SubscriberCmdResponse::NoResponse => Ok(py.None().into_bound(py).into()),
            SubscriberCmdResponse::Message(msg) => match msg {
                None => Ok(py.None().into()),
                Some(msg) => Ok(Message::new(msg).into_pyobject(py)?.into()),
            },
        }
    }
}
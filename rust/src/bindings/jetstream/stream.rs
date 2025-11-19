use pyo3::prelude::*;

use crate::task::TaskCaller;
use crate::bindings::converters;
use crate::bindings::converters::message::Message;


#[pyclass]
pub(crate) struct JetStreamStream {
    pub(crate) task_caller: TaskCaller<crate::cmds::jetstream::JetStreamStreamCmd, crate::cmds::jetstream::JetStreamStreamCmdResponse>,
    pub(crate) task_spawner: crate::task::TaskSpawner
}

#[pymethods]
impl JetStreamStream {

    fn get_consumer(&self, py: Python, name: String, typename: String, ordered: bool) -> anyhow::Result<Py<PyAny>> {
        let consumer: Py<PyAny> = match (typename.as_str(), ordered) {
            ("pull", false) => {
                let result = self.task_caller.req_response(
                    py, crate::cmds::jetstream::JetStreamStreamCmd::GetPullConsumer { name }
                )?;

                let crate::cmds::jetstream::JetStreamStreamCmdResponse::PullConsumer(consumer) = result?;
                let consumer = JetStreamPullConsumer{consumer, task_spawner: self.task_spawner.make_clone(py)};
                Py::new(py, consumer)?.into()
            },
            (t, o) => anyhow::bail!("Unsupported Consumer. Type:{t} Order:{o}")
        };

        Ok(consumer)
    }
    fn get_or_create_consumer(&self, py: Python, name: String, typename: String, ordered: bool, config: &Bound<pyo3::types::PyDict>) -> anyhow::Result<Py<PyAny>> {
        let consumer_config = converters::jetstream::consumer::py_to_consumer_config(config)?;
        let consumer: Py<PyAny> = match (typename.as_str(), ordered) {
            ("pull", false) => {
                let result = self.task_caller.req_response(
                    py, crate::cmds::jetstream::JetStreamStreamCmd::GetOrCreatePullConsumer{
                        name,
                        config: consumer_config
                    }
                )?;

                let crate::cmds::jetstream::JetStreamStreamCmdResponse::PullConsumer(consumer) = result?;
                let consumer = JetStreamPullConsumer{consumer, task_spawner: self.task_spawner.make_clone(py)};
                Py::new(py, consumer)?.into()
            },
            (t, o) => anyhow::bail!("Unsupported Consumer. Type:{t} Order:{o}")
        };

        Ok(consumer)
    }
}

#[pyclass]
struct JetStreamPullConsumer {
    task_spawner: crate::task::TaskSpawner,
    consumer: async_nats::jetstream::consumer::PullConsumer
}

#[pymethods]
impl JetStreamPullConsumer {
    fn make_receiver(&self, py: Python) -> anyhow::Result<JetStreamPullConsumerMessages> {
        let (task_caller, queue) =
            crate::task::TaskCaller::new(self.task_spawner.read_eventfd.clone_ref(py));

        let consumer = self.consumer.clone();
        let stream = self.task_spawner.spawn_blocking(py, async move {
            consumer.messages().await
        })??;

        self.task_spawner.spawn(async move {
            crate::cmds::jetstream::pull_consumer_messages_loop(stream, queue)
                .await
        });

        Ok(JetStreamPullConsumerMessages{task_caller})
    }
}

#[pyclass]
struct JetStreamPullConsumerMessages {
    task_caller: crate::task::TaskCaller<(), crate::cmds::jetstream::PullConsumerMessagesResult>,
}

#[pymethods]
impl JetStreamPullConsumerMessages {
    fn recv_msg(&mut self, py: Python) -> anyhow::Result<Option<Message>> {
        let result = self.task_caller.req_response(py, ())?;
        let Some(result) = result? else {
            return Ok(None);
        };

        Ok(Some(Message::new(result.message)))
    }
}
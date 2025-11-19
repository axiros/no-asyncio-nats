use pyo3::Py;
use pyo3::PyAny;
use pyo3::types::PyAnyMethods;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::eventfd::EventFd;
use crate::eventfd::Sender as EventFdSender;

type TaskResult<R> = anyhow::Result<R>;

pub(crate) struct Task<C, R> {
    pub command: C,
    pub event_fd_sender: EventFdSender,
    pub result_channel: oneshot::Sender<TaskResult<R>>,
}

impl<C, R> Task<C, R> {
    fn new(event_fd: &EventFd, command: C) -> (oneshot::Receiver<TaskResult<R>>, Self) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (
            receiver,
            Task {
                command,
                event_fd_sender: event_fd.make_sender(),
                result_channel: sender,
            },
        )
    }
}

pub(crate) type TaskQueueReceiver<C, R> = mpsc::UnboundedReceiver<Task<C, R>>;
pub(crate) type TaskQueueSender<C, R> = mpsc::UnboundedSender<Task<C, R>>;

pub(crate) struct TaskCaller<C, R> {
    task_queue: TaskQueueSender<C, R>,
    read_eventfd: Py<PyAny>,
}

impl<C, R> TaskCaller<C, R>
where
    C: Sync + Send + 'static,
    R: Sync + Send + 'static,
{
    pub fn new(read_eventfd: Py<PyAny>) -> (Self, TaskQueueReceiver<C, R>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let caller = TaskCaller {
            task_queue: sender,
            read_eventfd,
        };

        (caller, receiver)
    }

    pub fn from_other<OC, OR>(py: pyo3::Python, other: &TaskCaller<OC, OR>) -> (Self, TaskQueueReceiver<C, R>) {
        Self::new(other.read_eventfd.clone_ref(py))
    }

    pub fn req_response(&self, py: pyo3::Python, command: C) -> anyhow::Result<TaskResult<R>> {
        let event_fd = EventFd::new()?;
        let (mut result_channel, task) = Task::new(&event_fd, command);

        self.task_queue.send(task)?;

        self.read_eventfd.bind(py).call1((event_fd.as_raw_fd(),))?;
        Ok(result_channel.try_recv()?)
    }
}

macro_rules! run_task_loop {
    ($task_receiver:ident, { $($command_match:tt)* }) => {
        loop {
            let Some(task) = $task_receiver.recv().await else {
                break;
            };

            let result = match task.command {
                $($command_match)*
            };

            if matches!(task.result_channel.send(result), Err(_)) {
                eprintln!(
                    "result_channel is gone. Should not happen: {}",
                    std::backtrace::Backtrace::force_capture()
                );
            }

            std::mem::drop(task.event_fd_sender);
        }
    };
}

pub(crate) struct TaskSpawner {
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) read_eventfd: Py<PyAny>,
}

impl TaskSpawner {
    pub(crate) fn make_clone(&self, py: pyo3::Python) -> Self {
        return TaskSpawner {
            rt_handle: self.rt_handle.clone(),
            read_eventfd: self.read_eventfd.clone_ref(py),
        };
    }

    pub(crate) fn spawn<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.rt_handle.spawn(future);
    }

    pub(crate) fn spawn_blocking<F>(&self, py: pyo3::Python, future: F) -> anyhow::Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (event_fd, event_fd_sender) = crate::eventfd::make_pair()?;
        let (sender, mut receiver) = tokio::sync::oneshot::channel();

        self.rt_handle.spawn(async move {
            let result = future.await;
            if matches!(sender.send(result), Err(_)) {
                eprintln!(
                    "result_channel is gone. Should not happen: {}",
                    std::backtrace::Backtrace::force_capture()
                );
            }
            std::mem::drop(event_fd_sender);
        });

        self.read_eventfd.bind(py).call1((event_fd.as_raw_fd(),))?;
        Ok(receiver.try_recv()?)
    }
}

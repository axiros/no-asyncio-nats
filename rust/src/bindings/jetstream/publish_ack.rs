use pyo3::prelude::*;

#[pyclass]
pub(crate) struct PublishAckFuture {
    pub(crate) task_spawner: crate::task::TaskSpawner,
    pub(crate) future: Option<async_nats::jetstream::context::PublishAckFuture>
}

#[pymethods]
impl PublishAckFuture {
    fn wait(&mut self, py: Python) -> anyhow::Result<Py<PyAny>> {
        let Some(future) = self.future.take() else {
            return Ok(py.None());
        };

        let response = self.task_spawner.spawn_blocking(
            py,
            future.into_future()
        )??;

        let py_result = pyo3::types::PyDict::new(py);
        py_result.set_item("stream", response.stream.as_str())?;
        py_result.set_item("sequence", response.sequence)?;
        py_result.set_item("domain", response.domain.as_str())?;
        py_result.set_item("duplicate", response.duplicate)?;
        py_result.set_item("value", response.value)?;
        
        Ok(py_result.into())
    }
}
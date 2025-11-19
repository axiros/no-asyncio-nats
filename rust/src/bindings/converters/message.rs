use pyo3::prelude::*;

use crate::bindings::converters::headermap::HeaderMap;

pub(crate) struct Message {
    inner: async_nats::Message
}

impl Message {
    pub(crate) fn new(msg: async_nats::Message) -> Self {
        Message { inner: msg }
    }
}

impl<'py> IntoPyObject<'py> for Message {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = anyhow::Error;

    fn into_pyobject(
        self,
        py: Python<'py>,
    ) -> Result<Self::Output, Self::Error> {
        let result = pyo3::types::PyDict::new(py);

        result.set_item("subject", self.inner.subject.as_str())?;

        if let Some(reply) = self.inner.reply {
            result.set_item("reply", reply.as_str())?;
        }

        result.set_item("payload", self.inner.payload.as_ref())?;

        if let Some(headers) = self.inner.headers {
            result.set_item("headers", HeaderMap::new(headers))?;
        }

        if let Some(status) = self.inner.status {
            result.set_item("status", status.as_u16())?;
        }

        if let Some(description) = self.inner.description {
            result.set_item("description", description)?;
        }

        Ok(result.into_any())
    }
}

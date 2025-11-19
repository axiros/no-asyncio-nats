use pyo3::prelude::*;

pub(crate) struct HeaderMap {
    inner: async_nats::HeaderMap
}

impl HeaderMap {
    pub(crate) fn new(headers: async_nats::HeaderMap) -> Self {
        HeaderMap { inner: headers }
    }


}

impl From<HeaderMap> for async_nats::HeaderMap {
    fn from(ob: HeaderMap) -> Self {
        ob.inner
    }
}

impl<'py> IntoPyObject<'py> for HeaderMap {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = anyhow::Error;

    fn into_pyobject(
        self,
        py: Python<'py>,
    ) -> Result<Self::Output, Self::Error> {
        let result = pyo3::types::PyDict::new(py);
        for (name, value) in self.inner.iter() {
            if let Some(value) = value.last() {
                result
                    .set_item(AsRef::<str>::as_ref(name), AsRef::<str>::as_ref(value))?;
            }
        }

        Ok(result.into_any())
    }
}

impl FromPyObject<'_, '_> for HeaderMap {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> { 
        let mut result = async_nats::HeaderMap::new();

        for (key, value) in obj.cast::<pyo3::types::PyDict>()?.iter() {
            result.append(key.extract::<&str>()?, value.extract::<&str>()?)
        }

        Ok(HeaderMap{inner: result})
    }
}
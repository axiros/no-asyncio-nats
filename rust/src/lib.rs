use pyo3::prelude::*;

mod eventfd;

#[macro_use]
mod task;

mod cmds {
    pub(crate) mod client;
    pub(crate) mod subscriber;
    pub(crate) mod jetstream;

}

mod bindings {
    pub(crate) mod client;
    pub(crate) mod subscriber;
    pub(crate) mod jetstream;
    pub(crate) mod converters {
        pub(crate) mod connect_options;
        pub(crate) mod offset_datetime;
        pub(crate) mod headermap;
        pub(crate) mod message;
        pub(crate) mod jetstream {
            pub(crate) mod stream;
            pub(crate) mod consumer;
        }
    }
}

use crate::bindings::converters;

#[pyfunction]
#[pyo3(signature = (address, read_eventfd, options=None))]
fn connect(
    address: String,
    read_eventfd: Py<PyAny>,
    options: Option<std::collections::HashMap<String, Bound<PyAny>>>,
    py: Python,
) -> anyhow::Result<crate::bindings::client::Client> {
    let (task_caller, queue) = crate::task::TaskCaller::new(read_eventfd.clone_ref(py));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let connect_options = converters::connect_options::py_to_connect_options(
        &runtime,
        options
    )?;

    let client = runtime.block_on(async {
        async_nats::connect_with_options(address, connect_options).await
    })?;

    let rt_handle = runtime.handle().clone();

    std::thread::spawn(move || {
        runtime.block_on(async move {
            crate::cmds::client::main_loop(client, queue).await;
        });
    });


    Ok(crate::bindings::client::Client {
        task_caller,
        task_spawner: crate::task::TaskSpawner{rt_handle, read_eventfd}
    })
}

#[pymodule]
fn no_asyncio_nats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<crate::bindings::client::Client>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}

use pyo3::prelude::*;
use std::time::Duration;

pub fn py_to_connect_options(
    runtime: &tokio::runtime::Runtime,
    options: Option<std::collections::HashMap<String, Bound<PyAny>>>
) -> anyhow::Result<async_nats::ConnectOptions> {
    let mut connect_options = async_nats::ConnectOptions::new();

    if let Some(options) = options {
        // Authentication options
        if let Some(token) = options.get("token") {
            let token_str: String = token.extract()?;
            connect_options = connect_options.token(token_str);
        }

        if let Some(user_pass) = options.get("user_and_password") {
            let (user, pass): (String, String) = user_pass.extract()?;
            connect_options = connect_options.user_and_password(user, pass);
        }

        if let Some(nkey) = options.get("nkey") {
            let nkey_str: String = nkey.extract()?;
            connect_options = connect_options.nkey(nkey_str);
        }

        if let Some(credentials_file) = options.get("credentials_file") {
            let path: std::path::PathBuf = credentials_file.extract()?;
            connect_options = runtime
                .block_on(async { connect_options.credentials_file(path).await })?;
        }

        if let Some(credentials) = options.get("credentials") {
            let creds_str: String = credentials.extract()?;
            connect_options = connect_options.credentials(&creds_str)?;
        }

        // TLS options
        if let Some(tls_root_certificates) = options.get("tls_root_certificates") {
            let path: std::path::PathBuf = tls_root_certificates.extract()?;
            connect_options = connect_options.add_root_certificates(path);
        }

        let tls_client = (
            options.get("tls_client_cert"),
            options.get("tls_client_key"),
        );
        if let (Some(tls_client_cert), Some(tls_client_key)) = tls_client {
            let cert_path: std::path::PathBuf = tls_client_cert.extract()?;
            let key_path: std::path::PathBuf = tls_client_key.extract()?;
            connect_options = connect_options.add_client_certificate(cert_path, key_path);
        }

        if let Some(require_tls) = options.get("require_tls") {
            let require: bool = require_tls.extract()?;
            connect_options = connect_options.require_tls(require);
        }

        if let Some(tls_first) = options.get("tls_first") {
            if tls_first.is_truthy()? {
                connect_options = connect_options.tls_first();
            }
        }

        // Timeout and performance options
        if let Some(ping_interval) = options.get("ping_interval") {
            let duration: Duration = ping_interval.extract()?;
            connect_options = connect_options.ping_interval(duration);
        }

        if let Some(connection_timeout) = options.get("connection_timeout") {
            let duration: Duration = connection_timeout.extract()?;
            connect_options = connect_options.connection_timeout(duration);
        }

        if let Some(request_timeout) = options.get("request_timeout") {
            let duration: Duration = request_timeout.extract()?;
            connect_options = connect_options.request_timeout(Some(duration));
        }

        if let Some(subscription_capacity) = options.get("subscription_capacity") {
            let capacity: usize = subscription_capacity.extract()?;
            connect_options = connect_options.subscription_capacity(capacity);
        }

        if let Some(read_buffer_capacity) = options.get("read_buffer_capacity") {
            let size: u16 = read_buffer_capacity.extract()?;
            connect_options = connect_options.read_buffer_capacity(size);
        }

        // Connection behavior options
        if let Some(no_echo) = options.get("no_echo") {
            if no_echo.is_truthy()? {
                connect_options = connect_options.no_echo();
            }
        }

        if let Some(custom_inbox_prefix) = options.get("custom_inbox_prefix") {
            let prefix: String = custom_inbox_prefix.extract()?;
            connect_options = connect_options.custom_inbox_prefix(prefix);
        }

        if let Some(name) = options.get("name") {
            let name_str: String = name.extract()?;
            connect_options = connect_options.name(name_str);
        }

        if let Some(retry_on_initial_connect) = options.get("retry_on_initial_connect") {
            if retry_on_initial_connect.is_truthy()? {
                connect_options = connect_options.retry_on_initial_connect();
            }
        }

        if let Some(max_reconnects) = options.get("max_reconnects") {
            let max: usize = max_reconnects.extract()?;
            connect_options = connect_options.max_reconnects(Some(max));
        }

        if let Some(ignore_discovered_servers) = options.get("ignore_discovered_servers")
        {
            if ignore_discovered_servers.is_truthy()? {
                connect_options = connect_options.ignore_discovered_servers();
            }
        }

        if let Some(retain_servers_order) = options.get("retain_servers_order") {
            if retain_servers_order.is_truthy()? {
                connect_options = connect_options.retain_servers_order();
            }
        }
    };

    Ok(connect_options)
}
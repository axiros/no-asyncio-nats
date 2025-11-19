use pyo3::prelude::*;
use std::time::Duration;
use crate::bindings::converters::offset_datetime::py_to_offset_datetime;

pub fn py_to_consumer_config(
    py_config_dict: &Bound<pyo3::types::PyDict>
) -> anyhow::Result<async_nats::jetstream::consumer::pull::Config> {
    
    let mut consumer_config = async_nats::jetstream::consumer::pull::Config::default();

    // Handle optional string fields
    if let Some(value) = py_config_dict.get_item("durable_name")? {
        consumer_config.durable_name = value.extract::<Option<String>>()?;
    }

    if let Some(value) = py_config_dict.get_item("name")? {
        consumer_config.name = value.extract::<Option<String>>()?;
    }

    if let Some(value) = py_config_dict.get_item("description")? {
        consumer_config.description = value.extract::<Option<String>>()?;
    }

    if let Some(value) = py_config_dict.get_item("filter_subject")? {
        consumer_config.filter_subject = value.extract::<String>()?;
    }

    // Handle filter_subjects (Vec<String>)
    if let Some(value) = py_config_dict.get_item("filter_subjects")? {
        consumer_config.filter_subjects = value.extract::<Vec<String>>()?;
    }

    // Handle deliver_policy enum
    if let Some(value) = py_config_dict.get_item("deliver_policy")? {
        let policy: String = value.extract()?;
        consumer_config.deliver_policy = str_to_deliver_policy(&policy)?;
    }

    // Handle ack_policy enum
    if let Some(value) = py_config_dict.get_item("ack_policy")? {
        let policy: String = value.extract()?;
        consumer_config.ack_policy = str_to_ack_policy(&policy)?;
    }

    // Handle replay_policy enum
    if let Some(value) = py_config_dict.get_item("replay_policy")? {
        let policy: String = value.extract()?;
        consumer_config.replay_policy = str_to_replay_policy(&policy)?;
    }

    // Handle priority_policy enum
    if let Some(value) = py_config_dict.get_item("priority_policy")? {
        let policy: String = value.extract()?;
        consumer_config.priority_policy = str_to_priority_policy(&policy)?;
    }

    // Handle Duration fields
    if let Some(value) = py_config_dict.get_item("ack_wait")? {
        consumer_config.ack_wait = value.extract::<Duration>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_expires")? {
        consumer_config.max_expires = value.extract::<Duration>()?;
    }

    if let Some(value) = py_config_dict.get_item("inactive_threshold")? {
        consumer_config.inactive_threshold = value.extract::<Duration>()?;
    }

    // Handle numeric fields
    if let Some(value) = py_config_dict.get_item("max_deliver")? {
        consumer_config.max_deliver = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("rate_limit")? {
        consumer_config.rate_limit = value.extract::<u64>()?;
    }

    if let Some(value) = py_config_dict.get_item("sample_frequency")? {
        consumer_config.sample_frequency = value.extract::<u8>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_waiting")? {
        consumer_config.max_waiting = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_ack_pending")? {
        consumer_config.max_ack_pending = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_batch")? {
        consumer_config.max_batch = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_bytes")? {
        consumer_config.max_bytes = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("num_replicas")? {
        consumer_config.num_replicas = value.extract::<usize>()?;
    }

    // Handle boolean fields
    if let Some(value) = py_config_dict.get_item("headers_only")? {
        consumer_config.headers_only = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("memory_storage")? {
        consumer_config.memory_storage = value.is_truthy()?;
    }

    // Handle metadata HashMap
    if let Some(value) = py_config_dict.get_item("metadata")? {
        consumer_config.metadata = value.extract::<std::collections::HashMap<String, String>>()?;
    }

    // Handle backoff Vec<Duration>
    if let Some(value) = py_config_dict.get_item("backoff")? {
        consumer_config.backoff = value.extract::<Vec<Duration>>()?;
    }

    // Handle priority_groups Vec<String>
    if let Some(value) = py_config_dict.get_item("priority_groups")? {
        consumer_config.priority_groups = value.extract::<Vec<String>>()?;
    }

    // Handle pause_until Option<OffsetDateTime>
    if let Some(value) = py_config_dict.get_item("pause_until")? {
        consumer_config.pause_until = Some(py_to_offset_datetime(&value)?);
    }

    Ok(consumer_config)
}

fn str_to_deliver_policy(
    policy_str: &str
) -> anyhow::Result<async_nats::jetstream::consumer::DeliverPolicy> {
    match policy_str {
        "all" => Ok(async_nats::jetstream::consumer::DeliverPolicy::All),
        "last" => Ok(async_nats::jetstream::consumer::DeliverPolicy::Last),
        "new" => Ok(async_nats::jetstream::consumer::DeliverPolicy::New),
        "last_per_subject" => Ok(async_nats::jetstream::consumer::DeliverPolicy::LastPerSubject),
        policy => anyhow::bail!("Invalid deliver policy: {policy}"),
    }
}

fn str_to_ack_policy(
    policy_str: &str
) -> anyhow::Result<async_nats::jetstream::consumer::AckPolicy> {
    match policy_str {
        "explicit" => Ok(async_nats::jetstream::consumer::AckPolicy::Explicit),
        "none" => Ok(async_nats::jetstream::consumer::AckPolicy::None),
        "all" => Ok(async_nats::jetstream::consumer::AckPolicy::All),
        policy => anyhow::bail!("Invalid ack policy: {policy}"),
    }
}

fn str_to_replay_policy(
    policy_str: &str
) -> anyhow::Result<async_nats::jetstream::consumer::ReplayPolicy> {
    match policy_str {
        "instant" => Ok(async_nats::jetstream::consumer::ReplayPolicy::Instant),
        "original" => Ok(async_nats::jetstream::consumer::ReplayPolicy::Original),
        policy => anyhow::bail!("Invalid replay policy: {policy}"),
    }
}

fn str_to_priority_policy(
    policy_str: &str
) -> anyhow::Result<async_nats::jetstream::consumer::PriorityPolicy> {
    match policy_str {
        "overflow" => Ok(async_nats::jetstream::consumer::PriorityPolicy::Overflow),
        "pinned_client" => Ok(async_nats::jetstream::consumer::PriorityPolicy::PinnedClient),
        "prioritized" => Ok(async_nats::jetstream::consumer::PriorityPolicy::Prioritized),
        "none" => Ok(async_nats::jetstream::consumer::PriorityPolicy::None),
        policy => anyhow::bail!("Invalid priority policy: {policy}"),
    }
}
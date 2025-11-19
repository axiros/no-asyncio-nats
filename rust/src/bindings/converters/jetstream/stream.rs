use pyo3::prelude::*;
use std::time::Duration;

use crate::bindings::converters::offset_datetime::py_to_offset_datetime;

pub fn py_to_stream_config(
    py_config_dict: &Bound<'_, pyo3::types::PyDict>
) -> anyhow::Result<async_nats::jetstream::stream::Config> {
    
    let mut stream_config = async_nats::jetstream::stream::Config::default();

    stream_config.name = py_config_dict
        .get_item("name")?
        .map(|ob| ob.extract())
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("JetStream Source must have a name"))?;

    if let Some(value) = py_config_dict.get_item("no_ack")? {
        stream_config.no_ack = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_direct")? {
        stream_config.allow_direct = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("subjects")? {
        stream_config.subjects = value.extract::<Vec<String>>()?;
    }

    if let Some(value) = py_config_dict.get_item("retention")? {
        let retention: String = value.extract()?;
        stream_config.retention = match retention.as_str() {
            "limits" => async_nats::jetstream::stream::RetentionPolicy::Limits,
            "interest" => async_nats::jetstream::stream::RetentionPolicy::Interest,
            "workqueue" => async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            policy => anyhow::bail!("Invalid retention policy: {policy}")
        };
    }

    if let Some(value) = py_config_dict.get_item("storage")? {
        let storage: String = value.extract()?;
        stream_config.storage = match storage.as_str() {
            "file" => async_nats::jetstream::stream::StorageType::File,
            "memory" => async_nats::jetstream::stream::StorageType::Memory,
            typename => anyhow::bail!("Invalid storage type: {typename}")
        };
    }

    if let Some(value) = py_config_dict.get_item("discard")? {
        let discard: String = value.extract()?;
        stream_config.discard = match discard.as_str() {
            "new" => async_nats::jetstream::stream::DiscardPolicy::New,
            "old" => async_nats::jetstream::stream::DiscardPolicy::Old,
            policy => anyhow::bail!("Invalid discard policy: {policy}")
        };
    }

    if let Some(value) = py_config_dict.get_item("discard_new_per_subject")? {
        stream_config.discard_new_per_subject = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("compression")? {
        let compression: String = value.extract()?;
        stream_config.compression = match compression.as_str() {
            "none" => None,
            "s2" => Some(async_nats::jetstream::stream::Compression::S2),
            policy=> anyhow::bail!("Invalid compression policy {policy}")
        };
    }

    if let Some(value) = py_config_dict.get_item("num_replicas")? {
        stream_config.num_replicas = value.extract::<usize>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_bytes")? {
        stream_config.max_bytes = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_messages")? {
        stream_config.max_messages = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_messages_per_subject")? {
        stream_config.max_messages_per_subject = value.extract::<i64>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_age")? {
        stream_config.max_age = value.extract::<Duration>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_message_size")? {
        stream_config.max_message_size = value.extract::<i32>()?;
    }

    if let Some(value) = py_config_dict.get_item("max_consumers")? {
        stream_config.max_consumers = value.extract::<i32>()?;
    }

    if let Some(value) = py_config_dict.get_item("duplicate_window")? {
        stream_config.duplicate_window = value.extract::<Duration>()?;
    }

    if let Some(value) = py_config_dict.get_item("sealed")? {
        stream_config.sealed = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("mirror_direct")? {
        stream_config.mirror_direct = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_rollup")? {
        stream_config.allow_rollup = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("deny_delete")? {
        stream_config.deny_delete = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("deny_purge")? {
        stream_config.deny_purge = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_message_ttl")? {
        stream_config.allow_message_ttl = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_atomic_publish")? {
        stream_config.allow_atomic_publish = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_message_schedules")? {
        stream_config.allow_message_schedules = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("allow_message_counter")? {
        stream_config.allow_message_counter = value.is_truthy()?;
    }

    if let Some(value) = py_config_dict.get_item("description")? {
        stream_config.description = Some(value.extract::<String>()?);
    }

    if let Some(value) = py_config_dict.get_item("template_owner")? {
        stream_config.template_owner = value.extract::<String>()?;
    }

    if let Some(value) = py_config_dict.get_item("first_sequence")? {
        stream_config.first_sequence = Some(value.extract::<u64>()?);
    }

    if let Some(value) = py_config_dict.get_item("metadata")? {
        stream_config.metadata = value.extract::<std::collections::HashMap<String, String>>()?;
    }

    if let Some(value) = py_config_dict.get_item("republish")? {
        stream_config.republish = Some(py_to_republish(&value)?);
    }

    // Handle mirror field
    if let Some(value) = py_config_dict.get_item("mirror")? {
        stream_config.mirror = Some(py_to_source(&value)?);
    }

    // Handle sources field
    if let Some(value) = py_config_dict.get_item("sources")? {
        let sources_list = value.extract::<Vec<Bound<pyo3::types::PyDict>>>()?;
        stream_config.sources = Some(
            sources_list.iter()
                .map(|s| py_to_source(s))
                .collect::<Result<Vec<_>, _>>()?
        );
    }

    // Handle subject_transform field
    if let Some(value) = py_config_dict.get_item("subject_transform")? {
        stream_config.subject_transform = Some(py_to_subject_transform(&value)?);
    }

    // Handle consumer_limits field
    if let Some(value) = py_config_dict.get_item("consumer_limits")? {
        stream_config.consumer_limits = Some(py_to_consumer_limits(&value)?);
    }

    // Handle placement field
    if let Some(value) = py_config_dict.get_item("placement")? {
        stream_config.placement = Some(py_to_placement(&value)?);
    }

    // Handle persist_mode field
    if let Some(value) = py_config_dict.get_item("persist_mode")? {
        let mode_str: String = value.extract()?;
        stream_config.persist_mode = Some(str_to_persistence_mode(&mode_str)?);
    }

    // Handle pause_until field
    if let Some(value) = py_config_dict.get_item("pause_until")? {
        stream_config.pause_until = Some(py_to_offset_datetime(&value)?);
    }

    // Handle subject_delete_marker_ttl field
    if let Some(value) = py_config_dict.get_item("subject_delete_marker_ttl")? {
        stream_config.subject_delete_marker_ttl = Some(value.extract::<Duration>()?);
    }

    Ok(stream_config)
}


fn py_to_subject_transform(
    py_dict: &Bound<pyo3::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::SubjectTransform> {
    let source: String = py_dict.get_item("source")?.extract()?;
    let destination: String = py_dict.get_item("destination")?.extract()?;
    
    Ok(async_nats::jetstream::stream::SubjectTransform {
        source,
        destination,
    })
}

fn py_to_source(
    py_dict: &Bound<pyo3::types::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::Source> {

    let py_dict: &Bound<pyo3::types::PyDict> = py_dict
        .cast()
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    let Some(name) = py_dict.get_item("name")?.map(|ob| ob.extract()).transpose()? else {
        anyhow::bail!("JetStream Source must have a name");
    };

    let start_sequence: Option<u64> = py_dict
        .get_item("start_sequence")?
        .map(|ob| ob.extract())
        .transpose()?;

    let start_time: Option<time::OffsetDateTime> = py_dict
        .get_item("start_time")?
        .map(|dt| py_to_offset_datetime(&dt))
        .transpose()?;

    let filter_subject: Option<String> = py_dict
        .get_item("filter_subject")?
        .map(|ob| ob.extract())
        .transpose()?;

    let external: Option<async_nats::jetstream::stream::External> = py_dict
        .get_item("external")?
        .map(|ext| py_to_external(&ext))
        .transpose()?;

    let domain: Option<String> = py_dict
        .get_item("domain")?
        .map(|ob| ob.extract())
        .transpose()?;

    let subject_transforms: Vec<async_nats::jetstream::stream::SubjectTransform> = py_dict
        .get_item("subject_transforms")?
        .map(|transforms| {
            transforms.extract::<Vec<Bound<PyAny>>>()?
                .iter()
                .map(|t| py_to_subject_transform(t))
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))?;
    
    Ok(async_nats::jetstream::stream::Source {
        name,
        start_sequence,
        start_time,
        filter_subject,
        external,
        domain,
        subject_transforms,
    })
}

fn py_to_external(
    py_dict: &Bound<pyo3::types::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::External> {
    let api_prefix: String = py_dict.get_item("api_prefix")?.extract()?;
    let delivery_prefix: Option<String> = py_dict.get_item("delivery_prefix")?.extract()?;
    
    Ok(async_nats::jetstream::stream::External {
        api_prefix,
        delivery_prefix,
    })
}

fn py_to_consumer_limits(
    py_dict: &Bound<pyo3::types::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::ConsumerLimits> {
    let inactive_threshold: Duration = py_dict.get_item("inactive_threshold")?.extract()?;
    let max_ack_pending: i64 = py_dict.get_item("max_ack_pending")?.extract()?;
    
    Ok(async_nats::jetstream::stream::ConsumerLimits {
        inactive_threshold,
        max_ack_pending,
    })
}

fn py_to_republish(
    py_dict: &Bound<pyo3::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::Republish> {
    let source: String = py_dict.get_item("source")?.extract()?;
    let destination: String = py_dict.get_item("destination")?.extract()?;
    let headers_only: bool = py_dict.get_item("headers_only")?.is_truthy()?;
    
    Ok(async_nats::jetstream::stream::Republish {
        source,
        destination,
        headers_only,
    })
}

fn py_to_placement(
    py_dict: &Bound<pyo3::PyAny>
) -> anyhow::Result<async_nats::jetstream::stream::Placement> {
    let cluster: Option<String> = py_dict.get_item("cluster")?.extract()?;
    let tags: Vec<String> = py_dict.get_item("tags")?.extract()?;
    
    Ok(async_nats::jetstream::stream::Placement {
        cluster,
        tags,
    })
}

fn str_to_persistence_mode(
    mode_str: &str
) -> anyhow::Result<async_nats::jetstream::stream::PersistenceMode> {
    match mode_str {
        "default" => Ok(async_nats::jetstream::stream::PersistenceMode::Default),
        "async" => Ok(async_nats::jetstream::stream::PersistenceMode::Async),
        mode => anyhow::bail!("Invalid persistence mode: {mode}"),
    }
}


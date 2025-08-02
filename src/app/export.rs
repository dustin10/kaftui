use crate::kafka::Record;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;

/// Default prefix used for the name of the exported file when no partition key is set.
const DEFAULT_EXPORT_FILE_PREFIX: &str = "record-export";

/// View of a [`Record`] that is saved to a file in JSON format when the user requests that the
/// selected record be exported. This allows for better handling of the value field which would
/// just be rendered as a JSON encoded string otherwise.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportedRecord {
    /// Name of the topic that the record was consumed from.
    topic: String,
    /// Partition number the record was assigned in the topic.
    partition: i32,
    /// Offset of the record in the topic.
    offset: i64,
    /// Partition key for the record if one was set.
    key: Option<String>,
    /// Contains any headers from the Kafka record.
    headers: HashMap<String, String>,
    /// Value of the Kafka record, if one exists.
    value: Option<serde_json::Value>,
    /// UTC timestamp represeting when the event was created.
    timestamp: DateTime<Utc>,
}

impl From<&Record> for ExportedRecord {
    /// Converts a reference to a [`Record`] to an [`ExportedRecord`].
    fn from(record: &Record) -> Self {
        let json_value = record
            .value
            .as_ref()
            .and_then(|v| match serde_json::from_str(v) {
                Ok(json) => Some(json),
                Err(e) => {
                    tracing::error!("failed to serialize record value to JSON: {}", e);
                    None
                }
            });

        Self {
            topic: record.topic.clone(),
            partition: record.partition,
            offset: record.offset,
            key: record.key.clone(),
            headers: record.headers.clone(),
            value: json_value,
            timestamp: record.timestamp,
        }
    }
}

/// The [`Exporter`] is responsible for exporting a Kafka [`Record`] to the user's file system. It
/// does this by first serializing the [`Record`] to JSON and then saving the file in the
/// configured directory.
#[derive(Debug)]
pub struct Exporter {
    /// Directory on the file system where exported files will be saved.
    base_dir: String,
}

impl Exporter {
    /// Creates a new [`Exporter`] with the specified dependencies.
    pub fn new(base_dir: String) -> Self {
        Self { base_dir }
    }
    /// Exports the given [`Record`] to the file system in JSON format.
    pub fn export_record(&self, record: &Record) -> anyhow::Result<String> {
        let exported_record = ExportedRecord::from(record);

        let json =
            serde_json::to_string_pretty(&exported_record).context("serialize exported record")?;

        let name = exported_record
            .key
            .as_ref()
            .map_or(DEFAULT_EXPORT_FILE_PREFIX, |v| v);

        let file_path = format!(
            "{}{}{}-{}-{}.json",
            self.base_dir,
            std::path::MAIN_SEPARATOR,
            exported_record.topic,
            name,
            Utc::now().timestamp_millis()
        );

        let _ = std::fs::write(file_path.as_str(), json).context("write exported record to file");

        Ok(file_path)
    }
}

use crate::kafka::{
    schema::{Schema, SchemaRef, Version},
    Format, Record,
};

use anyhow::Context;
use chrono::{DateTime, Local};
use serde::Serialize;
use std::collections::HashMap;

/// Default prefix used for the name of the exported file when no partition key is set or it is in
/// a format that should not be used in the file name.
const DEFAULT_EXPORT_FILE_PREFIX: &str = "record";

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
    /// Local timestamp represeting when the event was created.
    timestamp: DateTime<Local>,
}

impl ExportedRecord {
    /// Converts a reference to a [`Record`] to an [`ExportedRecord`].
    fn from_record(record: Record, value_format: Format) -> Self {
        let json_value = record.value.as_ref().map(|v| match value_format {
            Format::None => serde_json::Value::String(v.clone()),
            Format::Json | Format::Avro | Format::Protobuf => match serde_json::from_str(v) {
                Ok(json) => json,
                Err(e) => {
                    tracing::error!("failed to serialize record value to JSON: {}", e);
                    serde_json::Value::String(v.clone())
                }
            },
        });

        Self {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            key: record.key,
            headers: record.headers,
            value: json_value,
            timestamp: record.timestamp,
        }
    }
}

/// View of a [`Schema`] that is saved to a file in JSON format when the user requests that the
/// selected schema be exported. This allows for better handling of the schema definition field
/// which would just be rendered as a JSON encoded string otherwise.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportedSchema {
    /// Identifier of the schema.
    id: i32,
    /// Globally unique identifier of the schema.
    guid: String,
    /// Version of the schema.
    version: Version,
    /// The schema type, i.e. AVRO, JSON, PROTOBUF.
    #[serde(rename = "type")]
    kind: String,
    /// The schema definition.
    schema: serde_json::Value,
    /// References to other schemas contained in this schema.
    references: Option<Vec<SchemaRef>>,
}

impl From<Schema> for ExportedSchema {
    /// Converts from a [`Schema`] to an [`ExportedSchema`]. If the schema defintion is not valid
    /// JSON it will be stored as a plain string instead.
    fn from(schema: Schema) -> Self {
        Self {
            id: schema.id,
            guid: schema.guid,
            version: schema.version,
            kind: schema.kind,
            schema: match serde_json::from_str(&schema.schema) {
                Ok(json) => json,
                Err(e) => {
                    tracing::error!("failed to serialize schema to JSON: {}", e);
                    serde_json::Value::String(schema.schema)
                }
            },
            references: schema.references,
        }
    }
}

/// The [`Exporter`] is responsible for exporting a Kafka [`Record`]s and [`Schema`]s to the user's
/// file system. It does this by first serializing the values to JSON and then saving them to a
/// file in the configured directory.
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
    pub fn export_record(
        &self,
        record: Record,
        key_format: Format,
        value_format: Format,
    ) -> anyhow::Result<String> {
        let exported_record = ExportedRecord::from_record(record, value_format);

        let json = serde_json::to_string_pretty(&exported_record)
            .context("serialize exported record to JSON")?;

        let name = if let Some(key) = exported_record.key.as_ref() {
            match key_format {
                Format::None => key,
                Format::Json | Format::Avro | Format::Protobuf => DEFAULT_EXPORT_FILE_PREFIX,
            }
        } else {
            DEFAULT_EXPORT_FILE_PREFIX
        };

        let file_path = format!(
            "{}{}{}-{}-{}.json",
            self.base_dir,
            std::path::MAIN_SEPARATOR,
            exported_record.topic,
            name,
            Local::now().timestamp_millis()
        );

        let _ = std::fs::write(file_path.as_str(), json).context("write exported record to file");

        Ok(file_path)
    }
    /// Exports the given [`Schema`] to the file system in JSON format.
    pub fn export_schema(&self, schema: Schema) -> anyhow::Result<String> {
        let exported_schema = ExportedSchema::from(schema);

        let json = serde_json::to_string_pretty(&exported_schema)
            .context("serialize exported schema to JSON")?;

        let file_path = format!(
            "{}{}schema-{}.json",
            self.base_dir,
            std::path::MAIN_SEPARATOR,
            exported_schema.id,
        );

        let _ = std::fs::write(file_path.as_str(), json).context("write exported schema to file");

        Ok(file_path)
    }
}

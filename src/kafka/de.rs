use anyhow::Context;
use async_trait::async_trait;
use protofish::{
    context::MessageInfo,
    decode::{MessageValue, PackedArray, UnknownValue, Value},
    prelude::Context as ProtoContext,
};
use rdkafka::message::{BorrowedHeaders, Headers};
use schema_registry_client::{
    rest::schema_registry_client::Client,
    serdes::{
        avro::AvroDeserializer,
        config::DeserializerConfig,
        json::JsonDeserializer,
        serde::{
            SerdeError, SerdeFormat, SerdeHeader, SerdeHeaders, SerdeType, SerializationContext,
        },
    },
};
use std::collections::HashMap;

/// The file extension for Protobuf schema files.
const PROTO_FILE_EXTENSION: &str = "proto";

/// A trait which defines the behavior required to deserialize the key of a Kafka message to a
/// String for display to the end user.
#[async_trait]
pub trait KeyDeserializer: Send + Sync {
    /// Transforms the bytes into a String representation of the key.
    async fn deserialize_key(&self, data: &[u8]) -> anyhow::Result<String>;
}

/// A trait which defines the behavior required to deserialize the value of a Kafka message to a
/// String for display to the end user.
#[async_trait]
pub trait ValueDeserializer: Send + Sync {
    /// Transforms the bytes into a String representation of the value.
    async fn deserialize_value(
        &self,
        topic: &str,
        headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String>;
}

/// Deserializer implementation that converts the Kafka message value directly to a UTF-8 string.
pub struct StringDeserializer;

#[async_trait]
impl KeyDeserializer for StringDeserializer {
    /// Transforms the array of bytes into a UTF-8 string, replacing any invalid sequences with
    /// the Unicode replacement character.
    async fn deserialize_key(&self, data: &[u8]) -> anyhow::Result<String> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}

#[async_trait]
impl ValueDeserializer for StringDeserializer {
    /// Transforms the array of bytes into a UTF-8 string, replacing any invalid sequences with
    /// the Unicode replacement character.
    async fn deserialize_value(
        &self,
        _topic: &str,
        _headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}

/// Implementation of the [`ValueDeserializer`] trait the parses the Kafka message value to JSON
/// and then pretty-prints it.
pub struct JsonValueDeserializer;

#[async_trait]
impl ValueDeserializer for JsonValueDeserializer {
    /// Transforms the array of bytes into a pretty-printed JSON string.
    async fn deserialize_value(
        &self,
        _topic: &str,
        _headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String> {
        let s = std::str::from_utf8(data).context("invalid UTF8 string data")?;

        let json: serde_json::Value = serde_json::from_str(s).context("create JSON value")?;

        serde_json::to_string_pretty(&json).context("prettify JSON string")
    }
}

/// Deserializer implementation that converts that uses the Confluent Schema Registry to safely
/// deserialize data using the JSON schema format.
pub struct JsonSchemaDeserializer<'a, C>
where
    C: Client + Sync,
{
    json: JsonDeserializer<'a, C>,
}

impl<'a, C> JsonSchemaDeserializer<'a, C>
where
    C: Client + Sync,
{
    /// Creates a new [`JsonSchemaDeserializer`] with the given schema registry [`Client`].
    pub fn new(client: &'a C) -> Result<Self, SerdeError> {
        let de_config = DeserializerConfig::new(None, true, HashMap::new());

        let json = JsonDeserializer::new(client, None, de_config)?;

        Ok(Self { json })
    }
}

#[async_trait]
impl<'a, C> ValueDeserializer for JsonSchemaDeserializer<'a, C>
where
    C: Client + Sync,
{
    /// Transforms the array of bytes into a string using the JSON schema deserializer.
    async fn deserialize_value(
        &self,
        topic: &str,
        headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String> {
        let ctx = SerializationContext {
            topic: topic.to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: headers.map(to_serde_headers),
        };

        match self.json.deserialize(&ctx, data).await {
            Ok(value) => serde_json::to_string_pretty(&value).context("prettify JSON string"),
            Err(e) => anyhow::bail!("unable to deserialize JSON value: {}", e),
        }
    }
}

/// Deserializer implementation that converts that uses the Confluent Schema Registry to safely
/// deserialize data using the Avro schema format.
pub struct AvroSchemaDeserializer<'a, C>
where
    C: Client,
{
    avro: AvroDeserializer<'a, C>,
}

impl<'a, C> AvroSchemaDeserializer<'a, C>
where
    C: Client + Sync,
{
    /// Creates a new [`AvroSchemaDeserializer`] with the given schema registry [`Client`].
    pub fn new(client: &'a C) -> Result<Self, SerdeError> {
        let de_config = DeserializerConfig::new(None, true, HashMap::new());

        let avro = AvroDeserializer::new(client, None, de_config)?;

        Ok(Self { avro })
    }
}

#[async_trait]
impl<'a, C> ValueDeserializer for AvroSchemaDeserializer<'a, C>
where
    C: Client + Sync,
{
    /// Transforms the array of bytes into a string using the Avro schema deserializer.
    async fn deserialize_value(
        &self,
        topic: &str,
        headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String> {
        let ctx = SerializationContext {
            topic: topic.to_string(),
            serde_type: SerdeType::Key,
            serde_format: SerdeFormat::Avro,
            headers: headers.map(to_serde_headers),
        };

        match self.avro.deserialize(&ctx, data).await {
            Ok(named_value) => {
                let value: serde_json::Value = named_value
                    .value
                    .try_into()
                    .context("convert avro value to serde_json value")?;

                serde_json::to_string_pretty(&value).context("prettify JSON string")
            }
            Err(e) => anyhow::bail!("unable to deserialize Avro value: {}", e),
        }
    }
}

/// Deserializer implementation that converts that uses the Confluent Schema Registry to safely
/// deserialize data using the Protobuf schema format.
pub struct ProtobufSchemaDeserializer {
    context: ProtoContext,
    message_type: String,
}

impl ProtobufSchemaDeserializer {
    /// Creates a new [`ProtoSchemaDeserializer`].
    pub fn new(
        protos_dir: impl AsRef<str>,
        message_type: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let context = read_files_recursive(protos_dir, PROTO_FILE_EXTENSION)
            .context("find proto files")
            .and_then(|protos| ProtoContext::parse(protos).context("parse protobuf files"))?;

        Ok(Self {
            context,
            message_type: message_type.into(),
        })
    }
    /// Recursively converts a Protobuf message value to a JSON string representation.
    fn message_to_json(&self, msg_info: &MessageInfo, msg_value: &MessageValue) -> String {
        let mut field_strs: Vec<String> = Vec::new();

        for field_value in msg_value.fields.iter() {
            let msg_field = match msg_info.get_field(field_value.number) {
                Some(f) => f,
                None => {
                    tracing::warn!(
                        "unable to find field info for field number {}",
                        field_value.number
                    );
                    continue;
                }
            };

            let field_str = match field_value.value {
                Value::Bool(b) => match b {
                    true => String::from("true"),
                    false => String::from("false"),
                },
                Value::Bytes(ref bytes) => format!("\"<{} raw bytes omitted>\"", bytes.len()),
                Value::Double(d) => d.to_string(),
                Value::Enum(ref enum_value) => {
                    let enum_info = self.context.resolve_enum(enum_value.enum_ref);

                    match enum_info.get_field_by_value(enum_value.value) {
                        Some(field) => format!("\"{}\"", field.name),
                        None => {
                            tracing::warn!(
                                "unable to find enum field for value {}",
                                enum_value.value
                            );
                            format!("\"<unknown enum value - {}>\"", enum_value.value)
                        }
                    }
                }
                Value::Fixed32(i) => i.to_string(),
                Value::Fixed64(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Incomplete(u, ref bytes) => format!(
                    "\"<incomplete value {} - {} bytes consumed>\"",
                    u,
                    bytes.len()
                ),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Message(ref child_value) => {
                    let child_info = self.context.resolve_message(child_value.msg_ref);

                    self.message_to_json(child_info, child_value)
                }
                Value::Packed(ref packed_array) => match packed_array {
                    PackedArray::Bool(bs) => to_json_array_string(bs),
                    PackedArray::Double(ds) => to_json_array_string(ds),
                    PackedArray::Fixed32(fs) => to_json_array_string(fs),
                    PackedArray::Fixed64(fs) => to_json_array_string(fs),
                    PackedArray::Float(fs) => to_json_array_string(fs),
                    PackedArray::Int32(is) => to_json_array_string(is),
                    PackedArray::Int64(is) => to_json_array_string(is),
                    PackedArray::SFixed32(is) => to_json_array_string(is),
                    PackedArray::SFixed64(is) => to_json_array_string(is),
                    PackedArray::SInt32(is) => to_json_array_string(is),
                    PackedArray::SInt64(is) => to_json_array_string(is),
                    PackedArray::UInt32(us) => to_json_array_string(us),
                    PackedArray::UInt64(us) => to_json_array_string(us),
                },
                Value::SFixed32(i) => i.to_string(),
                Value::SFixed64(i) => i.to_string(),
                Value::SInt32(i) => i.to_string(),
                Value::SInt64(i) => i.to_string(),
                Value::String(ref s) => format!("\"{}\"", s),
                Value::UInt32(i) => i.to_string(),
                Value::UInt64(i) => i.to_string(),
                Value::Unknown(ref unk_value) => match unk_value {
                    UnknownValue::Fixed32(u) => format!("\"<unknown 32-bit value: {}>\"", u),
                    UnknownValue::Fixed64(u) => format!("\"<unknown 64-bit value: {}>\"", u),
                    UnknownValue::Invalid(u, bytes) => format!(
                        "\"<invalid wire type: {} - {} bytes consumed>\"",
                        u,
                        bytes.len()
                    ),
                    UnknownValue::VariableLength(bytes) => format!(
                        "\"<unknown variable length value - {} bytes consumed>\"",
                        bytes.len()
                    ),
                    UnknownValue::Varint(u) => format!("\"<unknown variable int value: {}>\"", u),
                },
            };

            field_strs.push(format!("\"{}\":{}", msg_field.name, field_str));
        }

        format!("{{{}}}", field_strs.join(","))
    }
}

#[async_trait]
impl ValueDeserializer for ProtobufSchemaDeserializer {
    /// Transforms the array of bytes into a string using the Protobuf schema deserializer.
    async fn deserialize_value(
        &self,
        _topic: &str,
        _headers: Option<&BorrowedHeaders>,
        data: &[u8],
    ) -> anyhow::Result<String> {
        // record data starts at byte 5 when produced with the schema registry enabled serializer,
        // we are not technically validating the schema in this deserialzier so we skip those bytes
        // and use the remaining ones to decode the message.
        //
        // TODO: also assumes a single 0 byte at position 5 for message indexes which can be a
        // common case in protobuf serialiazation but maybe not always? works when testing against
        // the confluent schema registry protobuf serializer.
        let data = &data[6..];

        let msg_info = match self.context.get_message(&self.message_type) {
            Some(msg_info) => msg_info,
            None => {
                anyhow::bail!(
                    "failed to load protobuf message info for type {}",
                    self.message_type
                );
            }
        };

        let msg_value = self.context.decode(msg_info.self_ref, data);

        let json = self.message_to_json(msg_info, &msg_value);

        serde_json::from_str(json.as_str())
            .context("create JSON value")
            .and_then(|v: serde_json::Value| {
                serde_json::to_string_pretty(&v).context("prettify JSON string")
            })
    }
}

/// Creates a new [`SerdeHeaders`] from the given [`BorrowedHeaders`] which can be used in the
/// schema registry bsed deserialization context.
fn to_serde_headers(headers: &BorrowedHeaders) -> SerdeHeaders {
    let ser_headers = SerdeHeaders::default();
    for header in headers.iter() {
        ser_headers.insert(SerdeHeader {
            key: header.key.to_string(),
            value: header.value.map(|v| v.to_vec()),
        });
    }

    ser_headers
}

/// Recursively finds all files with the given extension in the specified directory and its
/// subdirectories, returning their contents as a vector of strings.
fn read_files_recursive(dir: impl AsRef<str>, target_ext: &str) -> anyhow::Result<Vec<String>> {
    let entries = std::fs::read_dir(dir.as_ref()).context("read configured protobuf directory")?;

    let mut contents: Vec<String> = Vec::new();

    for entry in entries {
        match entry {
            Ok(e) => {
                let path = e.path();

                if path.is_dir() {
                    let child_dir = match path.into_os_string().into_string() {
                        Ok(child_dir) => child_dir,
                        Err(_) => anyhow::bail!("unable to convert proto dir path to string"),
                    };

                    let child_contents = read_files_recursive(child_dir, target_ext)
                        .context("recursive find files")?;

                    contents.extend(child_contents);
                } else if let Some(file_ext) = path.extension()
                    && file_ext.to_str() == Some(target_ext)
                {
                    match std::fs::read_to_string(path) {
                        Ok(content) => contents.push(content),
                        Err(e) => anyhow::bail!("unable to read proto file: {}", e),
                    }
                }
            }
            Err(e) => anyhow::bail!("unable to read proto file entry: {}", e),
        }
    }

    Ok(contents)
}

/// Converts a slice of values that implement [`ToString`] into a JSON representation of an array.
fn to_json_array_string<T: ToString>(values: &[T]) -> String {
    let strs: Vec<String> = values.iter().map(ToString::to_string).collect();

    format!("[{}]", strs.join(","))
}

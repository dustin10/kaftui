use anyhow::Context;
use async_trait::async_trait;
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

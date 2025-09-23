use anyhow::Context;

/// A trait which defines the behavior required to deserialize the value of a Kafka message to a
/// String for display to the end user.
pub trait ValueDeserializer: Send + Sync {
    fn deserialize(&self, data: &[u8]) -> anyhow::Result<String>;
}

/// Implementation of the [`ValueDeserializer`] trait the converts the Kafka message value directly
/// to a UTF-8 [`String`].
pub struct StringValueDeserializer;

impl ValueDeserializer for StringValueDeserializer {
    /// Transforms the array of bytes into a UTF-8 string, replacing any invalid sequences with
    /// the Unicode replacement character.
    fn deserialize(&self, data: &[u8]) -> anyhow::Result<String> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}

/// Implementation of the [`ValueDeserializer`] trait the parses the Kafka message value to JSON
/// and then  pretty-prints it.
pub struct JsonValueDeserializer;

impl ValueDeserializer for JsonValueDeserializer {
    /// Transforms the array of bytes itno a pretty-printed JSON string.
    fn deserialize(&self, data: &[u8]) -> anyhow::Result<String> {
        let s = std::str::from_utf8(data).context("invalid UTF8 data")?;

        let json: serde_json::Value = serde_json::from_str(s).context("create JSON value")?;

        serde_json::to_string_pretty(&json).context("prettify JSON string")
    }
}

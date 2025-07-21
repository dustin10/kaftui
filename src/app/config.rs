use anyhow::Context;
use chrono::Utc;
use config::{Config as ConfigRs, ConfigError, Environment, Map, Source, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Prefix for the default group id for the Kafka consumer generated from the hostname of the
/// machine the application is running on.
pub const DEFAULT_CONSUMER_GROUP_ID_PREFIX: &str = "kaftui-";

/// Default maximum number of records consumed from the Kafka toic to hold in memory at any given
/// time.
pub const DEFAULT_MAX_RECORDS: usize = 256;

/// Configuration values which drive the behavior of the application.
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    /// Kafka bootstrap servers host value that the application will connect to.
    pub bootstrap_servers: String,
    /// Name of the Kafka topic to consume messages from.
    pub topic: String,
    /// Id of the consumer group that the application will use when consuming messages from the Kafka topic.
    pub group_id: String,
    /// Additional configuration properties that will be applied to the Kafka consumer.
    pub consumer_properties: Option<HashMap<String, String>>,
    /// JSONPath filter that is applied to a [`Record`]. Can be used to filter out any messages
    /// from the Kafka topic that the end user may not be interested in. A message will only be
    /// presented to the user if it matches the filter.
    pub filter: Option<String>,
    /// Maximum nunber of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    pub max_records: usize,
}

impl Config {
    /// Initializes the configuration for the application. Uses values from the any specified profile
    /// as defaults and then overlays arguments on top.
    ///
    /// Configuration precedence is applied as follows where 1 is the highest:
    ///
    /// 1. CLI arguments
    /// 2. Profile values, if one is specified
    /// 3. Environment variables
    /// 4. Default values
    pub fn new<P, S>(profile_name: Option<P>, args: S) -> anyhow::Result<Self>
    where
        P: AsRef<str>,
        S: Source + Send + Sync + 'static,
    {
        let mut profile: Option<Profile> = None;

        // check for a specified profile
        if let Some(name) = profile_name {
            let file_path = std::env::home_dir()
                .context("resolve home directory")?
                .join(".kaftui.json");

            let config = match std::fs::read_to_string(file_path) {
                Ok(s) => serde_json::from_str(&s).context("deserialize persisted config")?,
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => PersistedConfig::default(),
                Err(e) => return Err(e).context("read config file"),
            };

            profile = config.profiles.and_then(|ps| {
                ps.iter()
                    .find(|p| p.name.eq(name.as_ref()))
                    .into_iter()
                    .next()
                    .cloned()
            });
        }

        let config = ConfigRs::builder()
            .add_source(Defaults)
            .add_source(Environment::with_prefix("KAFTUI").separator("_"))
            .add_source(profile.unwrap_or_default())
            .add_source(args)
            .build()
            .context("create Config from sources")?;

        config.try_deserialize().context("deserialize Config")
    }
}

/// Empty struct that simply implements the [`Source`] trait to integrate the global appliction
/// default values into the configuration resolution.
#[derive(Debug)]
pub struct Defaults;

impl Source for Defaults {
    /// Clones the [`Source`] and lifts it into a [`Box`].
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(Defaults)
    }
    /// Collect all configuration properties available from this source into a [`Map`].
    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut cfg = config::Map::new();

        cfg.insert(String::from("group_id"), Value::from(generate_group_id()));

        cfg.insert(
            String::from("max_records"),
            Value::from(DEFAULT_MAX_RECORDS as i32),
        );

        Ok(cfg)
    }
}

/// Generates a consumer group id for the Kafka consumer based on the hostname of the maachine
/// running the application. If no hostname is able to be resolved then the current UTC epoch
/// timestamp milliseconds value will be used in it's place.
fn generate_group_id() -> String {
    match gethostname::gethostname().into_string() {
        Ok(name) => format!("{}-{}", DEFAULT_CONSUMER_GROUP_ID_PREFIX, name),
        Err(_) => {
            tracing::error!("failed to get hostname");
            format!(
                "{}-{}",
                DEFAULT_CONSUMER_GROUP_ID_PREFIX,
                Utc::now().timestamp_millis()
            )
        }
    }
}

/// Configuration that resides in the .kaftui.json file persisted on the user's machine.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedConfig {
    /// Contains any pre-configured [`Profile`]s that the user may have previously configured.
    profiles: Option<Vec<Profile>>,
}

/// A [`Profile`] a persisted set of configuration values that act as the default values for
/// execution of the application.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    /// Name that uniquely identifies a profile.
    name: String,
    /// Kafka bootstrap servers host value that the application will connect to.
    bootstrap_servers: Option<String>,
    /// Name of the Kafka topic to consume messages from.
    topic: Option<String>,
    /// Id of the consumer group that the application will use when consuming messages from the Kafka topic.
    group_id: Option<String>,
    /// JSONPath filter that is applied to a [`Record`]. Can be used to filter out any messages
    /// from the Kafka topic that the end user may not be interested in. A message will only be
    /// presented to the user if it matches the filter.
    filter: Option<String>,
    /// Maximum nunber of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    max_records: Option<usize>,
    /// Additional configuration properties that should be applied to the Kafka consumer.
    consumer_properties: Option<HashMap<String, String>>,
}

impl Source for Profile {
    /// Clones the [`Source`] and lifts it into a [`Box`].
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }
    /// Collect all configuration properties available from this source into a [`Map`].
    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut cfg = config::Map::new();

        if let Some(servers) = self.bootstrap_servers.as_ref() {
            cfg.insert(
                String::from("bootstrap_servers"),
                Value::from(servers.clone()),
            );
        }

        if let Some(topic) = self.topic.as_ref() {
            cfg.insert(String::from("topic"), config::Value::from(topic.clone()));
        }

        if let Some(group_id) = self.group_id.as_ref() {
            cfg.insert(String::from("group_id"), Value::from(group_id.clone()));
        }

        if let Some(filter) = self.filter.as_ref() {
            cfg.insert(String::from("filter"), config::Value::from(filter.clone()));
        }

        if let Some(max_records) = self.max_records.as_ref() {
            cfg.insert(
                String::from("max_records"),
                Value::from(*max_records as i32),
            );
        }

        if let Some(consumer_properties) = self.consumer_properties.as_ref() {
            cfg.insert(
                String::from("consumer_properties"),
                Value::from(consumer_properties.clone()),
            );
        }

        Ok(cfg)
    }
}

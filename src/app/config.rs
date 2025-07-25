use anyhow::Context;
use chrono::Utc;
use config::{Config as ConfigRs, ConfigError, Environment, Map, Source, Value, ValueKind};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::ErrorKind};

/// Prefix for the default group id for the Kafka consumer generated from the hostname of the
/// machine the application is running on.
pub const DEFAULT_CONSUMER_GROUP_ID_PREFIX: &str = "kaftui-";

/// Default maximum number of records consumed from the Kafka toic to hold in memory at any given
/// time.
pub const DEFAULT_MAX_RECORDS: usize = 256;

/// Default value for the scoll factor of the record value text panel.
const DEFAULT_SCROLL_FACTOR: u16 = 3;

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
    /// Controls how many lines each press of a key scrolls the record value text.
    pub scroll_factor: u16,
    /// Color configuration for the UI components of the application.
    pub theme: Theme,
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
    pub fn new<P, S>(profile_name: Option<P>, cli_args: S) -> anyhow::Result<Self>
    where
        P: AsRef<str>,
        S: Source + Send + Sync + 'static,
    {
        let mut profile: Option<Profile> = None;

        let file_path = std::env::home_dir()
            .context("resolve home directory")?
            .join(".kaftui.json");

        let persisted_config = match std::fs::read_to_string(file_path) {
            Ok(s) => serde_json::from_str(&s).context("deserialize persisted config")?,
            Err(e) if e.kind() != ErrorKind::NotFound => PersistedConfig::default(),
            Err(e) => return Err(e).context("read config file"),
        };

        // check for a specified profile
        if let Some(name) = profile_name {
            profile = persisted_config.profiles.as_ref().and_then(|ps| {
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
            .add_source(persisted_config)
            .add_source(profile.unwrap_or_default())
            .add_source(cli_args)
            .build()
            .context("create Config from sources")?;

        config.try_deserialize().context("deserialize Config")
    }
}

/// Empty struct that simply implements the [`Source`] trait to integrate the global application
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
        let mut cfg = Map::new();

        cfg.insert(String::from("group_id"), Value::from(generate_group_id()));

        cfg.insert(
            String::from("max_records"),
            Value::from(DEFAULT_MAX_RECORDS as i32),
        );

        cfg.insert(
            String::from("scroll_factor"),
            Value::from(DEFAULT_SCROLL_FACTOR),
        );

        cfg.insert(String::from("theme"), Value::from(Theme::default()));

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
    /// Maximum nunber of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    max_records: Option<usize>,
    /// Controls how many lines each press of a key scrolls the record value text.
    scroll_factor: Option<u16>,
    /// Color configuration for the UI components of the application.
    theme: Option<Theme>,
}

impl Source for PersistedConfig {
    /// Clones the [`Source`] and lifts it into a [`Box`].
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }
    /// Collect all configuration properties available from this source into a [`Map`].
    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let mut cfg = Map::new();

        if let Some(max_records) = self.max_records.as_ref() {
            cfg.insert(
                String::from("max_records"),
                Value::from(*max_records as i32),
            );
        }

        if let Some(scroll_factor) = self.scroll_factor {
            cfg.insert(String::from("scroll_factor"), Value::from(scroll_factor));
        }

        if let Some(theme) = self.theme.as_ref() {
            cfg.insert(String::from("theme"), Value::from(theme.clone()));
        }

        Ok(cfg)
    }
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
        let mut cfg = Map::new();

        if let Some(servers) = self.bootstrap_servers.as_ref() {
            cfg.insert(
                String::from("bootstrap_servers"),
                Value::from(servers.clone()),
            );
        }

        if let Some(topic) = self.topic.as_ref() {
            cfg.insert(String::from("topic"), Value::from(topic.clone()));
        }

        if let Some(group_id) = self.group_id.as_ref() {
            cfg.insert(String::from("group_id"), Value::from(group_id.clone()));
        }

        if let Some(filter) = self.filter.as_ref() {
            cfg.insert(String::from("filter"), Value::from(filter.clone()));
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

/// Contains the configuration values for the colors of the UI components that make up the
/// application. Color values should be 32 bits and the integer value for the hexadecimal
/// represenation for the RGB values as follows: 0x00RRGGBB.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Theme {
    /// Color used for the borders of the main info panels. Defaults to white.
    pub panel_border_color: String,
    /// Color used for the borders of the selected info panel. Defaults to cyan.
    pub selected_panel_border_color: String,
    /// Color used for the status text while the Kafka consumer is active. Defaults to green.
    pub status_text_color_processing: String,
    /// Color used for the status text while the Kafka consumer is paused. Defaults to red.
    pub status_text_color_paused: String,
    /// Color used for the key bindings text. Defaults to white.
    pub key_bindings_text_color: String,
    /// Color used for the label text in tables, etc. Defaults to white.
    pub label_color: String,
}

impl Default for Theme {
    /// Creates a new instance of [`Theme`] which contains the default color values for the
    /// components of the application.
    ///
    /// # Defaults
    ///
    /// The following colors are used for the defaults.
    ///
    /// * Panel Border - White
    /// * Selected Panel Border - Cyan
    /// * Processing Status Text - Green
    /// * Paused Status Text - Red
    /// * Key Bindings Text - White
    fn default() -> Self {
        Self {
            panel_border_color: String::from("FFFFFF"),
            selected_panel_border_color: String::from("00FFFF"),
            status_text_color_processing: String::from("00FF00"),
            status_text_color_paused: String::from("FF0000"),
            key_bindings_text_color: String::from("FFFFFF"),
            label_color: String::from("FFFFFF"),
        }
    }
}

impl From<Theme> for ValueKind {
    /// Consumes and converts a [`Theme`] to a [`ValueKind`] so that it can be used as a
    /// [`Source`].
    fn from(value: Theme) -> Self {
        let mut data = HashMap::new();

        data.insert(
            String::from("panelBorderColor"),
            Value::from(value.panel_border_color),
        );

        data.insert(
            String::from("selectedPanelBorderColor"),
            Value::from(value.selected_panel_border_color),
        );

        data.insert(
            String::from("statusTextColorPaused"),
            Value::from(value.status_text_color_paused),
        );

        data.insert(
            String::from("statusTextColorProcessing"),
            Value::from(value.status_text_color_processing),
        );

        data.insert(
            String::from("keyBindingsTextColor"),
            Value::from(value.key_bindings_text_color),
        );

        data.insert(String::from("labelColor"), Value::from(value.label_color));

        Self::Table(data)
    }
}

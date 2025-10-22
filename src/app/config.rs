use crate::kafka::{RecordFormat, SeekTo};

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

/// Default value for the scroll factor of the record value text panel.
const DEFAULT_SCROLL_FACTOR: u16 = 3;

/// Default value for the file export directory.
const DEFAULT_EXPORT_DIRECTORY: &str = ".";

/// Default maximum number of logs that should be stored in memory.
const DEFAULT_LOGS_MAX_HISTORY: u16 = 2048;

impl From<SeekTo> for ValueKind {
    /// Converts from an owned [`SeekTo`] to a [`ValueKind`].
    fn from(value: SeekTo) -> Self {
        Self::String(value.to_string())
    }
}

impl From<RecordFormat> for ValueKind {
    /// Converts from an owned [`RecordFormat`] to a [`ValueKind`].
    fn from(value: RecordFormat) -> Self {
        Self::String(value.to_string())
    }
}

/// Configuration values which drive the behavior of the application.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Kafka bootstrap servers host value that the application will connect to.
    pub bootstrap_servers: String,
    /// Name of the Kafka topic to consume messages from.
    pub topic: String,
    /// CSV of partitions numbers that the consumer should be assigned. If none, all of the
    /// partitions which make up the topic will be assigned.
    pub partitions: Option<String>,
    /// Variant of the [`RecordFormat`] enum which specifies the format of the data in the Kafka
    /// topic. Defaults to [`RecordFormat::None`].
    pub format: RecordFormat,
    /// Specifies the URL of the Schema Registry that should be used to validate data when
    /// deserializing records from the Kafka topic.
    pub schema_registry_url: Option<String>,
    /// Specifies bearer authentication token used to connect to the the Schema Registry.
    pub schema_registry_bearer_token: Option<String>,
    /// Specifies the basic auth user used to connect to the the Schema Registry.
    pub schema_registry_user: Option<String>,
    /// Specifies the basic auth password used to connect to the the Schema Registry.
    pub schema_registry_pass: Option<String>,
    /// Specifies the directory where the `.proto` files are located.
    pub protobuf_dir: Option<String>,
    /// Specifies the Protobuf message type which maps to the records in the Kafka topic.
    pub protobuf_type: Option<String>,
    /// Id of the consumer group that the application will use when consuming messages from the
    /// Kafka topic.
    pub group_id: String,
    /// Variant of the [`SeekTo`] enum that drives the partitions offsets the Kafka consumer seeks
    /// to before starting to consume records. Defaults to [`SeekTo::None`].
    pub seek_to: SeekTo,
    /// Additional configuration properties that will be applied to the Kafka consumer.
    pub consumer_properties: Option<HashMap<String, String>>,
    /// JSONPath filter that is applied to a [`Record`]. Can be used to filter out any messages
    /// from the Kafka topic that the end user may not be interested in. A message will only be
    /// presented to the user if it matches the filter.
    pub filter: Option<String>,
    /// Maximum number of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    pub max_records: usize,
    /// Controls how many lines each press of a key scrolls the record value text.
    pub scroll_factor: u16,
    /// Color configuration for the UI components of the application.
    pub theme: Theme,
    /// Directory on the file system where exported files will be saved.
    pub export_directory: String,
    /// If true, indicates that logs have been enabled by the user.
    pub logs_enabled: bool,
    /// Maximum number of logs that should be held in memory at any given time when logging is
    /// enabled.
    pub logs_max_history: u16,
}

impl Config {
    /// Initializes the configuration for the application. Uses values from the any specified
    /// profile as defaults and then overlays arguments on top.
    ///
    /// Configuration precedence is applied as follows where 1 is the highest:
    ///
    /// 1. Environment variables
    /// 2. CLI arguments
    /// 3. Profile values, if one is specified
    /// 4. Applicable configuration values from $HOME/.kaftui.json file
    /// 5. Default values
    pub fn new<P, S>(cli_args: S, profile_name: Option<P>) -> anyhow::Result<Self>
    where
        P: AsRef<str>,
        S: Source + Send + Sync + 'static,
    {
        let file_path = std::env::home_dir()
            .context("resolve home directory")?
            .join(".kaftui.json");

        let persisted_config = match std::fs::read_to_string(file_path) {
            Ok(s) => serde_json::from_str(&s).context("deserialize persisted config from JSON")?,
            Err(e) if e.kind() == ErrorKind::NotFound => PersistedConfig::default(),
            Err(e) => return Err(e).context("read persisted config file"),
        };

        let profile = profile_name.and_then(|name| {
            persisted_config.profiles.as_ref().and_then(|ps| {
                ps.iter()
                    .find(|p| p.name.eq(name.as_ref()))
                    .into_iter()
                    .next()
                    .cloned()
            })
        });

        let config = ConfigRs::builder()
            .add_source(Defaults)
            .add_source(persisted_config)
            .add_source(profile.unwrap_or_default())
            .add_source(cli_args)
            .add_source(Environment::with_prefix("kaftui").prefix_separator("_"))
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

        cfg.insert(String::from("format"), Value::from(RecordFormat::default()));

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

        cfg.insert(
            String::from("export_directory"),
            Value::from(String::from(DEFAULT_EXPORT_DIRECTORY)),
        );

        cfg.insert(String::from("logs_enabled"), Value::from(false));

        cfg.insert(
            String::from("logs_max_history"),
            Value::from(DEFAULT_LOGS_MAX_HISTORY),
        );

        cfg.insert(String::from("seek_to"), Value::from(SeekTo::default()));

        Ok(cfg)
    }
}

/// Generates a consumer group id for the Kafka consumer based on the hostname of the machine
/// running the application. If no hostname can be resolved then the current UTC epoch
/// timestamp milliseconds value will be used in it's place.
fn generate_group_id() -> String {
    match gethostname::gethostname().into_string() {
        Ok(name) => format!("{}{}", DEFAULT_CONSUMER_GROUP_ID_PREFIX, name),
        Err(e) => {
            tracing::warn!(
                "falling back to timestamp because hostname could not be resolved: {:?}",
                e
            );
            format!(
                "{}{}",
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
    /// Maximum number of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    max_records: Option<usize>,
    /// Controls how many lines each press of a key scrolls the record value text.
    scroll_factor: Option<u16>,
    /// Directory on the file system where exported files will be saved.
    export_directory: Option<String>,
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

        if let Some(export_directory) = self.export_directory.as_ref() {
            cfg.insert(
                String::from("export_directory"),
                Value::from(export_directory.clone()),
            );
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
    /// CSV of partitions numbers that the consumer should be assigned.
    partitions: Option<String>,
    /// Specifies the format of the data in the Kafka topic, for example `json`.
    format: Option<String>,
    /// Specifies the URL of the Schema Registry that should be used to validate data when
    /// deserializing records from the Kafka topic.
    schema_registry_url: Option<String>,
    /// Specifies the bearer auth token used to connect to the the Schema Registry.
    schema_registry_bearer_token: Option<String>,
    /// Specifies the basic auth user used to connect to the the Schema Registry.
    schema_registry_user: Option<String>,
    /// Specifies the basic auth password used to connect to the the Schema Registry.
    schema_registry_pass: Option<String>,
    /// Specifies the directory where the `.proto` files are located.
    protobuf_dir: Option<String>,
    /// Specifies the Protobuf message type which maps to the records in the Kafka topic.
    protobuf_type: Option<String>,
    /// Id of the consumer group that the application will use when consuming messages from the
    /// Kafka topic.
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

        if let Some(partitions) = self.partitions.as_ref() {
            cfg.insert(String::from("partitions"), Value::from(partitions.clone()));
        }

        if let Some(format) = self.format.as_ref() {
            let record_format: RecordFormat = format.into();
            cfg.insert(String::from("format"), Value::from(record_format));
        }

        if let Some(schema_registry_url) = self.schema_registry_url.as_ref() {
            cfg.insert(
                String::from("schema_registry_url"),
                Value::from(schema_registry_url.clone()),
            );
        }

        if let Some(schema_registry_bearer_token) = self.schema_registry_bearer_token.as_ref() {
            cfg.insert(
                String::from("schema_registry_bearer_token"),
                Value::from(schema_registry_bearer_token.clone()),
            );
        }

        if let Some(schema_registry_user) = self.schema_registry_user.as_ref() {
            cfg.insert(
                String::from("schema_registry_user"),
                Value::from(schema_registry_user.clone()),
            );
        }

        if let Some(schema_registry_pass) = self.schema_registry_pass.as_ref() {
            cfg.insert(
                String::from("schema_registry_pass"),
                Value::from(schema_registry_pass.clone()),
            );
        }

        if let Some(protobuf_dir) = self.protobuf_dir.as_ref() {
            cfg.insert(
                String::from("protobuf_dir"),
                Value::from(protobuf_dir.clone()),
            );
        }

        if let Some(protobuf_type) = self.protobuf_type.as_ref() {
            cfg.insert(
                String::from("protobuf_type"),
                Value::from(protobuf_type.clone()),
            );
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
/// representation for the RGB values as follows: 0x00RRGGBB.
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
    /// Color used for the text in the record list. Defaults to white.
    pub record_list_text_color: String,
    /// Color used for the text in the record info. Defaults to white.
    pub record_info_text_color: String,
    /// Color used for the text in the record value. Defaults to white.
    pub record_value_text_color: String,
    /// Color used for the text in the record headers. Defaults to white.
    pub record_headers_text_color: String,
    /// Color used for the text in the menu items. Defaults to white.
    pub menu_item_text_color: String,
    /// Color used for the text in the currently selected menu item. Defaults to yellow.
    pub selected_menu_item_text_color: String,
    /// Color used for the text in a successful notification message. Defaults to green.
    pub notification_text_color_success: String,
    /// Color used for the text in a warning notification message. Defaults to yellow.
    pub notification_text_color_warn: String,
    /// Color used for the text in an unsuccessful notification message. Defaults to red.
    pub notification_text_color_failure: String,
    /// Color used for the text in the stats UI. Defaults to white.
    pub stats_text_color: String,
    /// Primary color used for bars in a bar graph in the stats UI. Defaults to white.
    pub stats_bar_color: String,
    /// Secondary color used for bars in a bar graph in the stats UI. Defaults to white.
    pub stats_bar_secondary_color: String,
    /// Color used for the throughput chart in the stats UI. Defaults to white.
    pub stats_throughput_color: String,
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
    /// * Label Text - White
    /// * Record List Text - White
    /// * Record Info Text - White
    /// * Record Headers Text - White
    /// * Record Value Text - White
    /// * Menu Item Text - White
    /// * Selected Menu Item Text - Yellow
    /// * Success Notification Text - White
    /// * Warn Notification Text - Yellow
    /// * Failure Notification Text - Red
    /// * Stats Text - White
    /// * Stats Bar - White
    /// * Stats Bar Secondary - White
    /// * Stats Throughput - White
    fn default() -> Self {
        Self {
            panel_border_color: String::from("#FFFFFF"),
            selected_panel_border_color: String::from("#00FFFF"),
            status_text_color_processing: String::from("#00FF00"),
            status_text_color_paused: String::from("#FF0000"),
            key_bindings_text_color: String::from("#FFFFFF"),
            label_color: String::from("#FFFFFF"),
            record_list_text_color: String::from("#FFFFFF"),
            record_info_text_color: String::from("#FFFFFF"),
            record_value_text_color: String::from("#FFFFFF"),
            record_headers_text_color: String::from("#FFFFFF"),
            menu_item_text_color: String::from("#FFFFFF"),
            selected_menu_item_text_color: String::from("#FFFF00"),
            notification_text_color_success: String::from("#FFFFFF"),
            notification_text_color_warn: String::from("#FFFF00"),
            notification_text_color_failure: String::from("#FF0000"),
            stats_text_color: String::from("#FFFFFF"),
            stats_bar_color: String::from("#FFFFFF"),
            stats_bar_secondary_color: String::from("#FFFFFF"),
            stats_throughput_color: String::from("#FFFFFF"),
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

        data.insert(
            String::from("recordListTextColor"),
            Value::from(value.record_list_text_color),
        );

        data.insert(
            String::from("recordInfoTextColor"),
            Value::from(value.record_info_text_color),
        );

        data.insert(
            String::from("recordHeadersTextColor"),
            Value::from(value.record_headers_text_color),
        );

        data.insert(
            String::from("recordValueTextColor"),
            Value::from(value.record_value_text_color),
        );

        data.insert(
            String::from("menuItemTextColor"),
            Value::from(value.menu_item_text_color),
        );

        data.insert(
            String::from("selectedMenuItemTextColor"),
            Value::from(value.selected_menu_item_text_color),
        );

        data.insert(
            String::from("notificationTextColorSuccess"),
            Value::from(value.notification_text_color_success),
        );

        data.insert(
            String::from("notificationTextColorWarn"),
            Value::from(value.notification_text_color_warn),
        );

        data.insert(
            String::from("notificationTextColorFailure"),
            Value::from(value.notification_text_color_failure),
        );

        data.insert(
            String::from("statsTextColor"),
            Value::from(value.stats_text_color),
        );

        data.insert(
            String::from("statsBarColor"),
            Value::from(value.stats_bar_color),
        );

        data.insert(
            String::from("statsBarSecondaryColor"),
            Value::from(value.stats_bar_secondary_color),
        );

        data.insert(
            String::from("statsThroughputColor"),
            Value::from(value.stats_throughput_color),
        );

        Self::Table(data)
    }
}

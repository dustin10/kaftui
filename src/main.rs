mod app;
mod event;
mod kafka;
mod trace;
mod ui;
mod util;

use crate::{
    app::{App, config::Config},
    kafka::{
        Format, SeekTo,
        de::{
            AvroSchemaDeserializer, JsonSchemaDeserializer, JsonStringDeserializer,
            KeyDeserializer, ProtobufSchemaDeserializer, StringDeserializer, ValueDeserializer,
        },
    },
    trace::{CaptureLayer, Log},
};

use anyhow::Context;
use chrono::Local;
use clap::Parser;
use config::{ConfigError, Map, Source, Value};
use schema_registry_client::rest::{
    client_config::ClientConfig,
    schema_registry_client::{Client, SchemaRegistryClient},
};
use std::{fs::File, io::BufReader, sync::Arc};
use tokio::sync::mpsc::Receiver;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Registry, prelude::*};

/// A TUI application which can be used to view records published to a Kafka topic.
#[derive(Clone, Debug, Default, Parser)]
#[command()]
struct Cli {
    /// Host value for the Kafka brokers that the application will connect to when consuming
    /// records.
    #[arg(short, long)]
    bootstrap_servers: Option<String>,
    /// Name of the Kafka topic which records will be consumed from. If no topic is specified, the
    /// application will fallback to displaying the list of topics available on the Kafka cluster
    /// for the user to browse.
    #[arg(short, long)]
    topic: Option<String>,
    /// CSV of the partition numbers that the consumer should be assigned. This argument is used to
    /// restrict the set of partitions that will be consumed. If not specified, all partitions will
    /// be assigned.
    #[arg(long)]
    partitions: Option<String>,
    /// Path to a properties file containing additional configuration for the Kafka consumer other
    /// than the bootstrap servers and group id. Typically, configuration for authentication, etc.
    #[arg(long)]
    consumer_properties: Option<String>,
    /// Specifies the format of the key for the records contained in the Kafka topic. By default,
    /// the key is assumed to be in no special format and no special handling will be applied to it
    /// when displayed. Valid values: `json`, `avro`, or `protobuf`.
    #[arg(short, long)]
    key_format: Option<String>,
    /// Specifies the format of the value of the records contained in the Kafka topic. By default,
    /// the value is assumed to be in no special format and no special handling will be applied to
    /// it when displayed. Valid values: `json`, `avro`, or `protobuf`.
    #[arg(short, long)]
    value_format: Option<String>,
    /// Specifies the URL of the Schema Registry that should be used to validate data when
    /// deserializing records from the Kafka topic.
    #[arg(long)]
    schema_registry_url: Option<String>,
    /// Specifies the bearer authentication token used to connect to the the Schema Registry.
    #[arg(long)]
    schema_registry_bearer_token: Option<String>,
    /// Specifies the basic auth user used to connect to the the Schema Registry.
    #[arg(long)]
    schema_registry_user: Option<String>,
    /// Specifies the basic auth password used to connect to the the Schema Registry.
    #[arg(long)]
    schema_registry_pass: Option<String>,
    /// Specifies the directory where the `.proto` files are located. This argument is required
    /// when the format is set to `protobuf`.
    #[arg(long)]
    protobuf_dir: Option<String>,
    /// Specifies the Protobuf message type which corresponds to the key of the records in the
    /// Kafka topic.
    #[arg(long)]
    key_protobuf_type: Option<String>,
    /// Specifies the Protobuf message type which corresponds to the value of the records in the
    /// Kafka topic. This argument is required when the format is set to `protobuf`.
    #[arg(long)]
    value_protobuf_type: Option<String>,
    /// Id of the consumer group that the application will use when consuming records from the
    /// Kafka topic. By default a group id will be generated from the hostname of the machine that
    /// is executing the application.
    #[arg(short, long)]
    group_id: Option<String>,
    /// CSV of colon separated pairs of partitions and offsets that the Kafka consumer will seek to
    /// before starting to consume records.
    #[arg(long)]
    seek_to: Option<String>,
    /// JSONPath filter that is applied to a records as they are received from the consumer. Can be
    /// used to filter out any records from the Kafka topic that the end user may not be interested
    /// in. A record will only be presented to the user if it matches the filter. By default, no
    /// filter is applied.
    #[arg(short, long)]
    filter: Option<String>,
    /// Specifies the name of pre-configured set of values that will be used as default values for
    /// the execution of the application. Profiles are stored in the $HOME/.kaftui.json file. Any
    /// other arguments specified when executing the application will take precedence over the ones
    /// loaded from the profile.
    #[arg(short, long)]
    profile: Option<String>,
    /// Maximum number of records that should be held in memory and displayed in the record table
    /// at any given time after being consumed from the Kafka topic. Once the number is exceeded
    /// then older records will be removed as newer ones are inserted. Defaults to 256.
    #[arg(long)]
    max_records: Option<usize>,
}

impl Source for Cli {
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
            cfg.insert(String::from("topic"), config::Value::from(topic.clone()));
        }

        if let Some(partitions) = self.partitions.as_ref() {
            cfg.insert(String::from("partitions"), Value::from(partitions.clone()));
        }

        if let Some(key_format) = self.key_format.as_ref() {
            cfg.insert(
                String::from("key_format"),
                Value::from(Format::from(key_format)),
            );
        }

        if let Some(value_format) = self.value_format.as_ref() {
            cfg.insert(
                String::from("value_format"),
                Value::from(Format::from(value_format)),
            );
        }

        if let Some(protobuf_dir) = self.protobuf_dir.as_ref() {
            cfg.insert(
                String::from("protobuf_dir"),
                Value::from(protobuf_dir.clone()),
            );
        }

        if let Some(key_protobuf_type) = self.key_protobuf_type.as_ref() {
            cfg.insert(
                String::from("key_protobuf_type"),
                Value::from(key_protobuf_type.clone()),
            );
        }

        if let Some(value_protobuf_type) = self.value_protobuf_type.as_ref() {
            cfg.insert(
                String::from("value_protobuf_type"),
                Value::from(value_protobuf_type.clone()),
            );
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

        if let Some(group_id) = self.group_id.as_ref() {
            cfg.insert(String::from("group_id"), Value::from(group_id.clone()));
        }

        let seek_to: SeekTo = self.seek_to.as_ref().map(Into::into).unwrap_or_default();
        cfg.insert(String::from("seek_to"), Value::from(seek_to));

        if let Some(filter) = self.filter.as_ref() {
            cfg.insert(String::from("filter"), config::Value::from(filter.clone()));
        }

        if let Some(max_records) = self.max_records.as_ref() {
            cfg.insert(
                String::from("max_records"),
                Value::from(*max_records as i32),
            );
        }

        if let Some(path) = self.consumer_properties.as_ref() {
            let file = File::open(path).expect("properties file can be opened");
            let consumer_properties = java_properties::read(BufReader::new(file)).map_err(|e| {
                ConfigError::Message(format!("failed to read consumer properties file: {}", e))
            })?;

            cfg.insert(
                String::from("consumer_properties"),
                Value::from(consumer_properties),
            );
        }

        Ok(cfg)
    }
}

/// Main entry point for the application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logs_rx = init_env();

    let args = Cli::parse();
    let profile_name = args.profile.clone();

    let config = Config::new(args, profile_name).context("create application config")?;

    run_app(config, logs_rx).await
}

/// Environment variable that can be used to enable capturing logs to a file for debugging.
const LOGS_ENABLED_ENV_VAR: &str = "KAFTUI_LOGS_ENABLED";

/// Environment variable that can be used to specify the directory that log files should be stored
/// in. If logs enabled, but no custom directory is specified using this environment variable then
/// the present working directory, i.e. `.`, will be used.
const LOGS_DIR_ENV_VAR: &str = "KAFTUI_LOGS_DIR";

/// Maximum bound on the number of messages that can be in the logs channel.
const LOGS_CHANNEL_SIZE: usize = 512;

/// Initializes the environment that the application will run in. If logging is enabled, returns
/// the log history that will be written to by the [`CaptureLayer`].
fn init_env() -> Option<Receiver<Log>> {
    let dot_env_result = dotenvy::dotenv();

    if !logs_enabled() {
        return None;
    }

    let logs_dir = logs_dir();

    let file_appender = tracing_appender::rolling::never(
        logs_dir,
        format!(
            "kaftui-logs-{}.json",
            Local::now().format("%d.%m.%Y-%H.%M.%S")
        ),
    );

    let file_layer = tracing_subscriber::fmt::Layer::default()
        .json()
        .with_file(true)
        .with_level(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_target(true)
        .with_writer(file_appender);

    let (logs_rx, logs_tx) = tokio::sync::mpsc::channel(LOGS_CHANNEL_SIZE);

    let capture_layer = CaptureLayer::new(logs_rx);

    // default to INFO level logs but respect the RUST_LOG env var.
    let global_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    Registry::default()
        .with(file_layer)
        .with(capture_layer)
        .with(global_filter)
        .init();

    // process dotenvy result after tracing has been initialized to ensure any relevant logs are
    // emitted and viewable by the end user.
    match dot_env_result {
        Ok(path) => tracing::info!(".env file loaded from {}", path.display()),
        Err(e) => match e {
            dotenvy::Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("no .env file found")
            }
            _ => tracing::warn!("failed to load .env file: {}", e),
        },
    };

    Some(logs_tx)
}

/// Returns true if the user has enabled application logging, false otherwise.
fn logs_enabled() -> bool {
    util::read_env_transformed_or(
        LOGS_ENABLED_ENV_VAR,
        |v| v.eq_ignore_ascii_case("true"),
        false,
    )
}

/// Resolves the directory on the file system where the file containing the application logs should
/// be written. If not configured explicitly by the user with the `KAFTUI_LOGS_DIR` environment
/// variable, then the present working directory, i.e. `.`, will be used.
fn logs_dir() -> String {
    util::read_env_or(LOGS_DIR_ENV_VAR, String::from("."))
}

/// Runs the application.
async fn run_app(config: Config, logs_rx: Option<Receiver<Log>>) -> anyhow::Result<()> {
    let schema_registry_client = create_schema_registry_client(&config);

    let (key_deserializer, value_deserializer) =
        create_deserializers(&config, schema_registry_client)
            .context("create key and value deserializers")?;

    let app = App::new(
        config,
        key_deserializer,
        value_deserializer,
        schema_registry_client,
    )
    .context("initialize application")?;

    let terminal = ratatui::init();

    let result = app.run(terminal, logs_rx).await;

    // make sure to always restore terminal before returning
    ratatui::restore();

    result
}

/// Creeates a [`SchemaRegistryClient`] if a URL is specified in the configuration. The reference to
/// the client is intentionally leaked to ensure it has a `'static` lifetime as required by the
/// Kafka record deserialziers. This is acceptable as the client is intended to live for the entire
/// duration of the application.
fn create_schema_registry_client(config: &Config) -> Option<&'static SchemaRegistryClient> {
    config.schema_registry_url.as_ref().map(|url| {
        let mut client_config = ClientConfig::new(vec![url.clone()]);
        if let Some(bearer) = config.schema_registry_bearer_token.as_ref() {
            tracing::info!("configuring bearer token auth for schema registry client");
            client_config.bearer_access_token = Some(bearer.clone());
        }

        if let Some(user) = config.schema_registry_user.as_ref() {
            tracing::info!("configuring basic auth for schema registry client");
            client_config.basic_auth = Some((user.clone(), config.schema_registry_pass.clone()));
        }

        let client = Box::new(SchemaRegistryClient::new(client_config));

        // do not need a mutable ref
        let client: &SchemaRegistryClient = Box::leak(client);

        client
    })
}

/// Creates both the [`KeyDeserializer`] and [`ValueDeserializer`] that will be used to deserialize
/// record keys and values consumed from the Kafka topic based on the application configuration.
fn create_deserializers(
    config: &Config,
    schema_registry_client: Option<&'static SchemaRegistryClient>,
) -> anyhow::Result<(Arc<dyn KeyDeserializer>, Arc<dyn ValueDeserializer>)> {
    // give special handling to the case where both key and value formats are the same to avoid
    // creating two deserializers of the same type
    match (config.key_format, config.value_format) {
        (Format::None, Format::None) => {
            tracing::info!("using simple string key and value deserializer");

            let deserializer = Arc::new(StringDeserializer);

            Ok((deserializer.clone(), deserializer))
        }
        (Format::Json, Format::Json) => match schema_registry_client {
            Some(schema_registry_client) => {
                tracing::info!("using JSONSchema key and value deserializer with schema registry");

                let json_schema_deserializer = JsonSchemaDeserializer::new(schema_registry_client)
                    .expect("JSONSchema deserializer created");

                let deserializer = Arc::new(json_schema_deserializer);

                Ok((deserializer.clone(), deserializer))
            }
            None => {
                tracing::info!("using JSON key and value deserializer without schema registry");

                let deserializer = Arc::new(JsonStringDeserializer);

                Ok((deserializer.clone(), deserializer))
            }
        },
        (Format::Avro, Format::Avro) => match schema_registry_client {
            Some(client) => {
                tracing::info!("using Avro schema key and value deserializer with schema registry");

                let avro_schema_deserializer =
                    AvroSchemaDeserializer::new(client).expect("Avro schema deserializer created");

                let deserializer = Arc::new(avro_schema_deserializer);

                Ok((deserializer.clone(), deserializer))
            }
            None => {
                anyhow::bail!("schema registry url must be specified when key format is avro")
            }
        },
        (Format::Protobuf, Format::Protobuf) => {
            tracing::info!("using Protobuf schema key and value deserializer with schema registry");

            let protobuf_dir = config.protobuf_dir.as_ref().ok_or_else(|| {
                anyhow::anyhow!("protobuf dir must be set when key format is protobuf")
            })?;

            if config.key_protobuf_type.is_none() && config.value_protobuf_type.is_none() {
                anyhow::bail!(
                    "key and value protobuf type must be specified when format is protobuf"
                );
            }

            let protobuf_schema_deserializer = ProtobufSchemaDeserializer::new(
                protobuf_dir,
                config.key_protobuf_type.clone(),
                config.value_protobuf_type.clone(),
            )
            .context("create Protobuf schema deserializer")?;

            let deserializer = Arc::new(protobuf_schema_deserializer);

            Ok((deserializer.clone(), deserializer))
        }
        (_, _) => {
            let key_deserializer = create_key_deserializer(config, schema_registry_client)?;

            let value_deserializer = create_value_deserializer(config, schema_registry_client)?;

            Ok((key_deserializer, value_deserializer))
        }
    }
}

/// Creates the [`KeyDeserializer`] that will be used to deserialize record keys consumed from
/// the Kafka topic based on the application configuration.
fn create_key_deserializer(
    config: &Config,
    schema_registry_client: Option<&'static SchemaRegistryClient>,
) -> anyhow::Result<Arc<dyn KeyDeserializer>> {
    let key_deserializer: Arc<dyn KeyDeserializer> = match config.key_format {
        Format::None => Arc::new(StringDeserializer),
        Format::Json => match schema_registry_client {
            Some(client) => {
                tracing::info!("using JSONSchema key deserializer with schema registry");

                let json_schema_deserializer =
                    JsonSchemaDeserializer::new(client).expect("JSONSchema deserializer created");

                Arc::new(json_schema_deserializer)
            }
            None => {
                tracing::info!("using JSON key deserializer without schema registry");

                Arc::new(JsonStringDeserializer)
            }
        },
        Format::Avro => match schema_registry_client {
            Some(client) => {
                tracing::info!("using Avro schema key deserializer with schema registry");

                let avro_schema_deserializer =
                    AvroSchemaDeserializer::new(client).expect("Avro schema deserializer created");

                Arc::new(avro_schema_deserializer)
            }
            None => {
                anyhow::bail!("schema registry url must be specified when key format is avro")
            }
        },
        Format::Protobuf => match schema_registry_client {
            Some(_client) => {
                tracing::info!("using Protobuf schema key deserializer with schema registry");

                let protobuf_dir = config.protobuf_dir.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("protobuf dir must be set when key format is protobuf")
                })?;

                if config.key_protobuf_type.is_none() {
                    anyhow::bail!(
                        "key protobuf type must be specified when key format is protobuf"
                    );
                }

                let value_type: Option<String> = None;

                let protobuf_schema_deserializer = ProtobufSchemaDeserializer::new(
                    protobuf_dir,
                    config.key_protobuf_type.clone(),
                    value_type,
                )
                .context("create Protobuf schema deserializer")?;

                Arc::new(protobuf_schema_deserializer)
            }
            None => {
                anyhow::bail!("schema registry url must be specified when key format is protobuf")
            }
        },
    };

    Ok(key_deserializer)
}

/// Creates the [`ValueDeserializer`] that will be used to deserialize record values consumed from
/// the Kafka topic based on the application configuration.
fn create_value_deserializer(
    config: &Config,
    schema_registry_client: Option<&'static SchemaRegistryClient>,
) -> anyhow::Result<Arc<dyn ValueDeserializer>> {
    let value_deserializer: Arc<dyn ValueDeserializer> = match config.value_format {
        Format::None => Arc::new(StringDeserializer),
        Format::Json => match schema_registry_client {
            Some(client) => {
                tracing::info!("using JSONSchema value deserializer with schema registry");

                let json_schema_deserializer =
                    JsonSchemaDeserializer::new(client).expect("JSONSchema deserializer created");

                Arc::new(json_schema_deserializer)
            }
            None => {
                tracing::info!("using JSON value deserializer without schema registry");

                Arc::new(JsonStringDeserializer)
            }
        },
        Format::Avro => match schema_registry_client {
            Some(client) => {
                tracing::info!("using Avro schema value deserializer with schema registry");

                let avro_schema_deserializer =
                    AvroSchemaDeserializer::new(client).expect("Avro schema deserializer created");

                Arc::new(avro_schema_deserializer)
            }
            None => {
                anyhow::bail!("schema registry url must be specified when value format is avro")
            }
        },
        Format::Protobuf => match schema_registry_client {
            Some(_client) => {
                tracing::info!("using Protobuf schema value deserializer with schema registry");

                let protobuf_dir = config.protobuf_dir.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("protobuf dir must be set when value format is protobuf")
                })?;

                if config.value_protobuf_type.is_none() {
                    anyhow::bail!(
                        "key protobuf type must be specified when value format is protobuf"
                    );
                }

                let key_type: Option<String> = None;

                let protobuf_schema_deserializer = ProtobufSchemaDeserializer::new(
                    protobuf_dir,
                    key_type,
                    config.value_protobuf_type.clone(),
                )
                .context("create Protobuf schema deserializer")?;

                Arc::new(protobuf_schema_deserializer)
            }
            None => {
                anyhow::bail!("schema registry url must be specified when value format is protobuf")
            }
        },
    };

    Ok(value_deserializer)
}

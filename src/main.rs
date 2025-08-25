mod app;
mod event;
mod kafka;
mod trace;
mod ui;
mod util;

use crate::{
    app::{config::Config, App},
    kafka::SeekTo,
    trace::{CaptureLayer, Log},
};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use config::{ConfigError, Map, Source, Value};
use std::{fs::File, io::BufReader};
use tokio::sync::mpsc::Receiver;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

/// A TUI application which can be used to view records published to a Kafka topic.
#[derive(Clone, Debug, Default, Parser)]
#[command()]
struct Cli {
    /// Host value for the Kafka brokers that the application will connect to when consuming
    /// records.
    #[arg(short, long)]
    bootstrap_servers: Option<String>,
    /// Name of the Kafka topic which records will be consumed from.
    #[arg(short, long)]
    topic: Option<String>,
    /// CSV of the partition numbers that the consumer should be assigned. This argument is used to
    /// restrict the set of partitions that will be consumed. If not specified, all partitions will
    /// be assigned.
    #[arg(long)]
    partitions: Option<String>,
    /// Id of the consumer group that the application will use when consuming records from the Kafka
    /// topic. By default a group id will be generated from the hostname of the machine that is
    /// execting the application.
    #[arg(short, long)]
    group_id: Option<String>,
    /// CSV of colon separated pairs of partitions and offsets that the Kafka consumer will seek to
    /// before starting to consume records.
    #[arg(long)]
    seek_to: Option<String>,
    /// JSONPath filter that is applied to a records as they are received from the consumer. Can be
    /// used to filter out any records from the Kafka topic that the end user may not be interested
    /// in. A record will only be presented to the user if it matches the filter. By default no
    /// filter is applied.
    #[arg(short, long)]
    filter: Option<String>,
    /// Specifies the name of pre-configured set of values that will be used as default values for
    /// the execution of the application. Profiles are stored in the $HOME/.kaftui.json file. Any
    /// other arguments specified when executing the application will take precedence over the ones
    /// loaded from the profile.
    #[arg(short, long)]
    profile: Option<String>,
    /// Path to a properties file containing additional configuration for the Kafka consumer other
    /// than the bootstrap servers and group id. Typically configuration for authentication, etc.
    #[arg(long)]
    consumer_properties: Option<String>,
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
            Utc::now().format("%d.%m.%Y-%H.%M.%S")
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
                tracing::debug!("no .env file found")
            }
            _ => tracing::warn!("failed to load .env file: {}", e),
        },
    };

    Some(logs_tx)
}

/// Returns true if the user has enabled application logging, false otherwise.
fn logs_enabled() -> bool {
    util::read_env_transformed(
        LOGS_ENABLED_ENV_VAR,
        |v| v.eq_ignore_ascii_case("true"),
        bool::default,
    )
}

/// Resolves the directory on the file system where the file containing the application logs should
/// be written. If not configured explicitly by the user with the `KAFTUI_LOGS_DIR` environment
/// variable, then the present working directory, i.e. `.`, will be used.
fn logs_dir() -> String {
    util::read_env(LOGS_DIR_ENV_VAR, || String::from("."))
}

/// Runs the application.
async fn run_app(config: Config, logs_rx: Option<Receiver<Log>>) -> anyhow::Result<()> {
    let terminal = ratatui::init();

    let result = App::new(config)
        .context("initialize application")?
        .run(terminal, logs_rx)
        .await;

    ratatui::restore();

    result
}

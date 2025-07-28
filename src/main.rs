mod app;
mod event;
mod kafka;
mod ui;

use crate::app::{config::Config, App};

use anyhow::Context;
use clap::Parser;
use config::{ConfigError, Map, Source, Value};
use std::{fs::File, io::BufReader};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// A TUI application which can be used to view records published to Kafka topic.
#[derive(Clone, Debug, Default, Parser)]
#[command()]
struct Args {
    /// Kafka bootstrap servers host value that the application will connect to.
    #[arg(short, long)]
    bootstrap_servers: Option<String>,
    /// Name of the Kafka topic to consume records from.
    #[arg(short, long)]
    topic: Option<String>,
    /// Id of the group that the application will use when consuming records from the Kafka topic.
    /// By default a group id will be generated from the hostname of the machine that is executing
    /// the application.
    #[arg(short, long)]
    group_id: Option<String>,
    /// CSV of color separated pairs of partition and offset that the Kafka consumer will seek to
    /// before starting to consume records.
    #[arg(long)]
    seek_to: Option<String>,
    /// JSONPath filter that is applied to a record. Can be used to filter out any records from the
    /// Kafka topic that the end user may not be interested in. A record will only be presented to
    /// the user if it matches the filter. By default no filter is applied.
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
    consumer_properties_file: Option<String>,
    /// Maximum nunber of records that should be held in memory at any given time after being
    /// consumed from the Kafka topic. Defaults to 256.
    #[arg(long)]
    max_records: Option<usize>,
}

impl Source for Args {
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

        if let Some(seek_to) = self.seek_to.as_ref() {
            cfg.insert(String::from("seek_to"), Value::from(seek_to.clone()));
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

        if let Some(path) = self.consumer_properties_file.as_ref() {
            let file = File::open(path).expect("properties file can be opened");
            let consumer_properties =
                java_properties::read(BufReader::new(file)).expect("properties file can be read");

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
    init_env();

    let args = Args::parse();
    let profile_name = args.profile.clone();

    let config = Config::new(args, profile_name).context("create application config")?;

    run_app(config).await
}

/// Environment variable that can be used to enable capturing logs to a file for debugging.
const ENABLE_LOGS_ENV_VAR: &str = "KAFTUI_ENABLE_LOGS";

/// Initializes the environment that the application will run in.
fn init_env() {
    let dot_env_result = dotenvy::dotenv();

    let enable_logs = std::env::var(ENABLE_LOGS_ENV_VAR)
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if !enable_logs {
        return;
    }

    let remove_result = std::fs::remove_file("output.log");

    let file_writer = tracing_appender::rolling::never(".", "output.log");

    // default to INFO logs but allow the RUST_LOG env variable to override.
    tracing_subscriber::fmt()
        .json()
        .with_writer(file_writer)
        .with_level(true)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    // process results after tracing has been initialized
    match dot_env_result {
        Ok(path) => tracing::info!("loaded .env file from {}", path.display()),
        Err(e) => match e {
            dotenvy::Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!("no .env file found")
            }
            _ => tracing::warn!("failed to load .env file: {}", e),
        },
    };

    match remove_result {
        Ok(_) => tracing::debug!("removed log file from previous run"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!("no log file from previous run found to remove")
        }
        Err(e) => tracing::warn!("failed to remove previous log file: {}", e),
    }
}

/// Runs the application.
async fn run_app(config: Config) -> anyhow::Result<()> {
    let terminal = ratatui::init();

    let result = App::new(config)
        .context("initialize application")?
        .run(terminal)
        .await;

    ratatui::restore();

    result
}

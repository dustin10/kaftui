mod app;
mod event;
mod kafka;
mod ui;

use crate::app::{App, DEFAULT_CONSUMER_GROUP_ID_PREFIX, DEFAULT_MAX_RECORDS};

use anyhow::Context;
use app::Config;
use chrono::Utc;
use clap::Parser;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// The [`Args`] struct is a contains the resolved values for the command line arguments supported
/// by the application.
#[derive(Debug, Parser)]
#[command()]
struct Args {
    /// Kafka bootstrap servers host value that the application will connect to.
    #[arg(short, long)]
    bootstrap_servers: String,
    /// Name of the Kafka topic to consume records from.
    #[arg(short, long)]
    topic: String,
    /// Optional. Id of the group that the application will use when consuming messages from the
    /// Kafka topic. By default a group id will be generated from the hostname of the machine that
    /// is executing the application.
    #[arg(short, long)]
    group_id: Option<String>,
    /// Optional. Path to a properties file containing additional configuration for the Kafka
    /// consumer other than the bootstrap servers and group id. Typically configuration for
    /// authentication, etc.
    #[arg(long)]
    consumer_properties_file: Option<String>,
    /// Optional. Maximum nunber of records that should be held in memory at any given time after
    /// being consumed from the Kafka topic. Defaults to 256.
    #[arg(long)]
    max_records: Option<usize>,
    /// Optional. JSONPath filter that is applied to a record. Can be used to filter out any
    /// records from the Kafka topic that the end user may not be interested in. A message will
    /// only be presented to the user if it matches the filter. By default no filter is applied.
    #[arg(short, long)]
    filter: Option<String>,
}

impl From<Args> for Config {
    /// Consumes and converts an instance of [`Args`] to one of [`Config`].
    fn from(value: Args) -> Self {
        let group_id = value.group_id.unwrap_or_else(generate_group_id);

        Self::builder()
            .bootstrap_servers(value.bootstrap_servers)
            .topic(value.topic)
            .group_id(group_id)
            .consumer_properties_file(value.consumer_properties_file)
            .filter(value.filter)
            .max_records(value.max_records.unwrap_or(DEFAULT_MAX_RECORDS))
            .build()
            .expect("valid app config")
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

/// Main entry point for the application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_env();

    run_app(args).await
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
async fn run_app(args: Args) -> anyhow::Result<()> {
    let terminal = ratatui::init();

    let result = App::new(args.into())
        .context("initialize application")?
        .run(terminal)
        .await;

    ratatui::restore();

    result
}

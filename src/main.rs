mod app;
mod event;
mod kafka;
mod ui;

use crate::app::{App, Config};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// Prefix for the default group id for the Kafka consumer generated from the hostname of the
/// machine the application is running on.
pub const DEFAULT_CONSUMER_GROUP_ID_PREFIX: &str = "kaftui-";

/// Default maximum number of records consumed from the Kafka toic to hold in memory at any given
/// time.
pub const DEFAULT_MAX_RECORDS: usize = 256;

/// The [`Args`] struct is a contains the resolved values for the command line arguments supported
/// by the application.
#[derive(Debug, Parser)]
#[command()]
struct Args {
    /// Kafka bootstrap servers host value that the application will connect to.
    #[arg(short, long)]
    bootstrap_servers: Option<String>,
    /// Name of the Kafka topic to consume records from.
    #[arg(short, long)]
    topic: Option<String>,
    /// Id of the group that the application will use when consuming messages from the
    /// Kafka topic. By default a group id will be generated from the hostname of the machine that
    /// is executing the application.
    #[arg(short, long)]
    group_id: Option<String>,
    /// Path to a properties file containing additional configuration for the Kafka
    /// consumer other than the bootstrap servers and group id. Typically configuration for
    /// authentication, etc.
    #[arg(long)]
    consumer_properties_file: Option<String>,
    /// Maximum nunber of records that should be held in memory at any given time after
    /// being consumed from the Kafka topic. Defaults to 256.
    #[arg(long)]
    max_records: Option<usize>,
    /// JSONPath filter that is applied to a record. Can be used to filter out any
    /// records from the Kafka topic that the end user may not be interested in. A message will
    /// only be presented to the user if it matches the filter. By default no filter is applied.
    #[arg(short, long)]
    filter: Option<String>,
    /// Specifies the name of pre-configured set of values that will be used as default
    /// values for the execution of the application. Profiles are stored in the $HOME/.kaftui.json
    /// file. Any other arguments specified when executing the application will take precedence
    /// over the ones loaded from the profile.
    #[arg(short, long)]
    profile: Option<String>,
}

impl From<Args> for Config {
    /// Consumes and converts an instance of [`Args`] to one of [`Config`].
    ///
    /// # Panic
    ///
    /// The function will panic if a consumer properties file has been specified but it cannot be
    /// read successfully.
    fn from(value: Args) -> Self {
        let group_id = value.group_id.unwrap_or_else(generate_group_id);

        let consumer_properties = if let Some(path) = value.consumer_properties_file {
            let file = File::open(path).expect("properties file can be opened");
            let props =
                java_properties::read(BufReader::new(file)).expect("properties file can be read");

            Some(props)
        } else {
            None
        };

        Self::builder()
            .bootstrap_servers(
                value
                    .bootstrap_servers
                    .expect("bootstrap servers configured"),
            )
            .topic(value.topic.expect("topic configured"))
            .group_id(group_id)
            .consumer_properties(consumer_properties)
            .filter(value.filter)
            .max_records(value.max_records.unwrap_or(DEFAULT_MAX_RECORDS))
            .build()
            .expect("valid app config")
    }
}

/// Configuration that resides in the .kaftui.json file persisted on the user's machine.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedConfig {
    /// Contains any pre-configured [`Profile`]s that the user may have previously configured.
    profiles: Option<Vec<Profile>>,
}

/// A [`Profile`] a persisted set of configuration values that act as the default values for
/// execution of the application.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    init_env();

    let config = init_config().context("initialize application config")?;

    run_app(config).await
}

/// Initializes the configuration for the application. Uses values from the any specified profile
/// as defaults and then overlays arguments on top.
fn init_config() -> anyhow::Result<Config> {
    let args = Args::parse();

    if args.profile.is_none() {
        return Ok(args.into());
    }

    let name = args.profile.as_ref().expect("profile configured");

    let file_path = std::env::home_dir()
        .context("resolve home directory")?
        .join(".kaftui.json");

    let json = match std::fs::read_to_string(file_path) {
        Ok(s) => s,
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Ok(args.into()),
        Err(e) => return Err(e).context("read config file"),
    };

    let config: PersistedConfig =
        serde_json::from_str(&json).context("deserialize persisted config")?;

    if config.profiles.is_none() {
        return Ok(args.into());
    }

    let profiles = config.profiles.expect("profiles configured");

    let mut profile: Option<&Profile> = None;
    for p in profiles.iter() {
        if p.name.eq(name) {
            profile = Some(p);
            break;
        }
    }

    if profile.is_none() {
        return Ok(args.into());
    }

    let profile = profile.expect("profile exists");

    let bootstrap_servers = profile.bootstrap_servers.clone().unwrap_or_else(|| {
        args.bootstrap_servers
            .expect("bootstrap servers configured")
    });

    let topic = profile
        .topic
        .clone()
        .unwrap_or_else(|| args.topic.expect("topic configured"));

    let group_id = profile
        .group_id
        .clone()
        .unwrap_or_else(|| args.group_id.unwrap_or_else(generate_group_id));

    let mut consumer_properties = profile.consumer_properties.clone().unwrap_or_default();

    if let Some(path) = args.consumer_properties_file {
        let file = File::open(path).context("open properties file")?;
        let props = java_properties::read(BufReader::new(file)).context("read properties file")?;

        consumer_properties.extend(props);
    }

    let mut filter = profile.filter.clone();
    if filter.is_none() {
        filter = args.filter;
    }

    let max_records = profile
        .max_records
        .unwrap_or_else(|| args.max_records.unwrap_or(DEFAULT_MAX_RECORDS));

    Config::builder()
        .bootstrap_servers(bootstrap_servers)
        .topic(topic)
        .group_id(group_id)
        .consumer_properties(Some(consumer_properties))
        .filter(filter)
        .max_records(max_records)
        .build()
        .context("valid application config")
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

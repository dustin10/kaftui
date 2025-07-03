mod app;
mod event;
mod kafka;
mod ui;

use crate::app::App;

use app::AppConfig;
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
    /// Name of the topic to consume messages from.
    #[arg(short, long)]
    topic: String,
    /// Optional. Id of the group that the application will use when consuming messages from the Kafka topic.
    #[arg(short, long)]
    group_id: Option<String>,
    /// Optional. Flag indicating that application logs should be output to a file.
    #[arg(long)]
    enable_logs: bool,
}

impl From<Args> for AppConfig {
    /// Consumes and converts an instance of [`Args`] to one of [`AppConfig`].
    fn from(value: Args) -> Self {
        Self {
            bootstrap_servers: value.bootstrap_servers,
            topic: value.topic,
            group_id: value
                .group_id
                .unwrap_or_else(|| String::from("kaftui-consumer")),
        }
    }
}

/// Main entry point for the application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_env(args.enable_logs);

    run_app(args).await
}

/// Initializes the environment that the application will run in.
fn init_env(enable_logs: bool) {
    let dot_env_result = dotenvy::dotenv();

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

    let result = App::new(args.into())?.run(terminal).await;

    ratatui::restore();

    result
}

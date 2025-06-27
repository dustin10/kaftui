mod app;
mod event;
mod ui;

use crate::app::App;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// Main entry point for the application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_env();
    run_app().await
}

/// Initializes the environment that the application will run in.
fn init_env() {
    // TODO: before release disable log output by default and make it opt in - for now always output it
    let dot_env_result = dotenvy::dotenv();

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
async fn run_app() -> anyhow::Result<()> {
    let terminal = ratatui::init();
    let result = App::new().run(terminal).await;
    ratatui::restore();
    result
}

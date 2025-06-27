use crate::app::App;

mod app;
mod event;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let terminal = ratatui::init();
    let result = App::new().run(terminal).await;
    ratatui::restore();
    result
}

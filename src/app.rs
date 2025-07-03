use crate::{
    event::{AppEvent, Event, EventBus},
    kafka::{Consumer, ConsumerConfig, Record},
};

use anyhow::Context;
use crossterm::event::MouseEvent;
use derive_builder::Builder;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent},
    DefaultTerminal,
};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Manages the global appliation state.
#[derive(Debug)]
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
    /// Current [`Record`] that is being viewed.
    pub record: Option<Record>,
}

impl State {
    /// Creates a new default [`State`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for State {
    /// Creates a new instance of [`State`] initialized to the default state.
    fn default() -> Self {
        Self {
            running: true,
            record: None,
        }
    }
}

/// Enumeration of the various screens that the application can display to the end user.
#[derive(Debug, PartialEq)]
pub enum Screen {
    /// Active when the user is viewing messages being consumed from a Kafka topic.
    ConsumeTopic,
}

/// Configuration values which drive the behavior of the application.
#[derive(Builder, Debug)]
pub struct AppConfig {
    /// Kafka bootstrap servers host value that the application will connect to.
    bootstrap_servers: String,
    /// Name of the Kafka topic to consume messages from.
    topic: String,
    /// Id of the consumer group that the application will use when consuming messages from the Kafka topic.
    group_id: String,
    /// JSONPath filter that is applied to a [`Record`]. Can be used to filter out any messages
    /// from the Kafka topic that the end user may not be interested in. A message will only be
    /// presented to the user if it matches the filter.
    filter: Option<String>,
}

impl AppConfig {
    /// Creates a new default [`AppConfigBuilder`].
    pub fn builder() -> AppConfigBuilder {
        AppConfigBuilder::default()
    }
}

/// Drives the execution of the application and coordinates the various subsystems.
pub struct App {
    /// Configuration for the application.
    config: AppConfig,
    /// Contains the current state of the application.
    pub state: State,
    /// Emits events to be handled by the application.
    event_bus: Arc<Mutex<EventBus>>,
    /// Consumer used to read records from a Kafka topic.
    consumer: Consumer,
    /// Holds the [`Screen`] the user is currently viewing.
    pub screen: Screen,
}

impl App {
    /// Creates a new default [`App`].
    pub fn new(config: AppConfig) -> anyhow::Result<Self> {
        let event_bus = Arc::new(Mutex::new(EventBus::new()));

        let consumer_config = ConsumerConfig::builder()
            .bootstrap_servers(config.bootstrap_servers.clone())
            .group_id(config.group_id.clone())
            .build()
            .expect("valid consumer configuration");

        let consumer = Consumer::new(consumer_config, Arc::clone(&event_bus));

        Ok(Self {
            config,
            state: State::default(),
            event_bus,
            consumer,
            screen: Screen::ConsumeTopic,
        })
    }
    /// Run the main loop of the application.
    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        let event_bus_guard = self.event_bus.lock().await;
        event_bus_guard.start();
        std::mem::drop(event_bus_guard);

        self.consumer
            .start(self.config.topic.clone(), self.config.filter.clone())
            .await
            .context("start Kafka consumer")?;

        while self.state.running {
            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;

            let mut event_bus_guard = self.event_bus.lock().await;
            let event = event_bus_guard.next().await?;
            std::mem::drop(event_bus_guard);

            match event {
                Event::Tick => self.tick(),
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Mouse(mouse_event) => {
                        self.on_mouse_event(mouse_event)?
                    }
                    crossterm::event::Event::Key(key_event) => self.on_key_event(key_event).await?,
                    _ => {}
                },
                Event::App(app_event) => match app_event {
                    AppEvent::Quit => self.quit(),
                    AppEvent::RecordReceived(r) => self.on_record_received(r),
                },
            }
        }

        tracing::debug!("application stopped");

        Ok(())
    }
    /// Handles mouse events emitted by the [`EventBus`].
    fn on_mouse_event(&mut self, _mouse_event: MouseEvent) -> anyhow::Result<()> {
        Ok(())
    }
    /// Handles key events emitted by the [`EventBus`].
    async fn on_key_event(&mut self, key_event: KeyEvent) -> anyhow::Result<()> {
        match key_event.code {
            KeyCode::Esc => self.event_bus.lock().await.send(AppEvent::Quit),
            _ => {}
        }

        Ok(())
    }
    /// Handles the record recieved event emitted by the [`EventBus`].
    fn on_record_received(&mut self, record: Record) {
        tracing::debug!("Kafka record received");
        self.state.record = Some(record);
    }
    /// Handles the tick event of the terminal.
    fn tick(&self) {}
    /// Quits the application.
    fn quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
}

pub mod config;
pub mod export;

use crate::{
    app::{config::Config, export::Exporter},
    event::{Event, EventBus},
    kafka::{
        Consumer, ConsumerConfig, ConsumerEvent, ConsumerMode, Record,
        de::{KeyDeserializer, ValueDeserializer},
        schema::{HttpSchemaClient, Schema},
    },
    trace::Log,
    ui::{
        Component, Logs, LogsConfig, Records, RecordsConfig, Schemas, SchemasConfig, Settings,
        SettingsConfig, Stats, StatsConfig,
    },
};

use anyhow::Context;
use chrono::{DateTime, Duration, Local};
use crossterm::event::{KeyCode, KeyEvent};
use futures::{FutureExt, StreamExt};
use ratatui::{DefaultTerminal, crossterm::event::Event as TerminalEvent};
use schema_registry_client::rest::{
    client_config::ClientConfig,
    schema_registry_client::{Client, SchemaRegistryClient},
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::Arc,
};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver};

/// Size of the buffer that polled application events are placed into.
const APP_EVENTS_BUFFER_SIZE: usize = 16;

/// Maximum bound on the number of messages that can be in the consumer channel.
const CONSUMER_EVENTS_CHANNEL_SIZE: usize = 1024;

/// Size of the buffer that polled consumer events are placed into.
const CONSUMER_EVENTS_BUFFER_SIZE: usize = 64;

/// Maximum bound on the number of messages that can be in the terminal channel.
const TERMINAL_EVENT_CHANNEL_SIZE: usize = 64;

/// Size of the buffer that polled log events are placed into.
const LOG_EVENT_BUFFER_SIZE: usize = 16;

/// Number of notification seconds after a [`Notification`] is created that it should not be
/// eligible to visible to the user any longer.
const NOTIFICATION_EXPIRATION_SECS: i64 = 3;

/// Number of seconds that make up the interval between ticks. The tick event allows the
/// application to perform any periodic operations that are not event-driven.
const TICK_INTERVAL_SECS: u64 = 1;

/// Holds data relevant to a key press that was buffered because it did not directly map to an
/// action. This is used for a simple implementation of vim-style key bindings, e.g. `gg` is bound
/// to selecting the first record in the list.
#[derive(Debug)]
pub struct BufferedKeyPress {
    /// Last key that was pressed that did not map to an action.
    key: char,
    /// Time that the buffered key press will expire.
    ttl: DateTime<Local>,
}

impl BufferedKeyPress {
    /// Creates a new [`BufferedKeyPress`] with the key that was pressed by the user.
    fn new(key: char) -> Self {
        Self {
            key,
            ttl: Local::now() + Duration::seconds(1),
        }
    }
    /// Determines if the key press matches the specified character. False will always be returned
    /// if the key press has expired.
    pub fn is(&self, key: char) -> bool {
        !self.is_expired() && self.key == key
    }
    /// Determines if the key press has expired based on the TTL that was set when it was initially
    /// buffered.
    fn is_expired(&self) -> bool {
        self.ttl < Local::now()
    }
}

/// Enumeration of the available status values that a [`Notification`] can have.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NotificationStatus {
    /// Notification of a successful action.
    Success,
    /// Notification is a warning. Usually something didn't work but a default was used instead or
    /// some other default action was taken.
    #[allow(dead_code)]
    Warn,
    /// Notification of a failed action.
    Failure,
}

/// A [`Notification`] is a message that is presented to the user with the results of either an
/// action that is taken by them or by the application itself, e.g. the result of exporting a
/// record to a file.
#[derive(Clone, Debug)]
pub struct Notification {
    /// Status of the notification.
    pub status: NotificationStatus,
    /// Summary text for the notification. The summary is displayed in the header for a short
    /// period of time.
    pub summary: String,
    /// Timestamp when the notification was created by the application.
    pub created: DateTime<Local>,
}

impl Notification {
    /// Creates a new notification for the user with the specified data.
    pub fn new(status: NotificationStatus, summary: impl Into<String>) -> Self {
        Self {
            status,
            summary: summary.into(),
            created: Local::now(),
        }
    }
    /// Creates a new success notification for the user with the specified data.
    pub fn success(summary: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Success, summary)
    }
    /// Creates a new warn notification for the user with the specified data.
    #[allow(dead_code)]
    pub fn warn(summary: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Warn, summary)
    }
    /// Creates a new failure notification for the user with the specified data.
    pub fn failure(summary: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Failure, summary)
    }
    /// Determines if the notification has expired and should no longer be visible.
    fn is_expired(&self) -> bool {
        (self.created + Duration::seconds(NOTIFICATION_EXPIRATION_SECS)) < Local::now()
    }
}

/// Manages the global application state.
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
    /// Flag that indicates whether the application is initializing.
    pub initializing: bool,
    /// Stores the current [`ConsumerMode`] of the application which controls whether or not
    /// records are currently being consumed from the topic.
    pub consumer_mode: Rc<Cell<ConsumerMode>>,
    /// [`Component`] that the user is currently viewing and interacting with.
    pub active_component: Rc<RefCell<dyn Component>>,
    /// Contains any [`Notification`]s that should be displayed to the user.
    pub notification: Option<Notification>,
}

impl State {
    /// Creates a new [`State`] with the given dependencies.
    pub fn new(
        consumer_mode: Rc<Cell<ConsumerMode>>,
        active_component: Rc<RefCell<dyn Component>>,
    ) -> Self {
        Self {
            running: true,
            initializing: true,
            consumer_mode,
            active_component,
            notification: None,
        }
    }
    /// Sets the active [`Component`] that the user is viewing and interacting with.
    fn activate_component(&mut self, component: Rc<RefCell<dyn Component>>) {
        self.active_component = component;
        self.active_component.borrow_mut().on_activate();
    }
}

/// Drives the execution of the application and coordinates the various subsystems.
pub struct App {
    /// Configuration for the application.
    pub config: Rc<Config>,
    /// Contains the current state of the application.
    pub state: State,
    /// All [`Component`]s available to the user.
    pub components: Vec<Rc<RefCell<dyn Component>>>,
    /// Buffers the valid `char`s that correspond to menu items.
    menu_item_chars: Vec<char>,
    /// If available, contains the last key pressed that did not map to an active key binding.
    buffered_key_press: Option<BufferedKeyPress>,
    /// Channel receiver that is used to receive application events.
    event_rx: UnboundedReceiver<Event>,
    /// Channel receiver that is used to receive records from the Kafka consumer.
    consumer_rx: Receiver<ConsumerEvent>,
    /// Emits events to be handled by the application.
    event_bus: Arc<EventBus>,
    /// Consumer used to read records from a Kafka topic.
    consumer: Arc<Consumer>,
    /// Responsible for exporting Kafka records to the file system.
    exporter: Exporter,
}

impl App {
    /// Creates a new [`App`] with the specified dependencies.
    pub fn new(
        config: Config,
        key_deserializer: Arc<dyn KeyDeserializer>,
        value_deserializer: Arc<dyn ValueDeserializer>,
    ) -> anyhow::Result<Self> {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        let event_bus = Arc::new(EventBus::new(event_tx));

        let (consumer_tx, consumer_rx) = tokio::sync::mpsc::channel(CONSUMER_EVENTS_CHANNEL_SIZE);

        let mut consumer_props = HashMap::new();

        if let Some(ref props) = config.consumer_properties {
            consumer_props.extend(props.clone());
        }

        consumer_props.insert(
            String::from("bootstrap.servers"),
            config.bootstrap_servers.clone(),
        );

        consumer_props.insert(String::from("group.id"), config.group_id.clone());

        let partitions = config
            .partitions
            .as_ref()
            .map(|csv| csv.split(","))
            .map(|ps| {
                ps.map(|p| p.parse::<i32>().expect("valid partition value"))
                    .collect()
            })
            .unwrap_or_default();

        let consumer_config = ConsumerConfig::builder()
            .props(consumer_props)
            .topic(config.topic.clone())
            .partitions(partitions)
            .seek_to(config.seek_to.clone())
            .filter(config.filter.clone())
            .build()
            .expect("valid ConsumerConfig");

        let consumer = Consumer::new(
            consumer_config,
            key_deserializer,
            value_deserializer,
            consumer_tx,
        )
        .context("create consumer")?;

        let exporter = Exporter::new(config.export_directory.clone(), config.format);

        let consumer_mode = Rc::new(Cell::new(ConsumerMode::Processing));

        let records_component = Rc::new(RefCell::new(Records::new(
            RecordsConfig::builder()
                .consumer_mode(Rc::clone(&consumer_mode))
                .topic(config.topic.clone())
                .filter(config.filter.clone())
                .theme(&config.theme)
                .scroll_factor(config.scroll_factor)
                .max_records(config.max_records)
                .build()
                .expect("valid Records config"),
        )));

        let stats_component = Rc::new(RefCell::new(Stats::new(
            StatsConfig::builder()
                .consumer_mode(Rc::clone(&consumer_mode))
                .topic(config.topic.clone())
                .filter(config.filter.clone())
                .theme(&config.theme)
                .build()
                .expect("valid Stats config"),
        )));

        let mut components: Vec<Rc<RefCell<dyn Component>>> =
            vec![records_component.clone(), stats_component];

        if let Some(schema_registry_url) = config.schema_registry_url.as_ref() {
            // TODO: share schema registry client with the deserializer instead of creating a new
            // one here.
            let mut schema_registry_client_config =
                ClientConfig::new(vec![schema_registry_url.clone()]);

            if let Some(bearer) = config.schema_registry_bearer_token.as_ref() {
                tracing::info!("configuring bearer token auth for schema registry client");
                schema_registry_client_config.bearer_access_token = Some(bearer.clone());
            }

            if let Some(user) = config.schema_registry_user.as_ref() {
                tracing::info!("configuring basic auth for schema registry client");
                schema_registry_client_config.basic_auth =
                    Some((user.clone(), config.schema_registry_pass.clone()));
            }

            let schema_client =
                HttpSchemaClient::new(SchemaRegistryClient::new(schema_registry_client_config));

            let schemas_component = Rc::new(RefCell::new(Schemas::new(
                SchemasConfig::builder()
                    .schema_client(schema_client)
                    .scroll_factor(config.scroll_factor)
                    .theme(&config.theme)
                    .build()
                    .expect("valid Schemas config"),
            )));

            components.push(schemas_component);
        }

        let config = Rc::new(config);

        components.push(Rc::new(RefCell::new(Settings::new(
            SettingsConfig::builder()
                .config(Rc::clone(&config))
                .theme(&config.theme)
                .build()
                .expect("valid Settings config"),
        ))));

        if config.logs_enabled {
            let logs_component = Rc::new(RefCell::new(Logs::new(
                LogsConfig::builder()
                    .max_history(config.logs_max_history as usize)
                    .theme(&config.theme)
                    .build()
                    .expect("valid Notifications config"),
            )));

            components.push(logs_component);
        }

        let mut menu_item_chars = Vec::new();
        for i in 0..components.len() {
            let index = u8::try_from(i).expect("valid char") + 1;
            let item = (index + b'0') as char;

            menu_item_chars.push(item);
        }

        let state = State::new(consumer_mode, records_component);

        Ok(Self {
            config,
            state,
            event_rx,
            consumer_rx,
            event_bus,
            consumer: Arc::new(consumer),
            exporter,
            components,
            menu_item_chars,
            buffered_key_press: None,
        })
    }
    /// Run the main loop of the application.
    pub async fn run(
        mut self,
        mut terminal: DefaultTerminal,
        logs_rx: Option<Receiver<Log>>,
    ) -> anyhow::Result<()> {
        let (terminal_tx, mut terminal_rx) =
            tokio::sync::mpsc::channel(TERMINAL_EVENT_CHANNEL_SIZE);

        self.start_poll_terminal_async(terminal_tx);

        self.start_poll_consumer_async();

        if let Some(rx) = logs_rx {
            self.start_poll_logs_async(rx);
        }

        let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(TICK_INTERVAL_SECS));

        while self.state.running {
            terminal
                .draw(|frame| self.draw(frame))
                .context("draw UI to screen")?;

            let mut app_events_buffer = Vec::with_capacity(APP_EVENTS_BUFFER_SIZE);

            let mut consumer_events_buffer = Vec::with_capacity(CONSUMER_EVENTS_BUFFER_SIZE);

            tokio::select! {
                biased;
                Some(terminal_event) = terminal_rx.recv() => {
                    if let TerminalEvent::Key(key_event) = terminal_event {
                        self.on_key_event(key_event);
                    }
                }
                app_events_count =
                    self.event_rx.recv_many(&mut app_events_buffer, APP_EVENTS_BUFFER_SIZE) => {
                    if app_events_count > 0 {
                        for app_event in app_events_buffer.into_iter() {
                            self.on_app_event(app_event);
                        }
                    }
                },
                consumer_events_count =
                    self.consumer_rx.recv_many(&mut consumer_events_buffer, CONSUMER_EVENTS_BUFFER_SIZE) => {
                    if consumer_events_count > 0 {
                        for consumer_event in consumer_events_buffer.into_iter() {
                            self.on_consumer_event(consumer_event);
                        }
                    }
                }
                _ = tick.tick() => self.on_tick(),
            }
        }

        tracing::info!("exited main application loop");

        Ok(())
    }
    /// Starts the asynchronous task which polls the terminal for events.
    fn start_poll_terminal_async(&self, tx: Sender<TerminalEvent>) {
        let poll_terminal_task = PollTerminalTask { tx };

        tokio::spawn(async move {
            poll_terminal_task.run().await;
        });
    }
    /// Starts the consumer asynchronously. The result of the consumer startup is sent back to the
    /// application through the [`EventBus`].
    fn start_poll_consumer_async(&self) {
        let start_consumer_task = StartConsumerTask {
            consumer: Arc::clone(&self.consumer),
            event_bus: Arc::clone(&self.event_bus),
        };

        tokio::spawn(async move {
            start_consumer_task.run().await;
        });
    }
    /// Spawns a task that will receive [`Log`] messages on the specified [`Receiver`] and then
    /// publish an [`Event::LogEmitted`] application event.
    fn start_poll_logs_async(&self, rx: Receiver<Log>) {
        let poll_logs_task = PollLogsTask {
            rx,
            event_bus: Arc::clone(&self.event_bus),
        };

        tokio::spawn(async move {
            poll_logs_task.run().await;
        });
    }
    /// Handles the consumer started event emitted by the [`EventBus`].
    fn on_consumer_started(&mut self) {
        tracing::info!("Kafka consumer started");
        self.state.initializing = false;
    }
    /// Handles the tick event which fires at a regular interval. This allows the application to
    /// perform any periodic operations that are not event-driven.
    fn on_tick(&mut self) {
        if let Some(notification) = self.state.notification.as_ref()
            && notification.is_expired()
        {
            self.state.notification = None;
        }
    }
    /// Handles key events emitted by the [`EventBus`]. First attempts to map the event to an
    /// application level action and then defers to the active [`Component`].
    fn on_key_event(&mut self, key_event: KeyEvent) {
        let app_event = match key_event.code {
            KeyCode::Esc => Some(Event::Quit),
            KeyCode::Tab => Some(Event::SelectNextWidget),
            KeyCode::Char(c) if self.menu_item_chars.contains(&c) => {
                let digit = c.to_digit(10).expect("valid digit") - 1;
                let selected = digit as usize;

                Some(Event::SelectComponent(selected))
            }
            _ => self
                .state
                .active_component
                .borrow()
                .map_key_event(key_event, self.buffered_key_press.as_ref()),
        };

        if let Some(e) = app_event {
            self.buffered_key_press = None;
            self.on_app_event(e);
        } else if let KeyCode::Char(c) = key_event.code {
            self.buffered_key_press = Some(BufferedKeyPress::new(c));
        }
    }
    /// Handles application [`Event`]s either received over the [`EventBus`] or mapped directly by
    /// the application when events are received on other channels.
    fn on_app_event(&mut self, event: Event) {
        match event {
            Event::Quit => self.on_quit(),
            Event::ConsumerStarted => self.on_consumer_started(),
            Event::ConsumerStartFailure(e) => {
                panic!("failed to start Kafka consumer: {}", e)
            }
            Event::SelectComponent(idx) => self.on_select_component(idx),
            Event::ExportRecord(record) => self.on_export_record(record),
            Event::PauseProcessing => self.on_pause_processing(),
            Event::ResumeProcessing => self.on_resume_processing(),
            Event::DisplayNotification(notification) => self.on_display_notification(notification),
            Event::SelectNextWidget => self
                .state
                .active_component
                .borrow_mut()
                .on_app_event(&event),
            Event::ExportSchema(schema) => self.on_export_schema(schema),
            _ => {
                self.components
                    .iter()
                    .for_each(|c| c.borrow_mut().on_app_event(&event));
            }
        }
    }
    /// Handles [`ConsumerEvent`]s received on the Kafka consumer channel.
    fn on_consumer_event(&mut self, consumer_event: ConsumerEvent) {
        let app_event = match consumer_event {
            ConsumerEvent::Received(record) => Event::RecordReceived(record),
            ConsumerEvent::Filtered(record) => Event::RecordFiltered(record),
            ConsumerEvent::Statistics(stats) => Event::StatisticsReceived(stats),
        };

        self.on_app_event(app_event);
    }
    /// Handles the [`Event::ExportRecord`] event emitted by the [`EventBus`].
    fn on_export_record(&mut self, record: Record) {
        tracing::debug!("exporting selected record");

        let notification = match self.exporter.export_record(record) {
            Ok(path) => {
                tracing::info!("record exported to {}", path);
                Notification::success("Record Exported Successfully")
            }
            Err(e) => {
                tracing::error!("failed to export record: {}", e);
                Notification::failure("Record Export Failed")
            }
        };

        self.event_bus
            .send(Event::DisplayNotification(notification));
    }
    /// Handles the [`Event::ExportSchema`] event emitted by the [`EventBus`].
    fn on_export_schema(&mut self, schema: Schema) {
        tracing::debug!("exporting selected schema");

        let notification = match self.exporter.export_schema(schema) {
            Ok(path) => {
                tracing::info!("schema exported to {}", path);
                Notification::success("Schema Exported Successfully")
            }
            Err(e) => {
                tracing::error!("failed to export schema: {}", e);
                Notification::failure("Schema Export Failed")
            }
        };

        self.event_bus
            .send(Event::DisplayNotification(notification));
    }
    /// Handles the [`Event::PauseProcessing`] event emitted by the [`EventBus`].
    fn on_pause_processing(&mut self) {
        if self.state.consumer_mode.get() == ConsumerMode::Processing {
            tracing::info!("pausing Kafka consumer");

            self.state.consumer_mode.set(ConsumerMode::Paused);

            let notification = match self.consumer.pause() {
                Ok(_) => Notification::success("Consumer Paused Successfully"),
                Err(e) => {
                    tracing::error!("failed to pause consumer: {}", e);
                    Notification::failure("Pause Consumer Failed")
                }
            };

            self.event_bus
                .send(Event::DisplayNotification(notification));
        }
    }
    /// Handles the [`Event::ResumeProcessing`] event emitted by the [`EventBus`].
    fn on_resume_processing(&mut self) {
        if self.state.consumer_mode.get() == ConsumerMode::Paused {
            tracing::info!("resuming Kafka consumer");

            self.state.consumer_mode.set(ConsumerMode::Processing);

            let notification = match self.consumer.resume() {
                Ok(_) => Notification::success("Consumer Resumed Successfully"),
                Err(e) => {
                    tracing::error!("failed to resume consumer: {}", e);
                    Notification::failure("Resume Consumer Failed")
                }
            };

            self.event_bus
                .send(Event::DisplayNotification(notification));
        }
    }
    /// Handles the [`Event::DisplayNotification`] event emitted by the [`EventBus`].
    fn on_display_notification(&mut self, notification: Notification) {
        self.state.notification = Some(notification);
    }
    /// Handles the [`Event::Quit`] event emitted by the [`EventBus`].
    fn on_quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
    /// Handles the [`Event::SelectComponent`] event emitted by the [`EventBus`].
    fn on_select_component(&mut self, idx: usize) {
        tracing::debug!("attemping to select component {}", idx);

        if let Some(component) = self.components.get(idx) {
            tracing::debug!("activating {} component", component.borrow().name());
            self.state.activate_component(Rc::clone(component));
        }
    }
}

/// Asynchronous task that starts the Kafka consumer.
struct StartConsumerTask {
    /// Kafka consumer to start.
    consumer: Arc<Consumer>,
    /// [`EventBus`] on which the results of the startup task will be published.
    event_bus: Arc<EventBus>,
}

impl StartConsumerTask {
    /// Runs the task. Starts the consumer and send the appropriate [`Event`] based on the result
    /// of startup on the [`EventBus`].
    async fn run(self) {
        match self.consumer.start() {
            Ok(_) => self.event_bus.send(Event::ConsumerStarted),
            Err(e) => self.event_bus.send(Event::ConsumerStartFailure(e)),
        };
    }
}

/// Asynchronous task that polls the terminal backend for events for the application to handle.
struct PollTerminalTask {
    /// Channel [`Sender`] that is used to send [`TerminalEvent`]s as they are polled.
    tx: Sender<TerminalEvent>,
}

impl PollTerminalTask {
    /// Runs the task. Receives terminal events as they are produced and then send them over the
    /// terminal event channel to be handled in the main application loop.
    async fn run(self) {
        let mut reader = crossterm::event::EventStream::new();

        loop {
            let terminal_event = reader.next().fuse();

            tokio::select! {
                _ = self.tx.closed() => {
                    tracing::warn!("exiting poll terminal event loop because sender was closed");
                    break;
                }
                Some(Ok(event)) = terminal_event => {
                    self.on_terminal_event(event).await;
                }
            };
        }
    }
    /// Invoked when a [`TerminalEvent`] is received from the event stream.
    async fn on_terminal_event(&self, event: TerminalEvent) {
        match event {
            TerminalEvent::Key(key_event) => {
                tracing::debug!(
                    "application received key event with code '{}'",
                    key_event.code
                );

                if let Err(e) = self.tx.send(event).await {
                    tracing::error!("failed to send terminal event on channel: {}", e);
                }
            }
            TerminalEvent::FocusGained => tracing::debug!("application gained focus"),
            TerminalEvent::FocusLost => tracing::debug!("application lost focus"),
            TerminalEvent::Resize(w, h) => tracing::debug!("application resized to {}x{}", w, h),
            TerminalEvent::Mouse(_) => tracing::debug!("application received mouse event"),
            TerminalEvent::Paste(_) => tracing::debug!("application received paste event"),
        }
    }
}

/// Asynchronous task that receives logs emitted from the logging system and emits them as
/// application events.
struct PollLogsTask {
    /// Channel receiver that is used to receive logs emitted by the application.
    rx: Receiver<Log>,
    /// [`EventBus`] on which the results of the startup task will be published.
    event_bus: Arc<EventBus>,
}

impl PollLogsTask {
    /// Runs the task. Receives [`Log`]s emitted by the application and dispatches the
    /// [`Event::LogEmitted`] event on the [`EventBus`].
    async fn run(mut self) {
        loop {
            let mut logs_buffer = Vec::with_capacity(LOG_EVENT_BUFFER_SIZE);

            if self
                .rx
                .recv_many(&mut logs_buffer, LOG_EVENT_BUFFER_SIZE)
                .await
                > 0
            {
                for log in logs_buffer.into_iter() {
                    self.event_bus.send(Event::LogEmitted(log));
                }
            }
        }
    }
}

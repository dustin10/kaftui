pub mod config;
pub mod export;

use crate::{
    app::{config::Config, export::Exporter},
    event::{AppEvent, Event, EventBus},
    kafka::{Consumer, ConsumerMode, PartitionOffset, Record, RecordState},
    trace::Log,
    ui::{Component, Logs, LogsConfig, Records, RecordsConfig, Stats, StatsConfig},
};

use anyhow::Context;
use bounded_vec_deque::BoundedVecDeque;
use chrono::{DateTime, Duration, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::DefaultTerminal;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::Receiver;

/// Holds data relevant to a key press that was buffered because it did not directly map to an
/// action. This is used for a simple implementation of vim-style key bindings, e.g. `gg` is bound
/// to selecting the first record in the list.
#[derive(Debug)]
pub struct BufferedKeyPress {
    /// Last key that was pressed that did not map to an action.
    key: char,
    /// Time that the buffered key press will expire.
    ttl: DateTime<Utc>,
}

impl BufferedKeyPress {
    /// Creates a new [`BufferedKeyPress`] with the key that was pressed by the user.
    fn new(key: char) -> Self {
        Self {
            key,
            ttl: Utc::now() + Duration::seconds(1),
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
        self.ttl < Utc::now()
    }
}

/// Number of notification seconds after a [`Notification`] is created that it should not be
/// eligible to visible to the user any longer.
const NOTIFICATION_EXPIRATION_SECS: i64 = 3;

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
    pub created: DateTime<Utc>,
}

impl Notification {
    /// Creates a new notification for the user with the specified data.
    pub fn new(status: NotificationStatus, summary: impl Into<String>) -> Self {
        Self {
            status,
            summary: summary.into(),
            created: Utc::now(),
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
    pub fn is_expired(&self) -> bool {
        (self.created + Duration::seconds(NOTIFICATION_EXPIRATION_SECS)) < Utc::now()
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
    /// [`Component`] that the user is curently viewing and interacting with.
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
}

/// Drives the execution of the application and coordinates the various subsystems.
pub struct App {
    /// Configuration for the application.
    pub config: Config,
    /// Contains the current state of the application.
    pub state: State,
    /// All [`Component`]s available to the user.
    pub components: Vec<Rc<RefCell<dyn Component>>>,
    /// If available, contains the last key pressed that did not map to an active key binding.
    buffered_key_press: Option<BufferedKeyPress>,
    /// Channel receiver that is used to receive application events that are sent by the
    /// [`EventBus`].
    event_rx: Receiver<Event>,
    /// Channel receiver that is used to receive records from the Kafka consumer.
    consumer_rx: Receiver<RecordState>,
    /// Emits events to be handled by the application.
    event_bus: Arc<EventBus>,
    /// Consumer used to read records from a Kafka topic.
    consumer: Arc<Consumer>,
    /// Responsible for exporting Kafka records to the file system.
    exporter: Exporter,
}

/// Maximum bound on the nunber of messages that can be in the event channel.
const EVENT_CHANNEL_SIZE: usize = 16;

/// Maximum bound on the nunber of messages that can be in the consumer channel.
const CONSUMER_CHANNEL_SIZE: usize = 64;

impl App {
    /// Creates a new [`App`] with the specified dependencies.
    pub fn new(
        config: Config,
        logs: Option<Arc<Mutex<BoundedVecDeque<Log>>>>,
    ) -> anyhow::Result<Self> {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(EVENT_CHANNEL_SIZE);

        let event_bus = Arc::new(EventBus::new(event_tx));

        let (consumer_tx, consumer_rx) = tokio::sync::mpsc::channel(CONSUMER_CHANNEL_SIZE);

        let mut consumer_config = HashMap::new();

        if let Some(ref props) = config.consumer_properties {
            consumer_config.extend(props.clone());
        }

        consumer_config.insert(
            String::from("bootstrap.servers"),
            config.bootstrap_servers.clone(),
        );
        consumer_config.insert(String::from("group.id"), config.group_id.clone());

        let consumer = Consumer::new(consumer_config, consumer_tx).context("create consumer")?;

        let exporter = Exporter::new(config.export_directory.clone());

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

        if let Some(logs) = logs {
            let logs_component = Rc::new(RefCell::new(Logs::new(
                LogsConfig::builder()
                    .logs(logs)
                    .theme(&config.theme)
                    .build()
                    .expect("valid Notifications config"),
            )));

            components.push(logs_component);
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
            buffered_key_press: None,
        })
    }
    /// Run the main loop of the application.
    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        self.event_bus.start();

        self.start_consumer_async();

        while self.state.running {
            terminal
                .draw(|frame| self.draw(frame))
                .context("draw UI to screen")?;

            if let Ok(event) = self.event_rx.try_recv() {
                match event {
                    Event::Crossterm(crossterm_event) => {
                        if let crossterm::event::Event::Key(key_event) = crossterm_event {
                            self.on_key_event(key_event).await;
                        }
                    }
                    Event::App(app_event) => match app_event {
                        AppEvent::Quit => self.on_quit(),
                        AppEvent::ConsumerStarted => self.on_consumer_started(),
                        AppEvent::ConsumerStartFailure(e) => {
                            anyhow::bail!("failed to start Kafka consumer: {}", e)
                        }
                        AppEvent::SelectComponent(idx) => self.on_select_component(idx),
                        AppEvent::ExportRecord(record) => self.on_export_record(record).await,
                        AppEvent::PauseProcessing => self.on_pause_processing().await,
                        AppEvent::ResumeProcessing => self.on_resume_processing().await,
                        AppEvent::DisplayNotification(notification) => {
                            self.on_display_notification(notification)
                        }
                        _ => {
                            self.components
                                .iter()
                                .for_each(|c| c.borrow_mut().on_app_event(&app_event));
                        }
                    },
                }
            }

            if let Ok(consumed_record) = self.consumer_rx.try_recv() {
                match consumed_record {
                    RecordState::Received(record) => self.on_record_received(record).await,
                    RecordState::Filtered(record) => self.on_record_filtered(record).await,
                }
            }
        }

        tracing::debug!("application stopped");

        Ok(())
    }
    /// Starts the consumer asynchronously. The result of the consumer startup is sent back to the
    /// application through the [`EventBus`].
    fn start_consumer_async(&self) {
        let partitions = self
            .config
            .partitions
            .as_ref()
            .map(|csv| csv.split(","))
            .map(|ps| {
                ps.map(|p| p.parse::<i32>().expect("valid partition value"))
                    .collect()
            })
            .unwrap_or_default();

        let seek_to = self
            .config
            .seek_to
            .as_ref()
            .map(|csv| csv.split(",").map(Into::into).collect())
            .unwrap_or_default();

        let start_consumer_task = StartConsumerTask {
            consumer: Arc::clone(&self.consumer),
            topic: self.config.topic.clone(),
            partitions,
            seek_to,
            filter: self.config.filter.clone(),
            event_bus: Arc::clone(&self.event_bus),
        };

        tokio::spawn(async move {
            start_consumer_task.run().await;
        });
    }
    /// Handles the consumer started event emitted by the [`EventBus`].
    fn on_consumer_started(&mut self) {
        tracing::debug!("consumer started");
        self.state.initializing = false;
    }
    /// Invoked when a new [`Record`] is received on the consumer channel.
    async fn on_record_received(&mut self, record: Record) {
        tracing::debug!("Kafka record received");
        self.event_bus.send(AppEvent::RecordReceived(record)).await;
    }
    async fn on_record_filtered(&mut self, record: Record) {
        tracing::debug!("Kafka record filtered");
        self.event_bus.send(AppEvent::RecordFiltered(record)).await;
    }
    /// Handles key events emitted by the [`EventBus`]. First attempts to map the event to an
    /// application level action and then defers to the active [`Component`].
    async fn on_key_event(&mut self, key_event: KeyEvent) {
        // TODO: cleanup if possible and probably move to struct property so we arent recomputing
        // on every event.
        let mut menu_items = Vec::new();
        for i in 0..self.components.len() {
            let index = u8::try_from(i).expect("valid char") + 1;
            let item = (index + b'0') as char;

            menu_items.push(item);
        }

        let app_event = match key_event.code {
            KeyCode::Esc => Some(AppEvent::Quit),
            KeyCode::Tab => Some(AppEvent::SelectNextWidget),
            KeyCode::Char(c) if menu_items.contains(&c) => {
                let digit = c.to_digit(10).expect("valid digit") - 1;
                let selected = digit as usize;

                Some(AppEvent::SelectComponent(selected))
            }
            _ => {
                let event = self
                    .state
                    .active_component
                    .borrow()
                    .map_key_event(key_event, self.buffered_key_press.as_ref());

                if event.is_some() {
                    self.buffered_key_press = None;
                } else if let KeyCode::Char(c) = key_event.code {
                    self.buffered_key_press = Some(BufferedKeyPress::new(c));
                }

                event
            }
        };

        if let Some(e) = app_event {
            self.event_bus.send(e).await;
        }
    }
    /// Handles the [`AppEvent::ExportRecord`] event emitted by the [`EventBus`].
    async fn on_export_record(&mut self, record: Record) {
        tracing::debug!("exporting selected record");

        let notification = match self.exporter.export_record(&record) {
            Ok(_) => Notification::success("Record Exported Successfully"),
            Err(e) => {
                tracing::error!("export record failure: {}", e);
                Notification::failure("Record Export Failed")
            }
        };

        self.event_bus
            .send(AppEvent::DisplayNotification(notification))
            .await;
    }
    /// Handles the [`AppEvent::PauseProcessing`] event emitted by the [`EventBus`].
    async fn on_pause_processing(&mut self) {
        if self.state.consumer_mode.get() == ConsumerMode::Processing {
            tracing::debug!("pausing Kafka consumer");

            self.state.consumer_mode.set(ConsumerMode::Paused);

            let notification = match self.consumer.pause() {
                Ok(_) => Notification::success("Consumer Paused Successfully"),
                Err(e) => {
                    tracing::error!("failed to pause consumer: {}", e);
                    Notification::failure("Pause Consumer Failed")
                }
            };

            self.event_bus
                .send(AppEvent::DisplayNotification(notification))
                .await;
        }
    }
    /// Handles the [`AppEvent::ResumeProcessing`] event emitted by the [`EventBus`].
    async fn on_resume_processing(&mut self) {
        if self.state.consumer_mode.get() == ConsumerMode::Paused {
            tracing::debug!("resuming Kafka consumer");

            self.state.consumer_mode.set(ConsumerMode::Processing);

            let notification = match self.consumer.resume() {
                Ok(_) => Notification::success("Consumer Resumed Successfully"),
                Err(e) => {
                    tracing::error!("failed to resume consumer: {}", e);
                    Notification::failure("Resume Consumer Failed")
                }
            };

            self.event_bus
                .send(AppEvent::DisplayNotification(notification))
                .await;
        }
    }
    /// Handles the [`AppEvent::DisplayNotification`] event emitted by the [`EventBus`].
    fn on_display_notification(&mut self, notification: Notification) {
        self.state.notification = Some(notification);
    }
    /// Handles the [`AppEvent::Quit`] event emitted by the [`EventBus`].
    fn on_quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
    /// Handles the [`AppEvent::SelectComponent`] event emitted by the [`EventBus`].
    fn on_select_component(&mut self, idx: usize) {
        tracing::debug!("select component: {:?}", idx);

        if let Some(component) = self.components.get(idx) {
            self.state.active_component = Rc::clone(component);
        }
    }
}

/// Asynchronous task that starts the Kafka consumer.
struct StartConsumerTask {
    /// Kafka consumer to start.
    consumer: Arc<Consumer>,
    /// Topic to consume records from.
    topic: String,
    /// [`Vec`] of partitions that should be assigned to the Kafka consumer.
    partitions: Vec<i32>,
    /// [`Vec`] of partition and offset pairs that the Kafka consumer should seek to before starting to
    /// consume records.
    seek_to: Vec<PartitionOffset>,
    /// JSONPath filter to apply to the consumed records.
    filter: Option<String>,
    /// [`EventBus`] on which the results of the startup task will be published.
    event_bus: Arc<EventBus>,
}

impl StartConsumerTask {
    /// Runs the task. Starts the consumer and send the appropriate event based on the result to
    /// the [`EventBus`].
    async fn run(self) {
        match self
            .consumer
            .start(self.topic, self.partitions, self.seek_to, self.filter)
        {
            Ok(_) => self.event_bus.send(AppEvent::ConsumerStarted).await,
            Err(e) => self.event_bus.send(AppEvent::ConsumerStartFailure(e)).await,
        };
    }
}

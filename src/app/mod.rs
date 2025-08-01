pub mod config;
pub mod export;
pub mod input;

use crate::{
    app::{config::Config, export::Exporter, input::InputDispatcher},
    event::{AppEvent, Event, EventBus},
    kafka::{Consumer, ConsumerMode, Record},
};

use anyhow::Context;
use bounded_vec_deque::BoundedVecDeque;
use chrono::{DateTime, Duration, Utc};
use ratatui::{
    widgets::{ScrollbarState, TableState},
    DefaultTerminal,
};
use std::{cell::Cell, collections::HashMap, rc::Rc, sync::Arc};
use tokio::sync::mpsc::Receiver;

/// Maximum number of notifications that will be stored in memory to be displayed in the
/// notification history table. Once the limit is reacahed, as new notifications are added the
/// older ones are removed.
const NOTIFICATION_HISTORY_MAX_LEN: usize = 512;

/// Number of notification seconds after a [`Notification`] is created that it should not be
/// eligible to visible to the user any longer.
const NOTIFICATION_EXPIRATION_SECS: i64 = 3;

/// Enumeration of the widgets that the user can select.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SelectableWidget {
    /// Table that lists the records that have been consumed from the Kafka topic.
    RecordList,
    /// Text panel that outputs the value of the currently selected record.
    RecordValue,
    /// Table that lists the notifications that have been presented to the user for the duration of
    /// the application execution.
    NotificationHistoryList,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NotificationStatus {
    /// Notification of a successful action.
    Success,
    /// Notification is a warning. Usually something didn't work but a default was used instead or
    /// some other default action was taken.
    Warn,
    /// Notification of a failed action.
    Failure,
}

/// A [`Notification`] is a message that is presented to the user with the results of either an
/// action that is taken by them or by the application itself, e.g. the result of exporting a
/// record to a file.
#[derive(Debug)]
pub struct Notification {
    pub status: NotificationStatus,
    /// Summary text for the notification. The summary is displayed in the header for a short
    /// period of time.
    pub summary: String,
    /// Detailed text for the notification. The full message details are displayed to the user on
    /// the notificaiton history screen.
    pub details: String,
    /// Timestamp when the notification was created by the application.
    pub created: DateTime<Utc>,
}

impl Notification {
    /// Creates a new notification for the user with the specified data.
    pub fn new(
        status: NotificationStatus,
        summary: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self {
            status,
            summary: summary.into(),
            details: details.into(),
            created: Utc::now(),
        }
    }
    /// Creates a new success notification for the user with the specified data.
    pub fn success(summary: impl Into<String>, details: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Success, summary, details)
    }
    /// Creates a new warn notification for the user with the specified data.
    pub fn warn(summary: impl Into<String>, details: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Warn, summary, details)
    }
    /// Creates a new failure notification for the user with the specified data.
    pub fn failure(summary: impl Into<String>, details: impl Into<String>) -> Self {
        Self::new(NotificationStatus::Failure, summary, details)
    }
    /// Determines if the notification has expired and should no longer be visible.
    pub fn is_expired(&self) -> bool {
        (self.created + Duration::seconds(NOTIFICATION_EXPIRATION_SECS)) < Utc::now()
    }
}

/// Manages the global application state.
#[derive(Debug)]
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
    /// Holds the [`Screen`] the user is currently viewing.
    pub screen: Screen,
    /// Currently selected [`Record`] that is being viewed.
    pub selected: Option<Record>,
    /// Collection of the [`Record`]s that have been consumed from the Kafka topic.
    pub records: BoundedVecDeque<Record>,
    /// [`TableState`] for the table that the records consumed from the Kafka topic are rendered
    /// into.
    pub record_list_state: TableState,
    /// [`ScrollbarState`] for the table that the records consumed from the Kafka topic are
    /// rendered into.
    pub record_list_scroll_state: ScrollbarState,
    /// Contains the current scolling state for the record value text.
    pub record_value_scroll: (u16, u16),
    /// Total number of records consumed from the Kafka topic since the application was launched.
    pub total_consumed: u32,
    /// Stores the current [`ConsumerMode`] of the application which controls whether or not
    /// records are currently being consumed from the topic.
    pub consumer_mode: ConsumerMode,
    /// Stores the widget that the user currently has selected.
    pub selected_widget: Rc<Cell<SelectableWidget>>,
    /// Total number of notifications displayed to the user since the application was launched.
    pub total_notifications: u32,
    /// Stores the history of [`Notification`]s displayed to the user.
    pub notification_history: BoundedVecDeque<Notification>,
    /// [`TableState`] for the table that notifications from the history list are rendered into.
    pub notification_history_state: TableState,
    /// [`ScrollbarState`] for the table that notifications from the history list are rendered
    /// into.
    pub notification_history_scroll_state: ScrollbarState,
}

impl State {
    /// Creates a new [`State`] with the specified dependencies.
    pub fn new(max_records: usize) -> Self {
        Self {
            running: true,
            screen: Screen::Initialize,
            selected: None,
            records: BoundedVecDeque::new(max_records),
            record_list_state: TableState::default(),
            record_list_scroll_state: ScrollbarState::default(),
            record_value_scroll: (0, 0),
            total_consumed: 0,
            consumer_mode: ConsumerMode::Processing,
            selected_widget: Rc::new(Cell::new(SelectableWidget::RecordList)),
            total_notifications: 0,
            notification_history: BoundedVecDeque::new(NOTIFICATION_HISTORY_MAX_LEN),
            notification_history_state: TableState::default(),
            notification_history_scroll_state: ScrollbarState::default(),
        }
    }
    /// Moves the record value scroll state to the top.
    fn scroll_record_value_top(&mut self) {
        self.record_value_scroll.0 = 0;
    }
    /// Moves the record value scroll state down by `n` number of lines.
    fn scroll_record_value_down(&mut self, n: u16) {
        self.record_value_scroll.0 += n;
    }
    /// Moves the record value scroll state up by `n` number of lines.
    fn scroll_record_value_up(&mut self, n: u16) {
        if self.record_value_scroll.0 >= n {
            self.record_value_scroll.0 -= n;
        }
    }
    /// Pushes a new [`Record`] onto the current list when a new one is received from the Kafka
    /// consumer.
    fn push_record(&mut self, record: Record) {
        self.records.push_front(record);
        self.total_consumed += 1;

        if let Some(i) = self.record_list_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.record_list_state.select(Some(new_idx));
            self.record_list_scroll_state = self.record_list_scroll_state.position(new_idx);
        }
    }
    /// Updates the state such so the first [`Record`] in the list will be selected.
    fn select_first_record(&mut self) {
        if self.records.is_empty() {
            return;
        }

        self.record_list_state.select(Some(0));
        self.record_list_scroll_state = self.record_list_scroll_state.position(0);

        self.selected = self.records.front().cloned();

        self.record_value_scroll = (0, 0);
    }
    /// Updates the state such so the previous [`Record`] in the list will be selected.
    fn select_prev_record(&mut self) {
        if self.records.is_empty() {
            return;
        }

        if let Some(i) = self.record_list_state.selected().as_ref() {
            if *i == 0 {
                return;
            }

            let prev = *i - 1;

            self.record_list_state.select(Some(prev));
            self.record_list_scroll_state = self.record_list_scroll_state.position(prev);

            self.selected = self.records.get(prev).cloned();
        } else {
            self.record_list_state.select(Some(0));
            self.record_list_scroll_state = self.record_list_scroll_state.position(0);

            self.selected = self.records.front().cloned();
        }

        self.record_value_scroll = (0, 0);
    }
    /// Updates the state such so the next [`Record`] in the list will be selected.
    fn select_next_record(&mut self) {
        if self.records.is_empty() {
            return;
        }

        if let Some(i) = self.record_list_state.selected().as_ref() {
            if *i == self.records.len() - 1 {
                return;
            }

            let next = *i + 1;

            self.record_list_state.select(Some(next));
            self.record_list_scroll_state = self.record_list_scroll_state.position(next);

            self.selected = self.records.get(next).cloned();
        } else {
            self.record_list_state.select(Some(0));
            self.record_list_scroll_state = self.record_list_scroll_state.position(0);

            self.selected = self.records.front().cloned();
        }

        self.record_value_scroll = (0, 0);
    }
    /// Updates the state such so the last [`Record`] in the list will be selected.
    fn select_last_record(&mut self) {
        if self.records.is_empty() {
            return;
        }

        let last_idx = self.records.len() - 1;

        self.record_list_state.select(Some(last_idx));
        self.record_list_scroll_state = self.record_list_scroll_state.position(last_idx);

        self.selected = self.records.back().cloned();

        self.record_value_scroll = (0, 0);
    }
    /// Cycles the focus to the next available widget based on the currently selected widget.
    fn cycle_next_widget(&mut self) {
        let next_widget = match self.screen {
            Screen::Initialize => None,
            Screen::ConsumeTopic => match self.selected_widget.get() {
                SelectableWidget::RecordList if self.selected.is_some() => {
                    Some(SelectableWidget::RecordValue)
                }
                _ => Some(SelectableWidget::RecordList),
            },
            Screen::NotificationHistory => Some(SelectableWidget::NotificationHistoryList),
        };

        if let Some(widget) = next_widget {
            self.selected_widget.set(widget);
        }
    }
    /// Moves the notification history scroll state to the top.
    fn scroll_notification_history_list_top(&mut self) {
        self.notification_history_state.select(Some(0));
        self.notification_history_scroll_state = self.notification_history_scroll_state.position(0);
    }
    /// Moves the record value scroll state up by `n` number of lines.
    fn scroll_notification_history_list_up(&mut self, n: usize) {
        if self.notification_history.is_empty() {
            return;
        }

        if let Some(i) = self.notification_history_state.selected().as_ref() {
            if *i < n {
                return;
            }

            let prev = *i - n;

            self.notification_history_state.select(Some(prev));
            self.notification_history_scroll_state =
                self.notification_history_scroll_state.position(prev);
        } else {
            self.notification_history_state.select(Some(0));
            self.notification_history_scroll_state =
                self.notification_history_scroll_state.position(0);
        }
    }
    /// Moves the notification history scroll state down by `n` number of lines.
    fn scroll_notification_history_list_down(&mut self, n: usize) {
        if self.notification_history.is_empty() {
            return;
        }

        if let Some(i) = self.notification_history_state.selected().as_ref() {
            if *i + n > self.records.len() - 1 {
                return;
            }

            let next = *i + n;

            self.notification_history_state.select(Some(next));
            self.notification_history_scroll_state =
                self.notification_history_scroll_state.position(next);
        } else {
            self.notification_history_state.select(Some(0));
            self.notification_history_scroll_state =
                self.notification_history_scroll_state.position(0);
        }
    }
    /// Moves the notification history scroll state to the bottom.
    fn scroll_notification_history_list_bottom(&mut self) {
        let bottom = self.notification_history.len() - 1;

        self.notification_history_state.select(Some(bottom));
        self.notification_history_scroll_state =
            self.notification_history_scroll_state.position(bottom);
    }
    /// Pushes a new [`Notification`] onto the current list when a new one is generated.
    fn push_notification(&mut self, notification: Notification) {
        self.notification_history.push_front(notification);
        self.total_notifications += 1;

        if let Some(i) = self.notification_history_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.notification_history_state.select(Some(new_idx));
            self.notification_history_scroll_state =
                self.notification_history_scroll_state.position(new_idx);
        }
    }
}

/// Enumeration of the various screens that the application can display to the end user.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Screen {
    /// Active when the application is starting up and connecting to the Kafka brokers.
    Initialize,
    /// Active when the user is viewing records being consumed from the Kafka topic.
    ConsumeTopic,
    /// Active when the user is viewing the notification history.
    NotificationHistory,
}

/// Drives the execution of the application and coordinates the various subsystems.
pub struct App {
    /// Configuration for the application.
    pub config: Config,
    /// Contains the current state of the application.
    pub state: State, // TODO: probably need to split up state to be per screen
    /// Maps input from the user to application events published on the [`EventBus`].
    input_dispatcher: InputDispatcher,
    /// Channel receiver that is used to receive application events that are sent by the
    /// [`EventBus`].
    event_rx: Receiver<Event>,
    /// Channel receiver that is used to receive records from the Kafka consumer.
    consumer_rx: Receiver<Record>,
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
    /// Creates a new [`App`] configured by the specified [`Config`].
    pub fn new(config: Config) -> anyhow::Result<Self> {
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

        let max_records = config.max_records;

        let state = State::new(max_records);

        let input_dispatcher =
            InputDispatcher::new(Arc::clone(&event_bus), Rc::clone(&state.selected_widget));

        let exporter = Exporter::new(config.export_directory.clone());

        Ok(Self {
            config,
            input_dispatcher,
            state,
            event_rx,
            consumer_rx,
            event_bus,
            consumer: Arc::new(consumer),
            exporter,
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
                            self.input_dispatcher.on_key_event(key_event).await
                        }
                    }
                    Event::App(app_event) => match app_event {
                        AppEvent::Quit => self.on_quit(),
                        AppEvent::ConsumerStarted => self.on_consumer_started(),
                        AppEvent::ConsumerStartFailure(e) => {
                            anyhow::bail!("failed to start Kafka consumer: {}", e)
                        }
                        AppEvent::SelectFirstRecord => self.on_select_first_record(),
                        AppEvent::SelectPrevRecord => self.on_select_prev_record(),
                        AppEvent::SelectNextRecord => self.on_select_next_record(),
                        AppEvent::SelectLastRecord => self.on_select_last_record(),
                        AppEvent::ExportSelectedRecord => self.on_export_selected_record(),
                        AppEvent::PauseProcessing => self.on_pause_processing(),
                        AppEvent::ResumeProcessing => self.on_resume_processing(),
                        AppEvent::SelectNextWidget => self.on_select_next_widget(),
                        AppEvent::ScrollRecordValueTop => self.on_scroll_record_value_top(),
                        AppEvent::ScrollRecordValueDown => self.on_scroll_record_value_down(),
                        AppEvent::ScrollRecordValueUp => self.on_scroll_record_value_up(),
                        AppEvent::SelectScreen(screen) => self.on_select_screen(screen),
                        AppEvent::ScrollNotificationHistoryTop => {
                            self.on_scroll_notification_history_top()
                        }
                        AppEvent::ScrollNotificationHistoryUp => {
                            self.on_scroll_notification_history_up()
                        }
                        AppEvent::ScrollNotificationHistoryDown => {
                            self.on_scroll_notification_history_down()
                        }
                        AppEvent::ScrollNotificationHistoryBottom => {
                            self.on_scroll_notification_history_bottom()
                        }
                    },
                }
            }

            if let Ok(record) = self.consumer_rx.try_recv() {
                self.on_record_received(record);
            }
        }

        tracing::debug!("application stopped");

        Ok(())
    }
    /// Starts the consumer asynchronously. The result of the consumer startup is sent back to the
    /// application through the [`EventBus`].
    fn start_consumer_async(&self) {
        // TODO: clean this up
        let seek_to: Vec<(i32, i64)> = self
            .config
            .seek_to
            .as_ref()
            .map(|csv| {
                csv.split(",")
                    .map(|pair| {
                        let mut pair_itr = pair.split(":");

                        let p = pair_itr
                            .next()
                            .map(|p| p.parse::<i32>().expect("valid partition value"))
                            .expect("partition value set");

                        let o = pair_itr
                            .next()
                            .map(|o| o.parse::<i64>().expect("valid offset value"))
                            .expect("offset value set");

                        (p, o)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let start_consumer_task = StartConsumerTask {
            consumer: Arc::clone(&self.consumer),
            topic: self.config.topic.clone(),
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
        self.state.screen = Screen::ConsumeTopic;
    }
    /// Invoked when a new [`Record`] is received on the consumer channel.
    fn on_record_received(&mut self, record: Record) {
        tracing::debug!("Kafka record received");
        self.state.push_record(record);
    }
    /// Handles the [`AppEvent::SelectFirstRecord`] event emitted by the [`EventBus`].
    fn on_select_first_record(&mut self) {
        tracing::debug!("select first record");
        self.state.select_first_record();
    }
    /// Handles the [`AppEvent::SelectPrevRecord`] event emitted by the [`EventBus`].
    fn on_select_prev_record(&mut self) {
        tracing::debug!("select previous record");
        self.state.select_prev_record();
    }
    /// Handles the [`AppEvent::SelectNextRecord`] event emitted by the [`EventBus`].
    fn on_select_next_record(&mut self) {
        tracing::debug!("select next record");
        self.state.select_next_record();
    }
    /// Handles the [`AppEvent::SelectLastRecord`] event emitted by the [`EventBus`].
    fn on_select_last_record(&mut self) {
        tracing::debug!("select last record");
        self.state.select_last_record();
    }
    /// Handles the [`AppEvent::ExportSelectedRecord`] event emitted by the [`EventBus`].
    fn on_export_selected_record(&mut self) {
        if let Some(record) = self.state.selected.clone() {
            tracing::debug!("exporting selected record");

            // TODO: pass by ref instead?
            let notification = match self.exporter.export_record(record) {
                Ok(file_path) => Notification::success(
                    "Record exported successfully",
                    format!("Record succesfully exported to file at {}", file_path),
                ),
                Err(e) => {
                    tracing::error!("export record failure: {}", e);
                    Notification::failure(
                        "Record export failed",
                        format!("Failed to export the selected record: {}", e),
                    )
                }
            };

            self.state.push_notification(notification);
        }
    }
    /// Handles the [`AppEvent::PauseProcessing`] event emitted by the [`EventBus`].
    fn on_pause_processing(&mut self) {
        if self.state.consumer_mode == ConsumerMode::Processing {
            tracing::debug!("pausing Kafka consumer");

            self.state.consumer_mode = ConsumerMode::Paused;

            let notification = match self.consumer.pause() {
                Ok(_) => Notification::success(
                    "Consumer paused successfully",
                    "Kafka consumer was successfully paused at the request of the user",
                ),
                Err(e) => {
                    tracing::error!("failed to pause consumer: {}", e);
                    Notification::failure(
                        "Pause consumer failed",
                        format!("Failed to pause the Kafka consumer: {}", e),
                    )
                }
            };

            self.state.push_notification(notification);
        }
    }
    /// Handles the [`AppEvent::ResumeProcessing`] event emitted by the [`EventBus`].
    fn on_resume_processing(&mut self) {
        if self.state.consumer_mode == ConsumerMode::Paused {
            tracing::debug!("resuming Kafka consumer");

            self.state.consumer_mode = ConsumerMode::Processing;

            let notification = match self.consumer.resume() {
                Ok(_) => Notification::success(
                    "Consumer resumed successfully",
                    "Kafka consumer was successfully resumed at the request of the user",
                ),
                Err(e) => {
                    tracing::error!("failed to resume consumer: {}", e);
                    Notification::failure(
                        "Resume consumer failed",
                        format!("Failed to resume the Kafka consumer: {}", e),
                    )
                }
            };

            self.state.push_notification(notification);
        }
    }
    /// Handles the [`AppEvent::SelectNextWidget`] event emitted by the [`EventBus`].
    fn on_select_next_widget(&mut self) {
        tracing::debug!("cycling focus to next widget");
        self.state.cycle_next_widget();
    }
    /// Handles the [`AppEvent::ScrollRecordValueTop`] event emitted by the [`EventBus`].
    fn on_scroll_record_value_top(&mut self) {
        tracing::debug!("scroll record value to top");
        self.state.scroll_record_value_top();
    }
    /// Handles the [`AppEvent::ScrollRecordValueDown`] event emitted by the [`EventBus`].
    fn on_scroll_record_value_down(&mut self) {
        tracing::debug!("scroll record value down");
        self.state
            .scroll_record_value_down(self.config.scroll_factor);
    }
    /// Handles the [`AppEvent::ScrollRecordValueUp`] event emitted by the [`EventBus`].
    fn on_scroll_record_value_up(&mut self) {
        tracing::debug!("scroll record value up");
        self.state.scroll_record_value_up(self.config.scroll_factor);
    }
    /// Handles the [`AppEvent::Quit`] event emitted by the [`EventBus`].
    fn on_quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
    /// Handles the [`AppEvent::SelectScreen`] event emitted by the [`EventBus`].
    fn on_select_screen(&mut self, screen: Screen) {
        tracing::debug!("select screen: {:?}", screen);

        self.state.screen = screen;

        match self.state.screen {
            Screen::Initialize => {}
            Screen::ConsumeTopic => self.state.selected_widget.set(SelectableWidget::RecordList),
            Screen::NotificationHistory => self
                .state
                .selected_widget
                .set(SelectableWidget::NotificationHistoryList),
        }
    }
    /// Handles the [`AppEvent::ScrollNotificationHistoryTop`] event emitted by the [`EventBus`].
    fn on_scroll_notification_history_top(&mut self) {
        tracing::debug!("scroll notification history list to top");
        self.state.scroll_notification_history_list_top();
    }
    /// Handles the [`AppEvent::ScrollNotificationHistoryUp`] event emitted by the [`EventBus`].
    fn on_scroll_notification_history_up(&mut self) {
        tracing::debug!("scroll notification history list up");
        self.state.scroll_notification_history_list_up(1);
    }
    /// Handles the [`AppEvent::ScrollNotificationHistoryUp`] event emitted by the [`EventBus`].
    fn on_scroll_notification_history_down(&mut self) {
        tracing::debug!("scroll notification history list down");
        self.state.scroll_notification_history_list_down(1);
    }
    /// Handles the [`AppEvent::ScrollNotificationHistoryBottom`] event emitted by the [`EventBus`].
    fn on_scroll_notification_history_bottom(&mut self) {
        tracing::debug!("scroll notification history list to bottom");
        self.state.scroll_notification_history_list_bottom();
    }
}

/// Asynchronous task that starts the Kafka consumer.
struct StartConsumerTask {
    /// Kafka consumer to start.
    consumer: Arc<Consumer>,
    /// Topic to consume records from.
    topic: String,
    /// Vec of partition and offset pairs that the Kafka consumer will seek to before starting to
    /// consumer records.
    seek_to: Vec<(i32, i64)>,
    /// Any filter to apply to the consumed records.
    filter: Option<String>,
    /// [`EventBus`] on which the results of the startup will be sent.
    event_bus: Arc<EventBus>,
}

impl StartConsumerTask {
    /// Runs the task. Starts the consumer and send the appropriate event based on the result to
    /// the [`EventBus`].
    async fn run(self) {
        match self.consumer.start(self.topic, self.seek_to, self.filter) {
            Ok(()) => self.event_bus.send(AppEvent::ConsumerStarted).await,
            Err(e) => self.event_bus.send(AppEvent::ConsumerStartFailure(e)).await,
        };
    }
}

pub mod config;

use crate::{
    app::config::Config,
    event::{AppEvent, Event, EventBus},
    kafka::{Consumer, Record},
};

use anyhow::Context;
use bounded_vec_deque::BoundedVecDeque;
use chrono::{DateTime, Utc};
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent},
    widgets::{ScrollbarState, TableState},
    DefaultTerminal,
};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::Receiver;

/// Default prefix used for the name of the exported file when no partition key is set.
const DEFAULT_EXPORT_FILE_PREFIX: &str = "record-export";

/// Enumerates the different states that the Kafka consumer can be in.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ConsumerMode {
    /// Consumer is paused and not processing records from the topic.
    Paused,
    /// Consumer is processing records from the topic.
    Processing,
}

/// Enumeration of the widgets that the user can select.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SelectableWidget {
    /// Table that lists the records that have been consumed from the Kafka topic.
    RecordList,
    /// Text panel that outputs the value of the currently selected record.
    RecordValue,
}

/// Manages the global application state.
#[derive(Debug)]
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
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
    pub record_list_value_scroll: (u16, u16),
    /// Total number of records consumed from the Kafka topic since the application was launched.
    pub total_consumed: u32,
    /// Stores the current [`ConsumerMode`] of the application which controls whether or not
    /// records are currently being consumed from the topic.
    pub consumer_mode: ConsumerMode,
    /// Stores the widget that the user currently has selected.
    pub selected_widget: SelectableWidget,
}

impl State {
    /// Creates a new default [`State`].
    pub fn new(max_records: usize) -> Self {
        Self {
            running: true,
            selected: None,
            records: BoundedVecDeque::new(max_records),
            record_list_state: TableState::default(),
            record_list_scroll_state: ScrollbarState::default(),
            record_list_value_scroll: (0, 0),
            total_consumed: 0,
            consumer_mode: ConsumerMode::Processing,
            selected_widget: SelectableWidget::RecordList,
        }
    }
}

/// Enumeration of the various screens that the application can display to the end user.
#[derive(Debug, PartialEq)]
pub enum Screen {
    /// Active when the application is starting up and connecting to the Kafka brokers.
    Startup,
    /// Active when the user is viewing messages being consumed from a Kafka topic.
    ConsumeTopic,
}

/// View of a [`Record`] that is saved to a file in JSON format when the user requests that the
/// selected record be exported. This allows for better handling of the value field which would
/// just be rendered as a JSON encoded string otherwise.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportedRecord {
    /// Name of the topic that the record was consumed from.
    topic: String,
    /// Partition number the record was assigned in the topic.
    partition: i32,
    /// Offset of the record in the topic.
    offset: i64,
    /// Partition key for the record if one was set.
    key: Option<String>,
    /// Contains any headers from the Kafka record.
    headers: HashMap<String, String>,
    /// Value of the Kafka record.
    value: serde_json::Value,
    /// UTC timestamp represeting when the event was created.
    timestamp: DateTime<Utc>,
}

impl From<Record> for ExportedRecord {
    /// Converts the given [`Record`] to an [`ExportedRecord`].
    fn from(value: Record) -> Self {
        let json_value = if value.value.is_empty() {
            serde_json::from_str("{}").expect("valid JSON value")
        } else {
            match serde_json::from_str(&value.value) {
                Ok(json_value) => json_value,
                Err(e) => {
                    tracing::error!("invalid JSON value: {}", e);
                    serde_json::from_str("{}").expect("valid json value")
                }
            }
        };

        Self {
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            key: value.key,
            headers: value.headers,
            value: json_value,
            timestamp: value.timestamp,
        }
    }
}

/// Drives the execution of the application and coordinates the various subsystems.
pub struct App {
    /// Configuration for the application.
    pub config: Config,
    /// Contains the current state of the application.
    pub state: State,
    /// Channel receiver that is used to receive application events that are sent by the
    /// [`EventBus`].
    event_rx: Receiver<Event>,
    /// Channel receiver that is used to receive records from the Kafka consumer.
    consumer_rx: Receiver<Record>,
    /// Emits events to be handled by the application.
    event_bus: Arc<EventBus>,
    /// Consumer used to read records from a Kafka topic.
    consumer: Arc<Consumer>,
    /// Holds the [`Screen`] the user is currently viewing.
    pub screen: Screen,
}

impl App {
    /// Creates a new [`App`] configured by the specified [`Config`].
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(64);

        let event_bus = Arc::new(EventBus::new(event_tx));

        let (consumer_tx, consumer_rx) = tokio::sync::mpsc::channel(64);

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

        Ok(Self {
            config,
            state: State::new(max_records),
            event_rx,
            consumer_rx,
            event_bus,
            consumer: Arc::new(consumer),
            screen: Screen::Startup,
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

            tokio::select! {
                Some(e) = self.event_rx.recv() => match e {
                    Event::Tick => self.on_tick(),
                    Event::Crossterm(crossterm_event) => {
                        if let crossterm::event::Event::Key(key_event) = crossterm_event {
                            self.on_key_event(key_event).await
                        }
                    }
                    Event::App(app_event) => match app_event {
                        AppEvent::Quit => self.on_quit(),
                        AppEvent::ConsumerStarted => self.on_consumer_started(),
                        AppEvent::ConsumerStartFailure(e) => {
                            anyhow::bail!("failed to start Kafka consumer: {}", e)
                        }
                        AppEvent::SelectPrevRecord => self.on_select_prev_record(),
                        AppEvent::SelectNextRecord => self.on_select_next_record(),
                        AppEvent::ExportSelectedRecord => self.on_export_selected_record(),
                        AppEvent::PauseProcessing => self.on_pause_processing(),
                        AppEvent::ResumeProcessing => self.on_resume_processing(),
                        AppEvent::SelectNextWidget => self.on_select_next_widget(),
                        AppEvent::ScrollRecordValueDown => self.on_scroll_record_value_down(),
                        AppEvent::ScrollRecordValueUp => self.on_scroll_record_value_up(),
                    },
                },
                Some(r) = self.consumer_rx.recv() => self.on_record_received(r),
            }
        }

        tracing::debug!("application stopped");

        Ok(())
    }
    /// Starts the consumer asynchronously. The result of the consumer startup is sent back to the
    /// application through the [`EventBus`].
    fn start_consumer_async(&self) {
        let start_consumer_task = StartConsumerTask {
            consumer: Arc::clone(&self.consumer),
            topic: self.config.topic.clone(),
            filter: self.config.filter.clone(),
            event_bus: Arc::clone(&self.event_bus),
        };

        tokio::spawn(async move {
            start_consumer_task.run().await;
        });
    }
    /// Handles key events emitted by the [`EventBus`].
    async fn on_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Esc => self.event_bus.send(AppEvent::Quit).await,
            KeyCode::Tab => self.event_bus.send(AppEvent::SelectNextWidget).await,
            KeyCode::Char(c) => match c {
                'e' => self.event_bus.send(AppEvent::ExportSelectedRecord).await,
                'j' => match self.state.selected_widget {
                    SelectableWidget::RecordList => {
                        self.event_bus.send(AppEvent::SelectNextRecord).await
                    }
                    SelectableWidget::RecordValue => {
                        self.event_bus.send(AppEvent::ScrollRecordValueDown).await
                    }
                },
                'k' => match self.state.selected_widget {
                    SelectableWidget::RecordList => {
                        self.event_bus.send(AppEvent::SelectPrevRecord).await
                    }
                    SelectableWidget::RecordValue => {
                        self.event_bus.send(AppEvent::ScrollRecordValueUp).await
                    }
                },
                'p' => self.event_bus.send(AppEvent::PauseProcessing).await,
                'r' => self.event_bus.send(AppEvent::ResumeProcessing).await,
                _ => {}
            },
            _ => {}
        }
    }
    /// Handles the consumer started event emitted by the [`EventBus`].
    fn on_consumer_started(&mut self) {
        self.screen = Screen::ConsumeTopic;
    }
    /// Handles the record recieved event emitted by the [`EventBus`].
    fn on_record_received(&mut self, record: Record) {
        tracing::debug!("Kafka record received");
        self.state.records.push_front(record);
        self.state.total_consumed += 1;

        if let Some(i) = self.state.record_list_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.state.record_list_state.select(Some(new_idx));
            self.state.record_list_scroll_state =
                self.state.record_list_scroll_state.position(new_idx);
        }
    }
    /// Handles the select previous record event emitted by the [`EventBus`].
    fn on_select_prev_record(&mut self) {
        tracing::debug!("select previous record");

        if self.state.records.is_empty() {
            return;
        }

        if let Some(i) = self.state.record_list_state.selected().as_ref() {
            if *i == 0 {
                return;
            }

            let prev = *i - 1;

            self.state.record_list_state.select(Some(prev));
            self.state.record_list_scroll_state =
                self.state.record_list_scroll_state.position(prev);

            self.state.selected = self.state.records.get(prev).cloned();
        } else {
            self.state.record_list_state.select(Some(0));
            self.state.record_list_scroll_state = self.state.record_list_scroll_state.position(0);

            self.state.selected = self.state.records.front().cloned();
        }

        self.state.record_list_value_scroll = (0, 0);
    }
    /// Handles the select next record event emitted by the [`EventBus`].
    fn on_select_next_record(&mut self) {
        tracing::debug!("select next record");

        if self.state.records.is_empty() {
            return;
        }

        if let Some(i) = self.state.record_list_state.selected().as_ref() {
            if *i == self.state.records.len() - 1 {
                return;
            }

            let next = *i + 1;

            self.state.record_list_state.select(Some(next));
            self.state.record_list_scroll_state =
                self.state.record_list_scroll_state.position(next);

            self.state.selected = self.state.records.get(next).cloned();
        } else {
            self.state.record_list_state.select(Some(0));
            self.state.record_list_scroll_state = self.state.record_list_scroll_state.position(0);

            self.state.selected = self.state.records.front().cloned();
        }

        self.state.record_list_value_scroll = (0, 0);
    }
    /// Handles the export selected record event emitted by the [`EventBus`].
    fn on_export_selected_record(&self) {
        if let Some(r) = self.state.selected.clone() {
            let exported_record = ExportedRecord::from(r);

            match serde_json::to_string_pretty(&exported_record) {
                Ok(json) => {
                    // TODO: configurable export directory
                    let dir = ".";

                    let name = exported_record
                        .key
                        .as_ref()
                        .map_or(DEFAULT_EXPORT_FILE_PREFIX, |v| v);

                    let file_path = format!(
                        "{}{}{}-{}.json",
                        dir,
                        std::path::MAIN_SEPARATOR,
                        name,
                        Utc::now().timestamp_millis()
                    );

                    if let Err(e) = std::fs::write(file_path, json) {
                        tracing::error!("failed to export record to file: {}", e);
                    }
                }
                Err(e) => tracing::error!("unable to export selected record: {}", e),
            }
        }
    }
    /// Handles the pause record processing event emitted by the [`EventBus`].
    fn on_pause_processing(&mut self) {
        if self.state.consumer_mode == ConsumerMode::Processing {
            self.state.consumer_mode = ConsumerMode::Paused;

            if let Err(e) = self.consumer.pause() {
                tracing::error!("failed to pause consumer: {}", e);
            }
        }
    }
    /// Handles the resume record processing event emitted by the [`EventBus`].
    fn on_resume_processing(&mut self) {
        if self.state.consumer_mode == ConsumerMode::Paused {
            self.state.consumer_mode = ConsumerMode::Processing;

            if let Err(e) = self.consumer.resume() {
                tracing::error!("failed to resume consumer: {}", e);
            }
        }
    }
    /// Handles the select next widget event emitted by the [`EventBus`].
    fn on_select_next_widget(&mut self) {
        let selected = match self.state.selected_widget {
            SelectableWidget::RecordList if self.state.selected.is_some() => {
                SelectableWidget::RecordValue
            }
            _ => SelectableWidget::RecordList,
        };

        self.state.selected_widget = selected;
    }
    /// Handles the scroll record value down event emitted by the [`EventBus`].
    fn on_scroll_record_value_down(&mut self) {
        self.state.record_list_value_scroll.0 += self.config.scroll_factor;
    }
    /// Handles the scroll record value up event emitted by the [`EventBus`].
    fn on_scroll_record_value_up(&mut self) {
        if self.state.record_list_value_scroll.0 >= self.config.scroll_factor {
            self.state.record_list_value_scroll.0 -= self.config.scroll_factor;
        }
    }
    /// Handles the tick event of the terminal.
    fn on_tick(&self) {}
    /// Quits the application.
    fn on_quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
}

/// Asynchronous task that starts the Kafka consumer.
struct StartConsumerTask {
    /// Kafka consumer to start.
    consumer: Arc<Consumer>,
    /// Topic to consume records from.
    topic: String,
    /// Any filter to apply to the consumed records.
    filter: Option<String>,
    /// [`EventBus`] on which the results of the startup will be sent.
    event_bus: Arc<EventBus>,
}

impl StartConsumerTask {
    /// Runs the task. Starts the consumer and send the appropriate event based on the result to
    /// the [`EventBus`].
    async fn run(self) {
        match self.consumer.start(self.topic, self.filter) {
            Ok(()) => self.event_bus.send(AppEvent::ConsumerStarted).await,
            Err(e) => self.event_bus.send(AppEvent::ConsumerStartFailure(e)).await,
        };
    }
}

use crate::{
    event::{AppEvent, Event, EventBus},
    kafka::{Consumer, DebugMode, Record},
};

use anyhow::Context;
use bounded_vec_deque::BoundedVecDeque;
use chrono::{DateTime, Utc};
use crossterm::event::MouseEvent;
use derive_builder::Builder;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent},
    widgets::TableState,
    DefaultTerminal,
};
use serde::Serialize;
use std::{collections::HashMap, fs::File, io::BufReader, sync::Arc};
use tokio::sync::Mutex;

/// Default group id for the Kafka consumer.
pub const DEFAULT_CONSUMER_GROUP_ID: &str = "kaftui-consumer";

/// Default maximum number of records consumed from the Kafka toic to hold in memory at any given
/// time.
pub const DEFAULT_MAX_RECORDS: usize = 256;

/// Default prefix used for the name of the exported file when no partition key is set.
const DEFAULT_EXPORT_FILE_PREFIX: &str = "record-export";

/// Manages the global appliation state.
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
    /// Total number of records consumed from the Kafka topic since the application was launched.
    pub total_consumed: u32,
    /// Stores the current [`DebugMode`] of the application which controls how the records from the
    /// Kafka topic are consumed.
    pub debug_mode: DebugMode,
}

impl State {
    /// Creates a new default [`State`].
    pub fn with_max_records(max_records: usize) -> Self {
        Self {
            running: true,
            selected: None,
            records: BoundedVecDeque::new(max_records),
            record_list_state: TableState::new(),
            total_consumed: 0,
            debug_mode: DebugMode::Disable,
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
pub struct Config {
    /// Kafka bootstrap servers host value that the application will connect to.
    bootstrap_servers: String,
    /// Name of the Kafka topic to consume messages from.
    topic: String,
    /// Id of the consumer group that the application will use when consuming messages from the Kafka topic.
    group_id: String,
    /// Path to a properties file containing the configuration properties that will be
    /// applied to the Kafka consumer.
    consumer_properties_file: Option<String>,
    /// JSONPath filter that is applied to a [`Record`]. Can be used to filter out any messages
    /// from the Kafka topic that the end user may not be interested in. A message will only be
    /// presented to the user if it matches the filter.
    filter: Option<String>,
    /// Maximum nunber of [`Records`] that should be held in memory at any given time after being
    /// consumed from the Kafka topic.
    max_records: usize,
}

impl Config {
    /// Creates a new default [`ConfigBuilder`].
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
    /// Returns the configured Kafka topic name.
    pub fn topic(&self) -> &String {
        &self.topic
    }
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
    partition_key: Option<String>,
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
            partition_key: value.partition_key,
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
    /// Emits events to be handled by the application.
    event_bus: Arc<Mutex<EventBus>>,
    /// Consumer used to read records from a Kafka topic.
    consumer: Consumer,
    /// Holds the [`Screen`] the user is currently viewing.
    pub screen: Screen,
}

impl App {
    /// Creates a new [`App`] configured by the specified [`Config`].
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let event_bus = Arc::new(Mutex::new(EventBus::new()));

        let mut consumer_config = HashMap::new();

        if let Some(path) = &config.consumer_properties_file {
            let file = File::open(path).context("open properties file")?;
            let props =
                java_properties::read(BufReader::new(file)).context("read properties file")?;

            consumer_config.extend(props);
        }

        consumer_config.insert(
            String::from("bootstrap.servers"),
            config.bootstrap_servers.clone(),
        );
        consumer_config.insert(String::from("group.id"), config.group_id.clone());

        let consumer =
            Consumer::new(consumer_config, Arc::clone(&event_bus)).context("create consumer")?;

        let max_records = config.max_records;

        Ok(Self {
            config,
            state: State::with_max_records(max_records),
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
            .context("start Kafka consumer")?;

        while self.state.running {
            terminal.draw(|frame| self.draw(frame))?;

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
                    AppEvent::SelectPrevRecord => self.on_select_prev_record(),
                    AppEvent::SelectNextRecord => self.on_select_next_record(),
                    AppEvent::ExportSelectedRecord => self.on_export_selected_record(),
                    AppEvent::ToggleDebug => self.on_toggle_debug(),
                    AppEvent::PauseRecords => self.on_pause_records(),
                    AppEvent::StepRecord => self.on_step_record(),
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
            KeyCode::Char(c) => match c {
                'd' => self.event_bus.lock().await.send(AppEvent::ToggleDebug),
                'e' => self
                    .event_bus
                    .lock()
                    .await
                    .send(AppEvent::ExportSelectedRecord),
                'j' => self.event_bus.lock().await.send(AppEvent::SelectNextRecord),
                'k' => self.event_bus.lock().await.send(AppEvent::SelectPrevRecord),
                'p' => self.event_bus.lock().await.send(AppEvent::PauseRecords),
                's' => self.event_bus.lock().await.send(AppEvent::StepRecord),
                _ => {}
            },
            _ => {}
        }

        Ok(())
    }
    /// Handles the record recieved event emitted by the [`EventBus`].
    fn on_record_received(&mut self, record: Record) {
        tracing::debug!("Kafka record received");
        self.state.records.push_front(record);
        self.state.total_consumed += 1;

        if let Some(i) = self.state.record_list_state.selected().as_mut() {
            self.state.record_list_state.select(Some(*i + 1));
        }

        if self.state.debug_mode == DebugMode::Step {
            self.state.debug_mode = DebugMode::Pause;

            if let Err(e) = self.consumer.pause() {
                tracing::error!("failed to pause consumer: {}", e);
            }
        }
    }
    /// Handles the select previous record event emitted by the [`EventBus`].
    fn on_select_prev_record(&mut self) {
        tracing::debug!("select previous record");

        if self.state.records.is_empty() {
            return;
        }

        if let Some(i) = self.state.record_list_state.selected().as_mut() {
            if *i == 0 {
                return;
            }

            let prev = *i - 1;

            self.state.record_list_state.select(Some(prev));
            self.state.selected = self.state.records.get(prev).cloned();
        } else {
            self.state.record_list_state.select(Some(0));
            self.state.selected = self.state.records.front().cloned();
        }
    }
    /// Handles the select next record event emitted by the [`EventBus`].
    fn on_select_next_record(&mut self) {
        tracing::debug!("select next record");

        if self.state.records.is_empty() {
            return;
        }

        if let Some(i) = self.state.record_list_state.selected().as_mut() {
            if *i == self.state.records.len() - 1 {
                return;
            }

            let next = *i + 1;

            self.state.record_list_state.select(Some(next));
            self.state.selected = self.state.records.get(next).cloned();
        } else {
            self.state.record_list_state.select(Some(0));
            self.state.selected = self.state.records.front().cloned();
        }
    }
    /// Handles the export selected record event emitted by the [`EventBus`].
    fn on_export_selected_record(&self) {
        if let Some(r) = self.state.selected.clone() {
            let exported_record = ExportedRecord::from(r);

            match serde_json::to_string_pretty(&exported_record) {
                Ok(json) => {
                    // TODO: configurable export direcotry
                    let dir = ".";

                    let name = exported_record
                        .partition_key
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
    /// Handles the toggle debug event emitted by the [`EventBus`].
    fn on_toggle_debug(&mut self) {
        self.state.debug_mode = match self.state.debug_mode {
            DebugMode::Disable => {
                if let Err(e) = self.consumer.pause() {
                    tracing::error!("failed to pause consumer: {}", e);
                }

                DebugMode::Pause
            }
            DebugMode::Pause | DebugMode::Step => {
                if let Err(e) = self.consumer.resume() {
                    tracing::error!("failed to resume consumer: {}", e);
                }

                DebugMode::Disable
            }
        };
    }
    /// Handles the pause records event emitted by the [`EventBus`].
    fn on_pause_records(&mut self) {
        if self.state.debug_mode != DebugMode::Disable {
            self.state.debug_mode = DebugMode::Pause;

            if let Err(e) = self.consumer.pause() {
                tracing::error!("failed to pause consumer: {}", e);
            }
        }
    }
    /// Handles the step record event emitted by the [`EventBus`].
    fn on_step_record(&mut self) {
        if self.state.debug_mode == DebugMode::Pause {
            self.state.debug_mode = DebugMode::Step;

            if let Err(e) = self.consumer.resume() {
                tracing::error!("failed to resume consumer: {}", e);
            }
        }
    }
    /// Handles the tick event of the terminal.
    fn tick(&self) {}
    /// Quits the application.
    fn quit(&mut self) {
        tracing::debug!("quit application request received");
        self.state.running = false;
    }
}

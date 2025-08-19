use crate::{app::Notification, kafka::Record, trace::Log};

use futures::{FutureExt, StreamExt};
use ratatui::crossterm::event::Event as CrosstermEvent;
use rdkafka::Statistics;
use tokio::sync::mpsc::Sender;

// TODO: split these events into separate channels?

/// Enumeration of events which can be sent on the [`EventBus`].
#[derive(Debug)]
pub enum Event {
    /// Crossterm events. These events are emitted by the terminal backend.
    Crossterm(CrosstermEvent),
    /// Application events. These are events emitted related to the application domain.
    App(AppEvent),
}

/// Enumeration of events which can be produced by the application.
#[derive(Debug)]
pub enum AppEvent {
    /// Fires when the user requests to quit the application.
    Quit,
    /// Fires when the Kafka consumer was started successfully.
    ConsumerStarted,
    /// Fires when the Kafka consumer startup failed.
    ConsumerStartFailure(anyhow::Error),
    /// Fires when the Kafka consumer receives a new [`Record`].
    RecordReceived(Record),
    /// Fires when the Kafka consumer receives a new [`Record`] but it does not match the
    /// configured JSONPath filter.
    RecordFiltered(Record),
    /// Fires when the Kafka consumer receives updated [`Statistics`] from the librdkafka library.
    StatisticsReceived(Box<Statistics>),
    /// Fires when the user wants to select the first record in the list.
    SelectFirstRecord,
    /// Fires when the user wants to select the previous record in the list.
    SelectPrevRecord,
    /// Fires when the user wants to select the next record in the list.
    SelectNextRecord,
    /// Fires when the user wants to select the last record in the list.
    SelectLastRecord,
    /// Fires when the user wants to export a [`Record`] to a file.
    ExportRecord(Record),
    /// Fires when the user wants to continue processing records.
    ResumeProcessing,
    /// Fires when the user wants to pause record consumption.
    PauseProcessing,
    /// Fires when the user wants to select a different widget.
    SelectNextWidget,
    /// Fires when the user wants to scroll the record value widget to the top.
    ScrollRecordValueTop,
    /// Fires when the user wants to scroll the record value widget down.
    ScrollRecordValueDown,
    /// Fires when the user wants to scroll the record value widget up.
    ScrollRecordValueUp,
    /// Fires when the user selects a [`crate::ui::Component`] to view in the UI.
    SelectComponent(usize),
    /// Fires when a new [`Notification`] should be displayed to the user.
    DisplayNotification(Notification),
    /// Fires when the user wants to scroll to the top of the logs list.
    ScrollLogsTop,
    /// Fires when the user wants to scroll the logs list up.
    ScrollLogsUp,
    /// Fires when the user wants to scroll the logs list down.
    ScrollLogsDown,
    /// Fires when the user wants to scroll to the bottom of the logs list.
    ScrollLogsBottom,
    /// Fires when a [`Log`] is emitted by the application.
    LogEmitted(Log),
}

/// The bus over which [`Event`]s are published and consumed.
#[derive(Debug)]
pub struct EventBus {
    /// Event channel sender.
    sender: Sender<Event>,
}

impl EventBus {
    /// Constructs a new instance of [`EventBus`] and spawns a new thread to handle events.
    pub fn new(sender: Sender<Event>) -> Self {
        Self { sender }
    }
    /// Starts the the background thread that will emit the backend terminal events onto the event
    /// channel..
    pub fn start(&self) {
        let task = EventTask::new(self.sender.clone());

        tokio::spawn(async move { task.run().await });
    }
    /// Publishes an application event to on the bus for processing.
    pub async fn send(&self, app_event: AppEvent) {
        if let Err(e) = self.sender.send(Event::App(app_event)).await {
            tracing::error!("error sending event: {}", e);
        }
    }
}

/// A task which is executed in a background thread that handles reading Crossterm events and emitting tick
/// events on a regular schedule.
struct EventTask {
    /// Event channel sender.
    sender: Sender<Event>,
}

impl EventTask {
    /// Constructs a new instance of [`EventTask`].
    fn new(sender: Sender<Event>) -> Self {
        Self { sender }
    }
    /// Runs the task. The task polls for crossterm events and emits them into the event channel.
    /// The task will exit when the sender for the event channel is closed.
    async fn run(self) {
        let mut reader = crossterm::event::EventStream::new();

        loop {
            let crossterm_event = reader.next().fuse();

            tokio::select! {
                _ = self.sender.closed() => {
                    tracing::warn!("exiting event loop because sender was closed");
                    break;
                }
                Some(Ok(e)) = crossterm_event => {
                    tracing::debug!("dispatching crossterm event: {:?}", e);
                    self.send(Event::Crossterm(e)).await;
                }
            };
        }
    }
    /// Publishes an event to the bus for processing.
    async fn send(&self, event: Event) {
        if let Err(e) = self.sender.send(event).await {
            tracing::error!("error sending event: {}", e);
        }
    }
}

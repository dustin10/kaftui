use crate::{
    app::Notification,
    kafka::{schema::Schema, Record},
    trace::Log,
};

use rdkafka::Statistics;
use tokio::sync::mpsc::UnboundedSender;

/// Enumeration of the positions widgets can be scrolled to or the position of an item that should
/// be selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Position {
    /// Scroll to the top or select the top item.
    Top,
    /// Scroll down one or select the next item.
    Down,
    /// Scroll up one or select the previous item.
    Up,
    /// Scroll to the bottom or select the bottom item.
    Bottom,
}

/// Enumeration of events which can be produced by the application.
#[derive(Debug)]
pub enum Event {
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
    /// Fires when the user wants to select a record in the list.
    SelectRecord(Position),
    /// Fires when the user wants to export a [`Record`] to a file.
    ExportRecord(Record),
    /// Fires when the user wants to continue processing records.
    ResumeProcessing,
    /// Fires when the user wants to pause record consumption.
    PauseProcessing,
    /// Fires when the user wants to select a different widget.
    SelectNextWidget,
    /// Fires when the user wants to scroll the record value widget.
    ScrollRecordValue(Position),
    /// Fires when the user wants to scroll the record headers widget.
    ScrollRecordHeaders(Position),
    /// Fires when the user selects a [`crate::ui::Component`] to view in the UI.
    SelectComponent(usize),
    /// Fires when a new [`Notification`] should be displayed to the user.
    DisplayNotification(Notification),
    /// Fires when the user wants to scroll logs list widget.
    ScrollLogs(Position),
    /// Fires when a [`Log`] is emitted by the application.
    LogEmitted(Log),
    /// Fires when the user wants to select a subjects from the list.
    SelectSubject(Position),
    /// Fires when the user wants to select a subject schema version from the list.
    SelectSchemaVersion(Position),
    /// Fires when the user wants to scroll the schema definition widget.
    ScrollSchemaDefinition(Position),
    /// Fires when the user wants to scroll the schema references widget.
    ScrollSchemaReferences(Position),
    /// Fires when the user wants to export a [`Schema`] to a file.
    ExportSchema(Schema),
}

/// The bus over which [`Event`]s are published.
#[derive(Debug)]
pub struct EventBus {
    /// Underlying [`UnboundedSender`] for the application event channel.
    tx: UnboundedSender<Event>,
}

impl EventBus {
    /// Constructs a new instance of [`EventBus`] and spawns a new thread to handle events.
    pub fn new(tx: UnboundedSender<Event>) -> Self {
        Self { tx }
    }
    /// Publishes an application event to on the bus for processing.
    pub fn send(&self, app_event: Event) {
        if let Err(e) = self.tx.send(app_event) {
            tracing::error!("error sending application event over channel: {}", e);
        }
    }
}

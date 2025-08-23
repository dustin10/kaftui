use crate::{app::Notification, kafka::Record, trace::Log};

use rdkafka::Statistics;
use tokio::sync::mpsc::UnboundedSender;

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

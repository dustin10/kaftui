use crate::{
    app::Notification,
    kafka::{
        admin::{Topic, TopicConfig},
        schema::{Schema, Subject, Version},
        Record,
    },
    trace::Log,
};

use rdkafka::Statistics;
use tokio::sync::mpsc::UnboundedSender;

// TODO: try to come up with a better design for the way key events are handled between the UI
// componeents and the main application so that Event::Void is not needed.

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
    /// Fires when the user wants to export a [`Record`] to a file.
    ExportRecord(Record),
    /// Fires when the user wants to continue processing records.
    ResumeProcessing,
    /// Fires when the user wants to pause record consumption.
    PauseProcessing,
    /// Fires when the user wants to select a different widget.
    SelectNextWidget,
    /// Fires when the user selects a [`crate::ui::Component`] to view in the UI.
    SelectComponent(usize),
    /// Fires when a new [`Notification`] should be displayed to the user.
    DisplayNotification(Notification),
    /// Fires when a [`Log`] is emitted by the application.
    LogEmitted(Log),
    /// Fires when the list of subjects needs to be loaded from the schema registry.
    LoadSubjects,
    /// Fires when the list of subjects has been loaded from the schema registry.
    SubjectsLoaded(Vec<Subject>),
    /// Fires when the latest version of a schema needs to be loaded from the schema registry.
    LoadLatestSchema(Subject),
    /// Fires when the latest version of a schema has been loaded from the schema registry.
    LatestSchemaLoaded(Option<Schema>, Vec<Version>),
    /// Fires when a specific version of a schema needs to be loaded from the schema registry.
    LoadSchemaVersion(Subject, Version),
    /// Fires when a specific version of a schema has been loaded from the schema registry.
    SchemaVersionLoaded(Option<Schema>),
    /// Fires when the user wants to export a [`Schema`] to a file.
    ExportSchema(Schema),
    /// Fires when the list of topics needs to be loaded from the Kafka cluster.
    LoadTopics,
    /// Fires when the list of topics has been loaded from the Kafka cluster.
    TopicsLoaded(Vec<Topic>),
    /// Fires when configuration details for a topic needs to be loaded from the Kafka cluster.
    LoadTopicConfig(Topic),
    /// Fires when a topic configuration has been loaded from the Kafka cluster.
    TopicConfigLoaded(Option<TopicConfig>),
    /// Marker event used to indicate no operation.
    Void,
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

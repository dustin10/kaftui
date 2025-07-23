use futures::{FutureExt, StreamExt};
use ratatui::crossterm::event::Event as CrosstermEvent;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

/// Frequency at which tick events are emitted.
const TICK_FPS: f64 = 30.0;

/// Enumeration of events which can be sent on the [`EventBus`].
#[derive(Debug)]
pub enum Event {
    /// An event that is emitted on a regular schedule. This event is used to run any code which has to run
    /// outside of being a direct response to a user event. e.g. polling exernal systems, updating animations,
    /// or rendering the UI based on a fixed frame rate.
    Tick,
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
    /// Fires when the user wants to select the previous record in the list.
    SelectPrevRecord,
    /// Fires when the user wants to select the next record in the list.
    SelectNextRecord,
    /// Fires when the user wants to export the selected record to a file.
    ExportSelectedRecord,
    /// Fires when the user wants to continue processing records.
    ResumeProcessing,
    /// Fires when the user wants to pause record consumption.
    PauseProcessing,
    /// Fires when the user wants to select a different widget.
    SelectNextWidget,
    /// Fires when the user wants to scroll the record value widget down.
    ScrollRecordValueDown,
    /// Fires when the user wants to scroll the record value widget up.
    ScrollRecordValueUp,
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
    /// Starts the the background thread that will emit the backend terminal events as well as the
    /// tick event.
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
    /// Runs the task. The task emits tick events at a fixed rate and polls for crossterm events
    /// in between. The task will exit when the sender for the event channel is closed.
    async fn run(self) {
        let tick_rate = Duration::from_secs_f64(1.0 / TICK_FPS);
        let mut tick = tokio::time::interval(tick_rate);

        let mut reader = crossterm::event::EventStream::new();

        loop {
            let tick_delay = tick.tick();

            let crossterm_event = reader.next().fuse();

            tokio::select! {
                _ = self.sender.closed() => {
                    tracing::info!("exiting event loop because sender was closed");
                    break;
                }
                _ = tick_delay => {
                    self.send(Event::Tick).await;
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

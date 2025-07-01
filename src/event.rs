use crate::kafka::Record;
use futures::{FutureExt, StreamExt};
use ratatui::crossterm::event::Event as CrosstermEvent;
use std::time::Duration;
use tokio::sync::mpsc;

/// Frequency at which tick events are emitted.
const TICK_FPS: f64 = 30.0;

/// Enumeration of events which can be sent on the [`EventBus`].
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub enum AppEvent {
    /// Fires when the user requests to quit the application.
    Quit,
    /// Fires when a new [`Record`] is received from a Kafka topic.
    RecordReceived(Record),
}

/// The bus over which [`Event`]s are published and consumed.
#[derive(Debug)]
pub struct EventBus {
    /// Event channel sender.
    sender: mpsc::UnboundedSender<Event>,
    /// Event channel receiver.
    receiver: mpsc::UnboundedReceiver<Event>,
}

impl EventBus {
    /// Constructs a new instance of [`EventBus`] and spawns a new thread to handle events.
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts the the background thread that will emit the backend terminal events as well as the
    /// tick event.
    pub fn start(&self) {
        let task = EventTask::new(self.sender.clone());

        tokio::spawn(async { task.run().await });
    }
    /// Receives an event from the sender.
    ///
    /// This function blocks until an event is received.
    ///
    /// # Errors
    ///
    /// This function returns an error if the sender channel is disconnected. This can happen if an
    /// error occurs in the event thread. In practice, this should not happen unless there is a
    /// problem with the underlying terminal.
    pub async fn next(&mut self) -> anyhow::Result<Event> {
        self.receiver
            .recv()
            .await
            .ok_or(anyhow::anyhow!("failed to receive event"))
    }
    /// Publishes an application event to on the bus for processing.
    pub fn send(&mut self, app_event: AppEvent) {
        if let Err(e) = self.sender.send(Event::App(app_event)) {
            tracing::error!("error sending event: {}", e);
        }
    }
}

impl Default for EventBus {
    /// Creates a new instance of [`EventBus`] initialized to the default state.
    fn default() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        Self { sender, receiver }
    }
}

/// A task which is executed in a background thread that handles reading Crossterm events and emitting tick
/// events on a regular schedule.
struct EventTask {
    /// Event channel sender.
    sender: mpsc::UnboundedSender<Event>,
}

impl EventTask {
    /// Constructs a new instance of [`EventTask`].
    fn new(sender: mpsc::UnboundedSender<Event>) -> Self {
        Self { sender }
    }
    /// Runs the task. The task emits tick events at a fixed rate and polls for crossterm events
    /// in between. The task will exit when the sender for the event channel is closed.
    async fn run(self) -> anyhow::Result<()> {
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
                self.send(Event::Tick);
              }
              Some(Ok(e)) = crossterm_event => {
                self.send(Event::Crossterm(e));
              }
            };
        }

        Ok(())
    }
    /// Publishes an event to the bus for processing.
    fn send(&self, event: Event) {
        if let Err(e) = self.sender.send(event) {
            tracing::error!("error sending event: {}", e);
        }
    }
}

use bounded_vec_deque::BoundedVecDeque;
use chrono::{
    format::{DelayedFormat, StrftimeItems},
    DateTime, Local,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context, Layer};

/// Key for the field containing the message in a tracing event.
const MESSAGE_KEY: &str = "message";

/// Value used in a log when no file name exists in the tracing event metadata.
const NO_FILE_VALUE: &str = "<unknown>";

/// Value used in a log when no line exists in the tracing event metadata.
const NO_LINE_VALUE: u32 = 0;

/// Value used in a log when no message field exists in the tracing event.
const NO_MESSAGE_VALUE: &str = "<none>";

/// Pattern used to format the timestamp that is output in a log.
const DEFAULT_TIMESTAMP_FORMAT: &str = "%FT%T%.3f";

/// Enumerates the supported logging levels for the emulator.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Display for Level {
    /// Writes a string representation of the [`LogLevel`] value to the formatter.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl From<tracing::Level> for Level {
    /// Converts from an owned [`tracing::Level`] to a [`Level`].
    fn from(value: tracing::Level) -> Self {
        match value {
            tracing::Level::TRACE => Level::Trace,
            tracing::Level::DEBUG => Level::Debug,
            tracing::Level::INFO => Level::Info,
            tracing::Level::WARN => Level::Warn,
            tracing::Level::ERROR => Level::Error,
        }
    }
}

impl From<&tracing::Level> for Level {
    /// Converts from a reference to a [`tracing::Level`] to a [`Level`].
    fn from(value: &tracing::Level) -> Self {
        Level::from(*value)
    }
}

/// The [`Log`] struct contains all relevant data collected when a log is emitted by the
/// application.
#[derive(Debug)]
pub struct Log {
    // [`Level`] of the log message.
    pub level: Level,
    // Timestamp when the log was emitted.
    pub timestamp: DateTime<Local>,
    // Name of the file where the log was emitted.
    pub file: String,
    // Line in the file where the log was emitted.
    pub line: u32,
    // Message value of the emitted log.
    pub message: String,
}

impl Log {
    /// Formats the timestamp of the [`Log`] using the default format string.
    pub fn format_timestamp(&self) -> DelayedFormat<StrftimeItems<'_>> {
        self.timestamp.format(DEFAULT_TIMESTAMP_FORMAT)
    }
}

/// A tracing [`Layer`] implementation which captures log messages and buffers them in memory for
/// display in the UI.
#[derive(Debug)]
pub struct CaptureLayer {
    /// Buffered log messages with a bounded size.
    messages: Arc<Mutex<BoundedVecDeque<Log>>>,
}

impl CaptureLayer {
    /// Creates a new [`CaptureLayer`].
    pub fn new(messages: Arc<Mutex<BoundedVecDeque<Log>>>) -> Self {
        Self { messages }
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = CaptureVisitor::default();
        event.record(&mut visitor);

        let entry = Log {
            level: event.metadata().level().into(),
            timestamp: Local::now(),
            file: event.metadata().file().unwrap_or(NO_FILE_VALUE).to_owned(),
            line: event.metadata().line().unwrap_or(NO_LINE_VALUE),
            message: visitor
                .fields
                .get(MESSAGE_KEY)
                .unwrap_or(&String::from(NO_MESSAGE_VALUE))
                .to_owned(),
        };

        self.messages
            .lock()
            .expect("lock acquired")
            .push_front(entry);
    }
}

/// A simple [`Visit`] implementation that pushes the [`std::fmt::Debug`] representation of the field value
/// into a [`HashMap`] keyed by the name of the field.
#[derive(Debug, Default)]
struct CaptureVisitor<'k> {
    /// Contains the field data for a given [`Event`].
    fields: HashMap<&'k str, String>,
}

impl Visit for CaptureVisitor<'_> {
    /// Visit a value implementing the [`std::fmt::Debug`] trait.
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.fields.insert(field.name(), format!("{:?}", value));
    }
}

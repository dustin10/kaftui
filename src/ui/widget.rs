use crate::kafka::ConsumerMode;

use derive_builder::Builder;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    widgets::{Paragraph, Widget},
};

/// A simple [`Widget`] that renders text for the status line in the footer based on the current
/// [`ConsumerMode`] value for the Kafka consumer.
#[derive(Builder, Debug)]
pub struct ConsumerStatusLine<T, F, PR, PA>
where
    T: AsRef<str> + Clone,
    F: AsRef<str> + Clone,
    PR: Into<Style> + Clone,
    PA: Into<Style> + Clone,
{
    /// Current [`ConsumerMode`] of the Kafka consumer. Determines the color used to render the
    /// status line text.
    consumer_mode: ConsumerMode,
    /// Topic name that records are being consumed from.
    topic: T,
    /// Any filter that was configured by the user.
    filter: Option<F>,
    /// Style used for the text when the consumer mode is [`ConsumerMode::Processing`].
    processing_style: PR,
    /// Style used for the text when the consumer mode is [`ConsumerMode::Paused`].
    paused_style: PA,
}

impl<T, F, PR, PA> ConsumerStatusLine<T, F, PR, PA>
where
    T: AsRef<str> + Clone,
    F: AsRef<str> + Clone,
    PR: Into<Style> + Clone,
    PA: Into<Style> + Clone,
{
    /// Creates a new default [`ConsumerStatusLineBuilder`].
    pub fn builder() -> ConsumerStatusLineBuilder<T, F, PR, PA> {
        ConsumerStatusLineBuilder::default()
    }
}

impl<T, F, PR, PA> Widget for ConsumerStatusLine<T, F, PR, PA>
where
    T: AsRef<str> + Clone,
    F: AsRef<str> + Clone,
    PR: Into<Style> + Clone,
    PA: Into<Style> + Clone,
{
    /// Draws the status line text based on the current mode of the Kafka consumer.
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let (style, filter_text) = match self.consumer_mode {
            ConsumerMode::Processing => {
                let filter_text = self
                    .filter
                    .map(|f| format!(" (Filter: {})", f.as_ref()))
                    .unwrap_or_default();

                (self.processing_style.into(), filter_text)
            }
            ConsumerMode::Paused => (self.paused_style.into(), String::default()),
        };

        let paragraph = Paragraph::new(format!(
            "Topic: {} | {:?}{}",
            self.topic.as_ref(),
            self.consumer_mode,
            filter_text,
        ))
        .style(style);

        paragraph.render(area, buf);
    }
}

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
pub struct ConsumerStatusLine<'a, A, B>
where
    A: Into<Style> + Clone,
    B: Into<Style> + Clone,
{
    /// Current [`ConsumerMode`] of the Kafka consumer. Determines the color used to render the
    /// status line text.
    consumer_mode: ConsumerMode,
    /// Topic name that records are being consumed from.
    topic: &'a str,
    /// Any filter that was configured by the user.
    filter: Option<&'a String>,
    /// Style used for the text when the consumer mode is [`ConsumerMode::Processing`].
    processing_style: A,
    /// Style used for the text when the consumer mode is [`ConsumerMode::Paused`].
    paused_style: B,
}

impl<'a, A, B> ConsumerStatusLine<'a, A, B>
where
    A: Into<Style> + Clone,
    B: Into<Style> + Clone,
{
    /// Creates a new default [`ConsumerStatusLineBuilder`].
    pub fn builder() -> ConsumerStatusLineBuilder<'a, A, B> {
        ConsumerStatusLineBuilder::default()
    }
}

impl<'a, A, B> Widget for ConsumerStatusLine<'a, A, B>
where
    A: Into<Style> + Clone,
    B: Into<Style> + Clone,
{
    /// Draws the status line text based on the current mode of the Kafka consumer.
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let style = match self.consumer_mode {
            ConsumerMode::Processing => self.processing_style.into(),
            ConsumerMode::Paused => self.paused_style.into(),
        };

        let filter_text = self
            .filter
            .as_ref()
            .map(|f| format!(" (Filter: {})", f))
            .unwrap_or_default();

        let paragraph = Paragraph::new(format!(
            "Topic: {} | {:?}{}",
            self.topic, self.consumer_mode, filter_text,
        ))
        .style(style);

        paragraph.render(area, buf);
    }
}

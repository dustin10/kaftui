use crate::{
    app::{BufferedKeyPress, config::Theme},
    event::Event,
    kafka::admin::Topic,
    ui::Component,
};

use crossterm::event::KeyEvent;
use derive_builder::Builder;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, HighlightSpacing, List, ListItem, Padding, Paragraph},
};
use std::str::FromStr;

/// Key bindings that are always displayed to the user in the footer when viewing the topics
/// screen.
const TOPICS_KEY_BINDINGS: [&str; 5] = [
    super::KEY_BINDING_QUIT,
    super::KEY_BINDING_TOP,
    super::KEY_BINDING_NEXT,
    super::KEY_BINDING_PREV,
    super::KEY_BINDING_BOTTOM,
];

/// Enumeration of the widgets in the [`Topics`] component that can have focus.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum TopicsWidget {
    #[default]
    Topics,
}

#[derive(Debug, Default)]
struct TopicsState {
    /// Stores the widget that currently has focus.
    active_widget: TopicsWidget,
    /// List of topics retrieved from the Kafka cluster.
    topics: Vec<Topic>,
    /// Currently selected topic.
    selected_topic: Option<Topic>,
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Topics`]
/// component.
#[derive(Debug)]
struct TopicsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the borders of the selected info panel.
    selected_panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for the key bindings text. Defaults to white.
    key_bindings_text_color: Color,
}

impl From<&Theme> for TopicsTheme {
    /// Converts a reference to a [`Theme`] to a new [`TopicsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let selected_panel_border_color =
            Color::from_str(value.selected_panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            selected_panel_border_color,
            label_color,
            key_bindings_text_color,
        }
    }
}

/// Configuration used to create a new [`Topics`] component.
#[derive(Builder, Debug)]
pub struct TopicsConfig<'a> {
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> TopicsConfig<'a> {
    /// Creates a new default [`TopicsConfigBuilder`] which can be used to create a new
    /// [`TopicsConfig`].
    pub fn builder() -> TopicsConfigBuilder<'a> {
        TopicsConfigBuilder::default()
    }
}

impl<'a> From<TopicsConfig<'a>> for Topics {
    /// Converts from an owned [`TopicsConfig`] to an owned [`Topics`].
    fn from(value: TopicsConfig<'a>) -> Self {
        Self::new(value)
    }
}

/// The application [`Component`] that is responsible for displaying topics that exist on the Kafka
/// cluster.
pub struct Topics {
    /// Current state of the component and it's underlying widgets.
    state: TopicsState,
    /// Color scheme for the component.
    theme: TopicsTheme,
}

impl Topics {
    /// Creates a new [`Topics`] component using the specified [`TopicsConfig`].
    fn new(config: TopicsConfig) -> Self {
        Self {
            state: TopicsState::default(),
            theme: config.theme.into(),
        }
    }
    /// Invoked when the list of topics has been loaded from the Kafka cluster.
    fn on_topics_loaded(&mut self, topics: Vec<Topic>) {
        self.state.topics = topics;
    }
    /// Renders the list of topics contained in the Kafka cluster.
    fn render_topics(&self, frame: &mut Frame, area: Rect) {
        let mut topics_block = Block::bordered()
            .title(" Versions ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == TopicsWidget::Topics {
            topics_block = topics_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let list_items: Vec<ListItem> = self
            .state
            .topics
            .iter()
            .map(|t| ListItem::new::<&str>(t.name.as_ref()))
            .collect();

        let topics_list = List::new(list_items)
            .block(topics_block)
            .highlight_style(Modifier::REVERSED)
            .highlight_symbol(">")
            .highlight_spacing(HighlightSpacing::Always);

        frame.render_widget(topics_list, area);
    }
    /// Renders the details of a topic, if one is currently selected.
    fn render_topic_details(&self, frame: &mut Frame, area: Rect) {
        if let Some(_selected_topic) = &self.state.selected_topic {
            let details_block = Block::bordered()
                .title(" Topic Details ")
                .border_style(self.theme.panel_border_color)
                .padding(Padding::new(1, 1, 0, 0));

            frame.render_widget(details_block, area);
        } else {
            self.render_message(frame, area, "No topic selected");
        }
    }
    /// Renders the given message centered both vertically and horizontally in the given area.
    fn render_message(&self, frame: &mut Frame, area: Rect, msg: &str) {
        let [empty_area, text_area] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

        let empty_text = Paragraph::default().block(
            Block::default()
                .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
                .border_style(self.theme.panel_border_color),
        );

        let message_block = Block::default()
            .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
            .border_style(self.theme.panel_border_color);

        let message_text = Paragraph::new(msg)
            .style(self.theme.panel_border_color)
            .block(message_block)
            .centered();

        frame.render_widget(empty_text, empty_area);
        frame.render_widget(message_text, text_area);
    }
}

impl Component for Topics {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Topics"
    }
    /// Allows the [`Component`] to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &Event) {
        if let Event::TopicsLoaded(topics) = event {
            self.on_topics_loaded(topics.to_vec());
        }
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &mut self,
        _event: KeyEvent,
        _buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        None
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let line = Line::from_iter([
            Span::styled("Topics: ", Style::from(self.theme.label_color).bold()),
            Span::raw(self.state.topics.len().to_string()),
        ]);

        let text = Paragraph::new(line).left_aligned();

        frame.render_widget(text, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let text = Paragraph::new(TOPICS_KEY_BINDINGS.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let [left_panel, right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(80)])
            .areas(area);

        self.render_topics(frame, left_panel);
        self.render_topic_details(frame, right_panel);
    }
    /// Hook for the [`Component`] to run any logic required when it becomes active. The
    /// [`Component`] can also return an optional [`Event`] that will be dispatched.
    fn on_activate(&mut self) -> Option<Event> {
        if self.state.topics.is_empty() {
            Some(Event::LoadTopics)
        } else {
            None
        }
    }
}

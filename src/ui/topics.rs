use crate::{
    app::{BufferedKeyPress, config::Theme},
    event::Event,
    kafka::admin::{Topic, TopicConfig},
    ui::Component,
};

use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, HighlightSpacing, List, ListItem, ListState, Padding,
        Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table,
    },
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

/// Headers for the topic configuration table along with their fill constraints.
const TOPIC_CONFIG_HEADERS: [(&str, u16); 3] = [("Key", 5), ("Value", 4), ("Default", 1)];

/// Headers for the topic partitions table along with their fill constraints.
const TOPIC_PARTITIONS_HEADERS: [(&str, u16); 3] = [("ID", 3), ("Leader", 3), ("Replicas", 4)];

/// Enumerates representing the current network status of the [`Topics`] component.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum NetworkStatus {
    /// The component is idle and not performing any network operations.
    #[default]
    Idle,
    /// The component is currently loading the list of topics from the Kafka cluster.
    LoadingTopics,
    /// The component is currently loading the configuration for the selected topic.
    LoadingTopicConfig,
}

/// Enumeration of the widgets in the [`Topics`] component that can have focus.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum TopicsWidget {
    /// Topics list widget.
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
    /// Configuration details for the currently selected topic.
    selected_topic_config: Option<TopicConfig>,
    /// Manages state of the topics list widget.
    topics_list_state: ListState,
    /// Manages state of the topics list scrollbar.
    topics_scroll_state: ScrollbarState,
    /// Current network status of the component.
    network_status: NetworkStatus,
}

impl TopicsState {
    /// Selects the first topic in the list.
    fn select_first_topic(&mut self) -> Option<&Topic> {
        if self.topics.is_empty() {
            return None;
        }

        self.topics_list_state.select_first();
        self.topics_scroll_state.first();

        self.selected_topic = self.topics.first().cloned();

        self.selected_topic.as_ref()
    }
    /// Selects the next topic in the list.
    fn select_next_topic(&mut self) -> Option<&Topic> {
        if self.topics.is_empty() {
            return None;
        }

        if let Some(curr_idx) = self.topics_list_state.selected()
            && curr_idx == self.topics.len() - 1
        {
            return None;
        }

        self.topics_list_state.select_next();
        self.topics_scroll_state.next();

        let idx = self.topics_list_state.selected().expect("topic selected");

        self.selected_topic = self.topics.get(idx).cloned();

        self.selected_topic.as_ref()
    }
    /// Selects the previous topic in the list.
    fn select_prev_topic(&mut self) -> Option<&Topic> {
        if self.topics.is_empty() {
            return None;
        }

        self.topics_list_state.select_previous();
        self.topics_scroll_state.prev();

        let idx = self.topics_list_state.selected().expect("topic selected");

        self.selected_topic = self.topics.get(idx).cloned();

        self.selected_topic.as_ref()
    }
    /// Selects the last topic in the list.
    fn select_last_topic(&mut self) -> Option<&Topic> {
        if self.topics.is_empty() {
            return None;
        }

        self.topics_list_state.select_last();
        self.topics_scroll_state.last();

        self.selected_topic = self.topics.last().cloned();

        self.selected_topic.as_ref()
    }
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
    /// Constraints for the topic configuration table columns.
    topics_config_constraints: Vec<Constraint>,
    /// Constraints for the topic partitions table columns.
    topics_partitions_constraints: Vec<Constraint>,
}

impl Topics {
    /// Creates a new [`Topics`] component using the specified [`TopicsConfig`].
    fn new(config: TopicsConfig) -> Self {
        let config_constraints: Vec<Constraint> = TOPIC_CONFIG_HEADERS
            .iter()
            .map(|(_, fill)| Constraint::Fill(*fill))
            .collect();

        let partitions_constraints: Vec<Constraint> = TOPIC_PARTITIONS_HEADERS
            .iter()
            .map(|(_, fill)| Constraint::Fill(*fill))
            .collect();

        Self {
            state: TopicsState::default(),
            theme: config.theme.into(),
            topics_config_constraints: config_constraints,
            topics_partitions_constraints: partitions_constraints,
        }
    }
    /// Invoked when the list of topics has been loaded from the Kafka cluster.
    fn on_topics_loaded(&mut self, topics: Vec<Topic>) {
        self.state.network_status = NetworkStatus::Idle;
        self.state.topics = topics;
    }
    /// Invoked when the configuration for the selected topic has been loaded from the Kafka
    /// cluster.
    fn on_topic_config_loaded(&mut self, topic_config: Option<TopicConfig>) {
        self.state.network_status = NetworkStatus::Idle;
        self.state.selected_topic_config = topic_config;
    }
    /// Renders the list of topics contained in the Kafka cluster.
    fn render_topics(&mut self, frame: &mut Frame, area: Rect) {
        if self.state.network_status == NetworkStatus::LoadingTopics {
            self.render_message(frame, area, "Loading topics...");
            return;
        }

        let mut topics_block = Block::bordered()
            .title(" Topics ")
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

        frame.render_stateful_widget(topics_list, area, &mut self.state.topics_list_state);

        if self.state.selected_topic.is_some() {
            self.state.topics_scroll_state = self
                .state
                .topics_scroll_state
                .content_length(self.state.topics.len());

            let scrollbar = Scrollbar::default()
                .orientation(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None);

            frame.render_stateful_widget(
                scrollbar,
                area.inner(Margin {
                    horizontal: 1,
                    vertical: 1,
                }),
                &mut self.state.topics_scroll_state,
            );
        }
    }
    /// Renders the details of a topic, if one is currently selected.
    fn render_topic_details(&self, frame: &mut Frame, area: Rect) {
        if self.state.network_status == NetworkStatus::LoadingTopicConfig {
            self.render_message(frame, area, "Loading config...");
            return;
        }

        if let Some(_selected_topic) = &self.state.selected_topic {
            let [config_panel, partitions_panel] = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(area);

            self.render_topic_config(frame, config_panel);
            self.render_topic_partitions(frame, partitions_panel);
        } else {
            self.render_message(frame, area, "No topic selected");
        }
    }
    /// Renders the topic configuration details panel.
    fn render_topic_config(&self, frame: &mut Frame, area: Rect) {
        let Some(topic_config) = self.state.selected_topic_config.as_ref() else {
            return;
        };

        let title = format!(
            " Config - {} ",
            self.state
                .selected_topic
                .as_ref()
                .map_or("", |t| t.name.as_str())
        );

        let config_block = Block::bordered()
            .title(title)
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let config_rows: Vec<Row> = topic_config
            .entries()
            .iter()
            .map(|e| {
                Row::new(vec![
                    e.key.clone(),
                    e.value.clone().unwrap_or_else(|| String::from("")),
                    e.is_default.to_string(),
                ])
            })
            .collect();

        let header = TOPIC_CONFIG_HEADERS
            .iter()
            .map(|(title, _)| title.bold().style(self.theme.label_color))
            .collect();

        let config_table = Table::new(config_rows, &self.topics_config_constraints)
            .column_spacing(1)
            .header(header)
            .block(config_block);

        frame.render_widget(config_table, area);
    }
    /// Renders the topic partitions panel.
    fn render_topic_partitions(&self, frame: &mut Frame, area: Rect) {
        let Some(topic) = &self.state.selected_topic else {
            return;
        };

        let title = format!(" Partitions - {} ", topic.partitions.len());

        let partitions_block = Block::bordered()
            .title(title)
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let partitions_rows: Vec<Row> = topic
            .partitions
            .iter()
            .map(|p| {
                let replicas_str = p
                    .replicas
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(", ");

                Row::new(vec![p.id.to_string(), p.leader.to_string(), replicas_str])
            })
            .collect();

        let header = TOPIC_PARTITIONS_HEADERS
            .iter()
            .map(|(title, _)| title.bold().style(self.theme.label_color))
            .collect();

        let partitions_table = Table::new(partitions_rows, &self.topics_partitions_constraints)
            .column_spacing(1)
            .header(header)
            .block(partitions_block);

        frame.render_widget(partitions_table, area);
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
        match event {
            Event::TopicsLoaded(topics) => self.on_topics_loaded(topics.to_vec()),
            Event::TopicConfigLoaded(topic_config) => {
                self.on_topic_config_loaded(topic_config.clone())
            }
            _ => {}
        }
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &mut self,
        event: KeyEvent,
        buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        let mapped_event = match event.code {
            KeyCode::Char(c) => match c {
                'g' if buffered.filter(|kp| kp.is('g')).is_some() => self
                    .state
                    .select_first_topic()
                    .map(|t| Event::LoadTopicConfig(t.clone())),
                'j' => self
                    .state
                    .select_next_topic()
                    .map(|t| Event::LoadTopicConfig(t.clone())),
                'k' => self
                    .state
                    .select_prev_topic()
                    .map(|t| Event::LoadTopicConfig(t.clone())),
                'G' => self
                    .state
                    .select_last_topic()
                    .map(|t| Event::LoadTopicConfig(t.clone())),
                _ => None,
            },
            _ => None,
        };

        if let Some(Event::LoadTopicConfig(_)) = mapped_event {
            self.state.network_status = NetworkStatus::LoadingTopicConfig;
        }

        mapped_event
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
            self.state.network_status = NetworkStatus::LoadingTopics;
            Some(Event::LoadTopics)
        } else {
            None
        }
    }
}

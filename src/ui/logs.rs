use crate::{
    app::{config::Theme, BufferedKeyPress},
    event::Event,
    trace::Log,
    ui::Component,
};

use bounded_vec_deque::BoundedVecDeque;
use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    widgets::{
        Block, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table, TableState,
    },
    Frame,
};
use std::str::FromStr;

/// Key bindings that are displayed to the user in the footer when viewing the logs screen.
const LOGS_KEY_BINDINGS: [&str; 5] = [
    super::KEY_BINDING_QUIT,
    super::KEY_BINDING_TOP,
    super::KEY_BINDING_SCROLL_DOWN,
    super::KEY_BINDING_SCROLL_UP,
    super::KEY_BINDING_BOTTOM,
];

#[derive(Debug)]
struct LogsState {
    /// Bounded collection of log messages emitted by the application.
    logs: BoundedVecDeque<Log>,
    /// [`TableState`] for the table that log messages are rendered into.
    list_state: TableState,
    /// [`ScrollbarState`] for the table that logs messages are rendered into.
    list_scroll_state: ScrollbarState,
}

impl LogsState {
    /// Creates a new [`LogsState`].
    fn new(max_history: usize) -> Self {
        Self {
            logs: BoundedVecDeque::new(max_history),
            list_state: TableState::default(),
            list_scroll_state: ScrollbarState::default(),
        }
    }
    /// Moves the logs list scroll state to the top.
    fn scroll_list_top(&mut self) {
        self.list_state.select_first();
        self.list_scroll_state = self.list_scroll_state.position(0);
    }
    /// Moves the logs list scroll state up by one line.
    fn scroll_list_up(&mut self) {
        if self.logs.is_empty() {
            return;
        }

        self.list_state.select_previous();

        let idx = self.list_state.selected().expect("log selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
    }
    /// Moves the logs list scroll state down by one line.
    fn scroll_list_down(&mut self) {
        if self.logs.is_empty() {
            return;
        }

        if let Some(curr_idx) = self.list_state.selected()
            && curr_idx == self.logs.len() - 1
        {
            return;
        }

        self.list_state.select_next();

        let idx = self.list_state.selected().expect("notification selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
    }
    /// Moves the logs list scroll state to the bottom.
    fn scroll_list_bottom(&mut self) {
        let bottom = self.logs.len() - 1;

        self.list_state.select(Some(bottom));
        self.list_scroll_state = self.list_scroll_state.position(bottom);
    }
    fn on_log_emitted(&mut self, log: &Log) {
        self.logs.push_front(log.clone());
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Logs`]
/// component.
#[derive(Debug)]
struct LogsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for the key bindings text. Defaults to white.
    key_bindings_text_color: Color,
}

impl From<&Theme> for LogsTheme {
    /// Converts a reference to a [`Theme`] to a new [`LogsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            label_color,
            key_bindings_text_color,
        }
    }
}

/// Configuration used to create a new [`Logs`] component.
#[derive(Builder, Debug)]
pub struct LogsConfig<'a> {
    /// Maximum number of logs that should be held in memory at any given time.
    max_history: usize,
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> LogsConfig<'a> {
    /// Creates a new default [`LogsConfigBuilder`] which can be used to create a new
    /// [`LogConfig`].
    pub fn builder() -> LogsConfigBuilder<'a> {
        LogsConfigBuilder::default()
    }
}

/// The application [`Component`] that is responsible for displaying the any [`Log`] messages
/// emitted by the application to the user when logs are enabled.
#[derive(Debug)]
pub struct Logs {
    /// Current state of the component and it's underlying widgets.
    state: LogsState,
    /// Color scheme for the component.
    theme: LogsTheme,
}

impl From<LogsConfig<'_>> for Logs {
    /// Converts an owned [`LogsConfig`] to an owned [`Logs`].
    fn from(value: LogsConfig) -> Self {
        Self::new(value)
    }
}

impl Logs {
    /// Creates a new [`Logs`] component using the specified [`LogsConfig`].
    pub fn new(config: LogsConfig) -> Self {
        let state = LogsState::new(config.max_history);

        let theme = config.theme.into();

        Self { state, theme }
    }
}

impl Component for Logs {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Logs"
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(&self, event: KeyEvent, buffered: Option<&BufferedKeyPress>) -> Option<Event> {
        match event.code {
            KeyCode::Char(c) => match c {
                'g' if buffered.map(|kp| kp.is('g')).is_some() => Some(Event::ScrollLogsTop),
                'j' => Some(Event::ScrollLogsDown),
                'k' => Some(Event::ScrollLogsUp),
                'G' => Some(Event::ScrollLogsBottom),
                _ => None,
            },
            _ => None,
        }
    }
    /// Allows the [`Component`] to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &Event) {
        match event {
            Event::ScrollLogsTop => self.state.scroll_list_top(),
            Event::ScrollLogsUp => self.state.scroll_list_up(),
            Event::ScrollLogsDown => self.state.scroll_list_down(),
            Event::ScrollLogsBottom => self.state.scroll_list_bottom(),
            Event::LogEmitted(log) => self.state.on_log_emitted(log),
            _ => {}
        }
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let text = Paragraph::new(LOGS_KEY_BINDINGS.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let table_block = Block::bordered()
            .title(" Logs ")
            .border_style(self.theme.panel_border_color)
            .padding(ratatui::widgets::Padding::new(1, 1, 0, 0));

        let table_rows: Vec<Row> = self
            .state
            .logs
            .iter()
            .map(|l| {
                Row::new([
                    l.format_timestamp().to_string(),
                    format!("{:?}", l.level).to_uppercase(),
                    format!("{}:{}", l.file, l.line),
                    l.message.clone(),
                ])
            })
            .collect();

        let table = Table::new(
            table_rows,
            [
                Constraint::Fill(2),
                Constraint::Fill(1),
                Constraint::Fill(2),
                Constraint::Fill(10),
            ],
        )
        .column_spacing(1)
        .header(Row::new([
            "Timestamp".bold().style(self.theme.label_color),
            "Level".bold().style(self.theme.label_color),
            "File".bold().style(self.theme.label_color),
            "Message".bold().style(self.theme.label_color),
        ]))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .block(table_block);

        frame.render_stateful_widget(table, area, &mut self.state.list_state);

        self.state.list_scroll_state = self
            .state
            .list_scroll_state
            .content_length(self.state.logs.len());

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
            &mut self.state.list_scroll_state,
        );
    }
}

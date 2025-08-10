use crate::{
    app::{config::Theme, BufferedKeyPress},
    event::AppEvent,
    kafka::{ConsumerMode, Record},
    ui::Component,
};

use bounded_vec_deque::BoundedVecDeque;
use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::ToSpan,
    widgets::{
        Block, BorderType, Borders, Padding, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState, Wrap,
    },
    Frame,
};
use std::{cell::Cell, collections::BTreeMap, rc::Rc, str::FromStr};

/// Value displayed for the partition key field when one is not present in the Kafka record.
const EMPTY_PARTITION_KEY: &str = "<empty>";

/// Key bindings that are displayed to the user in the footer no matter what the current state of
/// the application is when viewing the consume topic screen.
const CONSUME_TOPIC_STANDARD_KEY_BINDINGS: [&str; 2] =
    [super::KEY_BINDING_QUIT, super::KEY_BINDING_CHANGE_FOCUS];

/// Text displayed to the user in the footer for the pause key binding.
const KEY_BINDING_PAUSE: &str = "(p) pause";

/// Text displayed to the user in the footer for the resume key binding.
const KEY_BINDING_RESUME: &str = "(r) resume";

/// Text displayed to the user in the footer for the export key binding.
const KEY_BINDING_EXPORT: &str = "(e) export";

/// Enumeration of the widgets in the [`Records`] component that can have focus.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum RecordsWidget {
    RecordList,
    RecordValue,
}

/// Configuration used to create a new [`Records`] component.
#[derive(Debug, Builder)]
pub struct RecordsConfig<'a> {
    /// Reference to the current [`ConsumerMode`].
    consumer_mode: Rc<Cell<ConsumerMode>>,
    /// Topic name that records are being consumed from.
    topic: String,
    /// Any filter that was configured by the user.
    filter: Option<String>,
    /// Controls how many lines each press of a key scrolls the record value text.
    scroll_factor: u16,
    /// Maximum number of records to be displayed in the table widget.
    max_records: usize,
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> RecordsConfig<'a> {
    /// Creates a new default [`RecordsConfigBuilder`] which can be used to create a new
    /// [`RecordsConfig`].
    pub fn builder() -> RecordsConfigBuilder<'a> {
        RecordsConfigBuilder::default()
    }
}

/// Manages state related to records consumed from the Kafka topic and the UI around them.
#[derive(Debug)]
struct RecordsState {
    /// Stores the current [`ConsumerMode`] of the application which indicates whether records are
    /// currently being consumed from the Kafka topic.
    consumer_mode: Rc<Cell<ConsumerMode>>,
    /// Stores the widget that the currently has focus.
    active_widget: RecordsWidget,
    /// Currently selected [`Record`] that is being viewed.
    selected: Option<Record>,
    /// Collection of the [`Record`]s that have been consumed from the Kafka topic.
    records: BoundedVecDeque<Record>,
    /// [`TableState`] for the table that the records consumed from the Kafka topic are rendered
    /// into.
    list_state: TableState,
    /// [`ScrollbarState`] for the table that the records consumed from the Kafka topic are
    /// rendered into.
    list_scroll_state: ScrollbarState,
    /// Contains the current scolling state for the record value text.
    value_scroll: (u16, u16),
    /// Total number of records consumed from the Kafka topic since the application was launched.
    total_consumed: u32,
}

impl RecordsState {
    /// Creates a new [`RecordsState`] using the specified value for the maximum number of records
    /// that an be cached in memory.
    fn new(consumer_mode: Rc<Cell<ConsumerMode>>, max_records: usize) -> Self {
        Self {
            consumer_mode,
            active_widget: RecordsWidget::RecordList,
            selected: None,
            records: BoundedVecDeque::new(max_records),
            list_state: TableState::default(),
            list_scroll_state: ScrollbarState::default(),
            value_scroll: (0, 0),
            total_consumed: 0,
        }
    }
    /// Determines if there is a [`Record`] currently selected.
    pub fn is_record_selected(&self) -> bool {
        self.selected.is_some()
    }
    /// Moves the record value scroll state to the top.
    fn scroll_value_top(&mut self) {
        self.value_scroll.0 = 0;
    }
    /// Moves the record value scroll state down by `n` number of lines.
    fn scroll_value_down(&mut self, n: u16) {
        self.value_scroll.0 += n;
    }
    /// Moves the record value scroll state up by `n` number of lines.
    fn scroll_value_up(&mut self, n: u16) {
        if self.value_scroll.0 >= n {
            self.value_scroll.0 -= n;
        }
    }
    /// Pushes a new [`Record`] onto the current list when a new one is received from the Kafka
    /// consumer.
    fn push_record(&mut self, record: Record) {
        self.records.push_front(record);
        self.total_consumed += 1;

        if let Some(i) = self.list_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.list_state.select(Some(new_idx));
            self.list_scroll_state = self.list_scroll_state.position(new_idx);
        }
    }
    /// Updates the state such so the first [`Record`] in the list will be selected.
    fn select_first(&mut self) {
        if self.records.is_empty() {
            return;
        }

        self.list_state.select_first();
        self.list_scroll_state = self.list_scroll_state.position(0);

        self.selected = self.records.front().cloned();

        self.value_scroll = (0, 0);
    }
    /// Updates the state such so the previous [`Record`] in the list will be selected.
    fn select_prev(&mut self) {
        if self.records.is_empty() {
            return;
        }

        self.list_state.select_previous();

        let idx = self.list_state.selected().expect("record selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
        self.selected = self.records.get(idx).cloned();

        self.value_scroll = (0, 0);
    }
    /// Updates the state such so the next [`Record`] in the list will be selected.
    fn select_next(&mut self) {
        if self.records.is_empty() {
            return;
        }

        if let Some(curr_idx) = self.list_state.selected()
            && curr_idx == self.records.len() - 1
        {
            return;
        }

        self.list_state.select_next();

        let idx = self.list_state.selected().expect("record selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
        self.selected = self.records.get(idx).cloned();

        self.value_scroll = (0, 0);
    }
    /// Updates the state such so the last [`Record`] in the list will be selected.
    fn select_last(&mut self) {
        if self.records.is_empty() {
            return;
        }

        self.list_state.select_last();

        let idx = self.list_state.selected().expect("record selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
        self.selected = self.records.back().cloned();

        self.value_scroll = (0, 0);
    }
    /// Cycles the focus to the next available widget based on the currently selected widget.
    fn select_next_widget(&mut self) {
        let next_widget = match self.active_widget {
            RecordsWidget::RecordList if self.selected.is_some() => {
                Some(RecordsWidget::RecordValue)
            }
            _ => Some(RecordsWidget::RecordList),
        };

        if let Some(widget) = next_widget {
            self.active_widget = widget;
        }
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Records`]
/// component.
#[derive(Debug)]
struct RecordsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the borders of the selected info panel.
    selected_panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for the text in the record list.
    record_list_text_color: Color,
    /// Color used for the status text while the Kafka consumer is active.
    processing_text_color: Color,
    /// Color used for the status text while the Kafka consumer is paused.
    paused_text_color: Color,
    /// Color used for the key bindings text.
    key_bindings_text_color: Color,
    /// Color used for the text in the record info.
    record_info_text_color: Color,
    /// Color used for the text in the record headers.
    record_headers_text_color: Color,
    /// Color used for the text in the record value.
    record_value_text_color: Color,
}

impl From<&Theme> for RecordsTheme {
    /// Converts a reference to a [`Theme`] to a new [`RecordsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let selected_panel_border_color =
            Color::from_str(value.selected_panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let record_list_text_color =
            Color::from_str(value.record_list_text_color.as_str()).expect("valid RGB hex");

        let processing_text_color =
            Color::from_str(value.status_text_color_processing.as_str()).expect("valid RGB hex");

        let paused_text_color =
            Color::from_str(value.status_text_color_paused.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        let record_info_text_color =
            Color::from_str(value.record_info_text_color.as_str()).expect("valid RGB hex");

        let record_headers_text_color =
            Color::from_str(value.record_headers_text_color.as_str()).expect("valid RGB hex");

        let record_value_text_color =
            Color::from_str(value.record_value_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            selected_panel_border_color,
            label_color,
            record_list_text_color,
            processing_text_color,
            paused_text_color,
            key_bindings_text_color,
            record_info_text_color,
            record_headers_text_color,
            record_value_text_color,
        }
    }
}

/// The application [`Component`] that is responsible for displaying the [`Record`]s consumed from
/// the Kafka topic and their details.
#[derive(Debug)]
pub struct Records {
    /// Kafka topic records are consumed from.
    topic: String,
    /// Any filter that was configured by the user.
    filter: Option<String>,
    /// Controls how many lines each press of a key scrolls the record value text.
    scroll_factor: u16,
    /// Color scheme for the component.
    theme: RecordsTheme,
    /// Current state of the component and it's underlying widgets.
    state: RecordsState,
}

impl Records {
    /// Creates a new [`Records`] component using the specified [`RecordsConfig`].
    pub fn new(config: RecordsConfig) -> Self {
        Self {
            topic: config.topic,
            filter: config.filter,
            scroll_factor: config.scroll_factor,
            theme: config.theme.into(),
            state: RecordsState::new(config.consumer_mode, config.max_records),
        }
    }
    /// Renders the record list table.
    fn render_record_list(&mut self, frame: &mut Frame, area: Rect) {
        let mut record_list_block = Block::bordered()
            .title(" Records ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == RecordsWidget::RecordList {
            record_list_block = record_list_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let records_rows = self.state.records.iter().map(|r| {
            let offset = r.offset.to_string();

            let key = r
                .key
                .clone()
                .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY));

            let partition = r.partition.to_string();

            let timestamp = r.timestamp.to_string();

            Row::new([partition, offset, key, timestamp])
        });

        let records_table = Table::new(
            records_rows,
            [
                Constraint::Fill(1),
                Constraint::Fill(1),
                Constraint::Fill(6),
                Constraint::Fill(2),
            ],
        )
        .column_spacing(1)
        .header(Row::new([
            "Partition".bold().style(self.theme.label_color),
            "Offset".bold().style(self.theme.label_color),
            "Key".bold().style(self.theme.label_color),
            "Timestamp".bold().style(self.theme.label_color),
        ]))
        .style(self.theme.record_list_text_color)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .block(record_list_block);

        frame.render_stateful_widget(records_table, area, &mut self.state.list_state);

        if self.state.selected.is_some() {
            self.state.list_scroll_state = self
                .state
                .list_scroll_state
                .content_length(self.state.records.len());

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
    /// Renders the panel containing the details of the selected [`Record`].
    fn render_record_details(&self, frame: &mut Frame, area: Rect) {
        let record = self.state.selected.clone().expect("selected Record exists");

        let layout_slices = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Fill(1),
                Constraint::Fill(3),
                Constraint::Fill(7),
            ])
            .split(area);

        let info_slice = layout_slices[0];
        let headers_slice = layout_slices[1];
        let value_slice = layout_slices[2];

        let info_block = Block::bordered()
            .title(" Info ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let key_value = record
            .key
            .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY));

        let info_rows = vec![
            Row::new([
                "Partition".bold().style(self.theme.label_color),
                record.partition.to_span(),
            ]),
            Row::new([
                "Offset".bold().style(self.theme.label_color),
                record.offset.to_span(),
            ]),
            Row::new([
                "Key".bold().style(self.theme.label_color),
                key_value.to_span(),
            ]),
            Row::new([
                "Timestamp".bold().style(self.theme.label_color),
                record.timestamp.to_span(),
            ]),
        ];

        let info_table = Table::new(info_rows, [Constraint::Fill(1), Constraint::Fill(9)])
            .column_spacing(1)
            .style(self.theme.record_info_text_color)
            .block(info_block);

        let headers_block = Block::bordered()
            .title(" Headers ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let header_rows: Vec<Row> = BTreeMap::from_iter(record.headers.iter())
            .into_iter()
            .map(|(k, v)| Row::new([k.as_str(), v.as_str()]))
            .collect();

        let headers_table = Table::new(header_rows, [Constraint::Min(1), Constraint::Fill(3)])
            .column_spacing(1)
            .header(Row::new([
                "Key".bold().style(self.theme.label_color),
                "Value".bold().style(self.theme.label_color),
            ]))
            .style(self.theme.record_headers_text_color)
            .block(headers_block);

        let mut value_block = Block::bordered()
            .title(" Value ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == RecordsWidget::RecordValue {
            value_block = value_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let value = match record.value {
            Some(v) if !v.is_empty() => match serde_json::from_str(&v)
                .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
            {
                Ok(json) => json,
                Err(e) => {
                    tracing::error!("invalid JSON value: {}", e);
                    v
                }
            },
            _ => String::from(""),
        };

        let value_paragraph = Paragraph::new(value)
            .block(value_block)
            .wrap(Wrap { trim: false })
            .style(self.theme.record_value_text_color)
            .scroll(self.state.value_scroll);

        frame.render_widget(info_table, info_slice);
        frame.render_widget(headers_table, headers_slice);
        frame.render_widget(value_paragraph, value_slice);
    }
    /// Renders the panel containing the details of a [`Record`] when there is currently none
    /// selected.
    fn render_record_empty(&self, frame: &mut Frame, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        let empty_area = layout[0];
        let no_record_text_area = layout[1];

        let empty_text = Paragraph::default().block(
            Block::default()
                .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
                .border_style(self.theme.panel_border_color),
        );

        let no_record_text = Paragraph::new("No Record Selected")
            .style(self.theme.panel_border_color)
            .block(
                Block::default()
                    .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
                    .border_style(self.theme.panel_border_color),
            )
            .centered();

        frame.render_widget(empty_text, empty_area);
        frame.render_widget(no_record_text, no_record_text_area);
    }
}

impl Component for Records {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Records"
    }
    /// Returns a [`Paragraph`] that will be used to render the current status line.
    fn status_line(&self) -> Paragraph<'_> {
        let status_text_color = match self.state.consumer_mode.get() {
            ConsumerMode::Processing => self.theme.processing_text_color,
            ConsumerMode::Paused => self.theme.paused_text_color,
        };

        let filter_text = self
            .filter
            .as_ref()
            .map(|f| format!(" (Filter: {})", f))
            .unwrap_or_default();

        Paragraph::new(format!(
            "Topic: {} | Consumed: {} | {:?}{}",
            self.topic,
            self.state.total_consumed,
            self.state.consumer_mode.get(),
            filter_text,
        ))
        .style(status_text_color)
    }
    /// Returns a [`Paragraph`] that will be used to render the active key bindings.
    fn key_bindings(&self) -> Paragraph<'_> {
        let consumer_mode_key_binding = match self.state.consumer_mode.get() {
            ConsumerMode::Processing => KEY_BINDING_PAUSE,
            ConsumerMode::Paused => KEY_BINDING_RESUME,
        };

        let mut key_bindings = Vec::from(CONSUME_TOPIC_STANDARD_KEY_BINDINGS);

        match self.state.active_widget {
            RecordsWidget::RecordList => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_NEXT);
                key_bindings.push(super::KEY_BINDING_PREV);
                key_bindings.push(super::KEY_BINDING_BOTTOM);
            }
            RecordsWidget::RecordValue => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_SCROLL_DOWN);
                key_bindings.push(super::KEY_BINDING_SCROLL_UP);
            }
        };

        key_bindings.push(consumer_mode_key_binding);

        if self.state.is_record_selected() {
            key_bindings.push(KEY_BINDING_EXPORT);
        }

        Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned()
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`AppEvent`] which will be published
    /// for processing.
    fn map_key_event(
        &self,
        event: KeyEvent,
        buffered: Option<&BufferedKeyPress>,
    ) -> Option<AppEvent> {
        match event.code {
            KeyCode::Char(c) => {
                // TODO: cleanup duplication of e,p and r
                match self.state.active_widget {
                    RecordsWidget::RecordList => match c {
                        'e' => self
                            .state
                            .selected
                            .as_ref()
                            .map(|r| AppEvent::ExportRecord(r.clone())),
                        'p' => Some(AppEvent::PauseProcessing),
                        'r' => Some(AppEvent::ResumeProcessing),
                        'g' if buffered.map(|kp| kp.is('g')).is_some() => {
                            Some(AppEvent::SelectFirstRecord)
                        }
                        'j' => Some(AppEvent::SelectNextRecord),
                        'k' => Some(AppEvent::SelectPrevRecord),
                        'G' => Some(AppEvent::SelectLastRecord),
                        _ => None,
                    },
                    RecordsWidget::RecordValue => match c {
                        'e' => self
                            .state
                            .selected
                            .as_ref()
                            .map(|r| AppEvent::ExportRecord(r.clone())),
                        'p' => Some(AppEvent::PauseProcessing),
                        'r' => Some(AppEvent::ResumeProcessing),
                        'g' if buffered.map(|kp| kp.is('g')).is_some() => {
                            Some(AppEvent::ScrollRecordValueTop)
                        }
                        'j' => Some(AppEvent::ScrollRecordValueDown),
                        'k' => Some(AppEvent::ScrollRecordValueUp),
                        _ => None,
                    },
                }
            }
            _ => None,
        }
    }
    /// Allows the component to handle any [`AppEvent`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &AppEvent) {
        match event {
            AppEvent::SelectFirstRecord => self.state.select_first(),
            AppEvent::SelectPrevRecord => self.state.select_prev(),
            AppEvent::SelectNextRecord => self.state.select_next(),
            AppEvent::SelectLastRecord => self.state.select_last(),
            AppEvent::SelectNextWidget => self.state.select_next_widget(),
            AppEvent::ScrollRecordValueTop => self.state.scroll_value_top(),
            AppEvent::ScrollRecordValueDown => self.state.scroll_value_down(self.scroll_factor),
            AppEvent::ScrollRecordValueUp => self.state.scroll_value_up(self.scroll_factor),
            AppEvent::RecordReceived(record) => self.state.push_record(record.clone()),
            _ => {}
        }
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let view_record_inner = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        let records_table_panel = view_record_inner[0];
        let record_details_panel = view_record_inner[1];

        self.render_record_list(frame, records_table_panel);

        if self.state.is_record_selected() {
            self.render_record_details(frame, record_details_panel);
        } else {
            self.render_record_empty(frame, record_details_panel);
        }
    }
}

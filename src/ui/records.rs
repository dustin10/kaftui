use crate::{
    app::{BufferedKeyPress, config::Theme},
    event::Event,
    kafka::{ConsumerMode, Record},
    ui::{Component, widget::ConsumerStatusLine},
};

use bounded_vec_deque::BoundedVecDeque;
use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::ToSpan,
    widgets::{
        Block, BorderType, Borders, Padding, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        ScrollbarState, Table, TableState, Wrap,
    },
};
use std::{cell::Cell, collections::BTreeMap, rc::Rc, str::FromStr};

/// Value displayed for the partition key field when one is not present in the Kafka record.
const EMPTY_PARTITION_KEY: &str = "<empty>";

/// Key bindings that are displayed to the user in the footer no matter what the current state of
/// the application is when viewing the records UI.
const RECORDS_STANDARD_KEY_BINDINGS: [&str; 2] =
    [super::KEY_BINDING_QUIT, super::KEY_BINDING_CHANGE_FOCUS];

/// Enumeration of the widgets in the [`Records`] component that can have focus.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum RecordsWidget {
    List,
    Value,
    Headers,
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

/// Manages state related to records consumed from the Kafka topic and the UI that renders them to
/// the user.
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
    /// Contains the current scrolling state for the record value text.
    value_scroll: (u16, u16),
    /// [`TableState`] for the table that record headers are rendered into.
    headers_state: TableState,
    /// [`ScrollbarState`] for the table that record headers are rendered into.
    headers_scroll_state: ScrollbarState,
}

impl RecordsState {
    /// Creates a new [`RecordsState`] using the specified value for the maximum number of records
    /// that an be cached in memory.
    fn new(consumer_mode: Rc<Cell<ConsumerMode>>, max_records: usize) -> Self {
        Self {
            consumer_mode,
            active_widget: RecordsWidget::List,
            selected: None,
            records: BoundedVecDeque::new(max_records),
            list_state: TableState::default(),
            list_scroll_state: ScrollbarState::default(),
            value_scroll: (0, 0),
            headers_state: TableState::default(),
            headers_scroll_state: ScrollbarState::default(),
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
    /// Moves the record headers scroll state to the top.
    fn scroll_headers_top(&mut self) {
        self.headers_state.select_first();
        self.headers_scroll_state = self.headers_scroll_state.position(0);
    }
    /// Moves the record headers scroll state down by one line.
    fn scroll_headers_down(&mut self) {
        let headers = &self.selected.as_ref().expect("record selected").headers;

        if headers.is_empty() {
            return;
        }

        if let Some(curr_idx) = self.list_state.selected()
            && curr_idx == headers.len() - 1
        {
            return;
        }

        self.headers_state.select_next();

        let idx = self.headers_state.selected().expect("header selected");

        self.headers_scroll_state = self.headers_scroll_state.position(idx);
    }
    /// Moves the record headers scroll state up by one line.
    fn scroll_headers_up(&mut self) {
        let headers = &self.selected.as_ref().expect("record selected").headers;

        if headers.is_empty() {
            return;
        }

        self.headers_state.select_previous();

        let idx = self.headers_state.selected().expect("header selected");

        self.headers_scroll_state = self.headers_scroll_state.position(idx);
    }
    /// Moves the record headers scroll state to the bottom.
    fn scroll_headers_bottom(&mut self) {
        let bottom = self
            .selected
            .as_ref()
            .expect("record selected")
            .headers
            .len()
            - 1;

        self.headers_state.select(Some(bottom));
        self.headers_scroll_state = self.headers_scroll_state.position(bottom);
    }
    /// Pushes a new [`Record`] onto the current list when a new one is received from the Kafka
    /// consumer.
    fn push_record(&mut self, record: Record) {
        self.records.push_front(record);

        if let Some(i) = self.list_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.list_state.select(Some(new_idx));
            self.list_scroll_state = self.list_scroll_state.position(new_idx);
        }
    }
    /// Resets the state of the record details widgets to their default values.
    fn reset_details_state(&mut self) {
        self.headers_state.select(None);
        self.headers_scroll_state = self.headers_scroll_state.position(0);

        self.value_scroll = (0, 0);
    }
    /// Updates the state such so the first [`Record`] in the list will be selected.
    fn select_first(&mut self) {
        if self.records.is_empty() {
            return;
        }

        self.list_state.select_first();
        self.list_scroll_state = self.list_scroll_state.position(0);

        self.selected = self.records.front().cloned();

        self.reset_details_state();
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

        self.reset_details_state();
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

        self.reset_details_state();
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

        self.reset_details_state();
    }
    /// Cycles the focus to the next available widget based on the currently selected widget.
    fn select_next_widget(&mut self) {
        let next_widget = match self.active_widget {
            RecordsWidget::List if self.selected.is_some() => Some(RecordsWidget::Headers),
            RecordsWidget::Headers => Some(RecordsWidget::Value),
            _ => Some(RecordsWidget::List),
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

impl From<RecordsConfig<'_>> for Records {
    /// Converts from an owned [`RecordsConfig`] to an owned [`Records`].
    fn from(value: RecordsConfig<'_>) -> Self {
        Self::new(value)
    }
}

impl Records {
    /// Creates a new [`Records`] component using the specified [`RecordsConfig`].
    pub fn new(config: RecordsConfig<'_>) -> Self {
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

        if self.state.active_widget == RecordsWidget::List {
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
    fn render_record_details(&mut self, frame: &mut Frame, area: Rect) {
        let record = self.state.selected.clone().expect("selected Record exists");

        let [info_slice, headers_slice, value_slice] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Fill(1),
                Constraint::Fill(3),
                Constraint::Fill(7),
            ])
            .areas(area);

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

        let mut headers_block = Block::bordered()
            .title(" Headers ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == RecordsWidget::Headers {
            headers_block = headers_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

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
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .style(self.theme.record_headers_text_color)
            .block(headers_block);

        self.state.headers_scroll_state = self
            .state
            .headers_scroll_state
            .content_length(record.headers.len());

        let headers_scrollbar = Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None);

        let mut value_block = Block::bordered()
            .title(" Value ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == RecordsWidget::Value {
            value_block = value_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let value = record.value.unwrap_or_default();

        let value_paragraph = Paragraph::new(value)
            .block(value_block)
            .wrap(Wrap { trim: false })
            .style(self.theme.record_value_text_color)
            .scroll(self.state.value_scroll);

        frame.render_widget(info_table, info_slice);

        frame.render_stateful_widget(headers_table, headers_slice, &mut self.state.headers_state);

        frame.render_stateful_widget(
            headers_scrollbar,
            headers_slice.inner(Margin {
                horizontal: 1,
                vertical: 1,
            }),
            &mut self.state.headers_scroll_state,
        );

        frame.render_widget(value_paragraph, value_slice);
    }
    /// Renders the panel containing the details of a [`Record`] when there is currently none
    /// selected.
    fn render_record_empty(&self, frame: &mut Frame, area: Rect) {
        let [empty_area, no_record_text_area] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

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
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let [records_table_panel, record_details_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

        self.render_record_list(frame, records_table_panel);

        if self.state.is_record_selected() {
            self.render_record_details(frame, record_details_panel);
        } else {
            self.render_record_empty(frame, record_details_panel);
        }
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(&self, event: KeyEvent, buffered: Option<&BufferedKeyPress>) -> Option<Event> {
        match event.code {
            KeyCode::Char(c) => match c {
                'e' => self
                    .state
                    .selected
                    .as_ref()
                    .map(|r| Event::ExportRecord(r.clone())),
                'p' => Some(Event::PauseProcessing),
                'r' => Some(Event::ResumeProcessing),
                _ => match self.state.active_widget {
                    RecordsWidget::List => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::SelectFirstRecord)
                        }
                        'j' => Some(Event::SelectNextRecord),
                        'k' => Some(Event::SelectPrevRecord),
                        'G' => Some(Event::SelectLastRecord),
                        _ => None,
                    },
                    RecordsWidget::Value => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollRecordValueTop)
                        }
                        'j' => Some(Event::ScrollRecordValueDown),
                        'k' => Some(Event::ScrollRecordValueUp),
                        _ => None,
                    },
                    RecordsWidget::Headers => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollRecordHeadersTop)
                        }
                        'j' => Some(Event::ScrollRecordHeadersDown),
                        'k' => Some(Event::ScrollRecordHeadersUp),
                        'G' => Some(Event::ScrollRecordHeadersBottom),
                        _ => None,
                    },
                },
            },
            _ => None,
        }
    }
    /// Allows the [`Component`] to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &Event) {
        match event {
            Event::SelectFirstRecord => self.state.select_first(),
            Event::SelectPrevRecord => self.state.select_prev(),
            Event::SelectNextRecord => self.state.select_next(),
            Event::SelectLastRecord => self.state.select_last(),
            Event::SelectNextWidget => self.state.select_next_widget(),
            Event::ScrollRecordValueTop => self.state.scroll_value_top(),
            Event::ScrollRecordValueDown => self.state.scroll_value_down(self.scroll_factor),
            Event::ScrollRecordValueUp => self.state.scroll_value_up(self.scroll_factor),
            Event::ScrollRecordHeadersTop => self.state.scroll_headers_top(),
            Event::ScrollRecordHeadersDown => self.state.scroll_headers_down(),
            Event::ScrollRecordHeadersUp => self.state.scroll_headers_up(),
            Event::ScrollRecordHeadersBottom => self.state.scroll_headers_bottom(),
            Event::RecordReceived(record) => self.state.push_record(record.clone()),
            _ => {}
        }
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let consumer_status_line = ConsumerStatusLine::builder()
            .consumer_mode(self.state.consumer_mode.get())
            .topic(self.topic.as_str())
            .filter(self.filter.as_ref())
            .processing_style(self.theme.processing_text_color)
            .paused_style(self.theme.paused_text_color)
            .build()
            .expect("valid consumer status line widget");

        frame.render_widget(consumer_status_line, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let consumer_mode_key_binding = match self.state.consumer_mode.get() {
            ConsumerMode::Processing => super::KEY_BINDING_PAUSE,
            ConsumerMode::Paused => super::KEY_BINDING_RESUME,
        };

        let mut key_bindings = Vec::from(RECORDS_STANDARD_KEY_BINDINGS);

        match self.state.active_widget {
            RecordsWidget::List => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_NEXT);
                key_bindings.push(super::KEY_BINDING_PREV);
                key_bindings.push(super::KEY_BINDING_BOTTOM);
            }
            RecordsWidget::Value => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_SCROLL_DOWN);
                key_bindings.push(super::KEY_BINDING_SCROLL_UP);
            }
            RecordsWidget::Headers => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_NEXT);
                key_bindings.push(super::KEY_BINDING_PREV);
                key_bindings.push(super::KEY_BINDING_BOTTOM);
            }
        };

        key_bindings.push(consumer_mode_key_binding);

        if self.state.is_record_selected() {
            key_bindings.push(super::KEY_BINDING_EXPORT);
        }

        let text = Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
}

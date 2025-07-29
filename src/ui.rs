use crate::app::{App, ConsumerMode, Screen, SelectableWidget};

use ratatui::{
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::ToSpan,
    widgets::{
        Block, BorderType, Borders, Padding, Paragraph, Row, Scrollbar, ScrollbarOrientation,
        Table, Wrap,
    },
    Frame,
};
use std::{collections::BTreeMap, num::ParseIntError};

/// Value displayed for the partition key field when one is not present in the Kafka record.
const EMPTY_PARTITION_KEY: &str = "<empty>";

/// Text displayed to the user in the footer for the quit key binding.
const KEY_BINDING_QUIT: &str = "(esc) quit";

/// Text displayed to the user in the footer for the cycle widget key binding.
const KEY_BINDING_CHANGE_FOCUS: &str = "(tab) change focus";

/// Text displayed to the user in the footer for the scroll down key binding.
const KEY_BINDING_SCROLL_DOWN: &str = "(j) down";

/// Text displayed to the user in the footer for the scroll up key binding.
const KEY_BINDING_SCROLL_UP: &str = "(k) up";

/// Text displayed to the user in the footer for the first record key binding.
const KEY_BINDING_FIRST: &str = "(gg) first";

/// Text displayed to the user in the footer for the next record key binding.
const KEY_BINDING_NEXT: &str = "(j) next";

/// Text displayed to the user in the footer for the previous record key binding.
const KEY_BINDING_PREV: &str = "(k) prev";

/// Text displayed to the user in the footer for the select last record key binding.
const KEY_BINDING_LAST: &str = "(G) last";

/// Text displayed to the user in the footer for the pause key binding.
const KEY_BINDING_PAUSE: &str = "(p) pause";

/// Text displayed to the user in the footer for the resume key binding.
const KEY_BINDING_RESUME: &str = "(r) resume";

/// Text displayed to the user in the footer for the export key binding.
const KEY_BINDING_EXPORT: &str = "(e) export";

/// Key bindings that are displayed to the user in the footer no matter what the current state of
/// the application is.
const STANDARD_KEY_BINDINGS: [&str; 2] = [KEY_BINDING_QUIT, KEY_BINDING_CHANGE_FOCUS];

impl App {
    /// Draws the UI for the application to the given [`Frame`] based on the current screen the
    /// user is viewing.
    pub fn draw(&mut self, frame: &mut Frame) {
        match self.screen {
            Screen::Initialize => render_initialize(self, frame),
            Screen::ConsumeTopic => render_consume_topic(self, frame),
        }
    }
}

// TODO: move away from parsing the string to Color on every draw by parsing once at startup

/// Converts a string containing a hex color value into a [`Color`].
fn color_from_hex_string(hex: &str) -> Result<Color, ParseIntError> {
    u32::from_str_radix(hex, 16).map(Color::from_u32)
}

/// Renders the UI to the terminal for the [`Screen::Initialize`] screen.
fn render_initialize(app: &App, frame: &mut Frame) {
    let border_color =
        color_from_hex_string(&app.config.theme.panel_border_color).expect("valid u32 hex");

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(frame.area());

    let empty_area = layout[0];
    let initializing_text_area = layout[1];

    let empty_text = Paragraph::default().block(
        Block::default()
            .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
            .border_style(border_color),
    );

    let no_record_text = Paragraph::new("Initializing...")
        .style(border_color)
        .block(
            Block::default()
                .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
                .border_style(border_color),
        )
        .centered();

    frame.render_widget(empty_text, empty_area);
    frame.render_widget(no_record_text, initializing_text_area);
}

/// Renders the UI to the terminal for the [`Screen::ConsumeTopic`] screen.
fn render_consume_topic(app: &mut App, frame: &mut Frame) {
    let full_screen = frame.area();

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(full_screen);

    let view_record = outer[0];
    let footer = outer[1];

    let view_record_inner = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(view_record);

    let left_panel = view_record_inner[0];
    let right_panel = view_record_inner[1];

    render_record_list(app, frame, left_panel);

    if app.state.selected.is_some() {
        render_record_details(app, frame, right_panel);
    } else {
        render_record_empty(app, frame, right_panel);
    }

    render_footer(app, frame, footer);
}

/// Renders the table that contains the [`Record`]s that have been consumed from the topic.
fn render_record_list(app: &mut App, frame: &mut Frame, area: Rect) {
    let state = &mut app.state;

    let label_color = color_from_hex_string(&app.config.theme.label_color).expect("valid u32 hex");

    let border_color =
        color_from_hex_string(&app.config.theme.panel_border_color).expect("valid u32 hex");

    let record_list_color =
        color_from_hex_string(&app.config.theme.record_list_color).expect("valid u32 hex");

    let mut record_list_block = Block::bordered()
        .title(" Records ")
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    if state.selected_widget == SelectableWidget::RecordList {
        let selected_panel_color =
            color_from_hex_string(&app.config.theme.selected_panel_border_color)
                .expect("valid u32 hex");

        record_list_block = record_list_block
            .border_type(BorderType::Thick)
            .border_style(selected_panel_color);
    }

    let records_rows = state.records.iter().map(|r| {
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
        "Partition".bold().style(label_color),
        "Offset".bold().style(label_color),
        "Key".bold().style(label_color),
        "Timestamp".bold().style(label_color),
    ]))
    .style(record_list_color)
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
    .block(record_list_block);

    frame.render_stateful_widget(records_table, area, &mut state.record_list_state);

    if state.selected.is_some() {
        state.record_list_scroll_state = state
            .record_list_scroll_state
            .content_length(state.records.len());

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
            &mut state.record_list_scroll_state,
        );
    }
}

/// Renders the record details panel when there is an active [`Record`] set.
fn render_record_details(app: &App, frame: &mut Frame, area: Rect) {
    let state = &app.state;

    let record = state.selected.clone().expect("selected record exists");

    let border_color =
        color_from_hex_string(&app.config.theme.panel_border_color).expect("valid u32 hex");

    let label_color = color_from_hex_string(&app.config.theme.label_color).expect("valid u32 hex");

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
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    let key_value = record
        .key
        .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY));

    let info_color =
        color_from_hex_string(&app.config.theme.record_info_color).expect("valid u32 hex");

    let info_rows = vec![
        Row::new([
            "Partition".bold().style(label_color),
            record.partition.to_span(),
        ]),
        Row::new(["Offset".bold().style(label_color), record.offset.to_span()]),
        Row::new(["Key".bold().style(label_color), key_value.to_span()]),
        Row::new([
            "Timestamp".bold().style(label_color),
            record.timestamp.to_span(),
        ]),
    ];

    let info_table = Table::new(info_rows, [Constraint::Fill(1), Constraint::Fill(9)])
        .column_spacing(1)
        .style(info_color)
        .block(info_block);

    let headers_block = Block::bordered()
        .title(" Headers ")
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    let headers_color =
        color_from_hex_string(&app.config.theme.record_headers_color).expect("valid u32 hex");

    let header_rows: Vec<Row> = BTreeMap::from_iter(record.headers.iter())
        .into_iter()
        .map(|(k, v)| Row::new([k.as_str(), v.as_str()]))
        .collect();

    let headers_table = Table::new(header_rows, [Constraint::Min(1), Constraint::Fill(3)])
        .column_spacing(1)
        .header(Row::new([
            "Key".bold().style(label_color),
            "Value".bold().style(label_color),
        ]))
        .style(headers_color)
        .block(headers_block);

    let mut value_block = Block::bordered()
        .title(" Value ")
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    if state.selected_widget == SelectableWidget::RecordValue {
        let selected_panel_color =
            color_from_hex_string(&app.config.theme.selected_panel_border_color)
                .expect("valid u32 hex");

        value_block = value_block
            .border_type(BorderType::Thick)
            .border_style(selected_panel_color);
    }

    let value = if record.value.is_empty() {
        String::from("")
    } else {
        match serde_json::from_str(&record.value)
            .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
        {
            Ok(json) => json,
            Err(e) => {
                tracing::error!("invalid JSON value: {}", e);
                record.value
            }
        }
    };

    let value_color =
        color_from_hex_string(&app.config.theme.record_value_color).expect("valid u32 hex");

    let value_paragraph = Paragraph::new(value)
        .block(value_block)
        .wrap(Wrap { trim: false })
        .style(value_color)
        .scroll(state.record_list_value_scroll);

    frame.render_widget(info_table, info_slice);
    frame.render_widget(headers_table, headers_slice);
    frame.render_widget(value_paragraph, value_slice);
}

/// Renders the record details panel when no active [`Record`] set.
fn render_record_empty(app: &App, frame: &mut Frame, area: Rect) {
    let border_color =
        color_from_hex_string(&app.config.theme.panel_border_color).expect("valid u32 hex");

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let empty_area = layout[0];
    let no_record_text_area = layout[1];

    let empty_text = Paragraph::default().block(
        Block::default()
            .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
            .border_style(border_color),
    );

    let no_record_text = Paragraph::new("No Record Selected")
        .style(border_color)
        .block(
            Block::default()
                .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
                .border_style(border_color),
        )
        .centered();

    frame.render_widget(empty_text, empty_area);
    frame.render_widget(no_record_text, no_record_text_area);
}

/// Renders the footer panel that contains the key bindings.
fn render_footer(app: &App, frame: &mut Frame, area: Rect) {
    let border_color =
        color_from_hex_string(&app.config.theme.panel_border_color).expect("valid u32 hex");

    let status_text_processing_color =
        color_from_hex_string(&app.config.theme.status_text_color_processing)
            .expect("valid u32 hex");

    let status_text_color_paused =
        color_from_hex_string(&app.config.theme.status_text_color_paused).expect("valid u32 hex");

    let key_bindings_text_color =
        color_from_hex_string(&app.config.theme.key_bindings_text_color).expect("valid u32 hex");

    let outer = Block::bordered()
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    let inner_area = outer.inner(area);

    let inner_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner_area);

    let left_panel = inner_layout[0];
    let right_panel = inner_layout[1];

    let (consumer_mode_key_binding, stats_color) = match app.state.consumer_mode {
        ConsumerMode::Processing => (KEY_BINDING_PAUSE, status_text_processing_color),
        ConsumerMode::Paused => (KEY_BINDING_RESUME, status_text_color_paused),
    };

    let filter_text = app
        .config
        .filter
        .as_ref()
        .map(|f| format!(" (Filter: {})", f))
        .unwrap_or_default();

    let stats = Paragraph::new(format!(
        "Topic: {} | Consumed: {} | {:?}{}",
        app.config.topic, app.state.total_consumed, app.state.consumer_mode, filter_text,
    ))
    .style(stats_color);

    let mut key_bindings = Vec::from(STANDARD_KEY_BINDINGS);

    match app.state.selected_widget {
        SelectableWidget::RecordList => {
            key_bindings.push(KEY_BINDING_FIRST);
            key_bindings.push(KEY_BINDING_NEXT);
            key_bindings.push(KEY_BINDING_PREV);
            key_bindings.push(KEY_BINDING_LAST);
        }
        SelectableWidget::RecordValue => {
            key_bindings.push(KEY_BINDING_SCROLL_DOWN);
            key_bindings.push(KEY_BINDING_SCROLL_UP);
        }
    };

    key_bindings.push(consumer_mode_key_binding);

    if app.state.selected.is_some() {
        key_bindings.push(KEY_BINDING_EXPORT);
    }

    let key_bindings_paragraph = Paragraph::new(key_bindings.join(" | "))
        .style(key_bindings_text_color)
        .right_aligned();

    frame.render_widget(outer, area);
    frame.render_widget(stats, left_panel);
    frame.render_widget(key_bindings_paragraph, right_panel);
}

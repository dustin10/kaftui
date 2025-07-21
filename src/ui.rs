use crate::app::{App, ConsumerMode, Screen, SelectableWidget, State};

use ratatui::{
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    widgets::{
        Block, BorderType, List, ListItem, Padding, Paragraph, Row, Scrollbar,
        ScrollbarOrientation, Table, Wrap,
    },
    Frame,
};

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

/// Text displayed to the user in the footer for the next record key binding.
const KEY_BINDING_NEXT: &str = "(j) next";

/// Text displayed to the user in the footer for the previous record key binding.
const KEY_BINDING_PREV: &str = "(k) prev";

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
            Screen::ConsumeTopic => render_consume_topic(self, frame),
        }
    }
}

/// Renders the UI to the terminal for the [`Screen::ConsumeTopic`] screen.
fn render_consume_topic(app: &mut App, frame: &mut Frame) {
    let state = &mut app.state;

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

    render_record_list(state, frame, left_panel);

    if state.selected.is_some() {
        render_record_details(state, frame, right_panel);
    } else {
        render_record_empty(frame, right_panel);
    }

    render_footer(app, frame, footer);
}

/// Renders the table that contains the [`Record`]s that have been consumed from the topic.
fn render_record_list(state: &mut State, frame: &mut Frame, area: Rect) {
    let mut record_list_block = Block::bordered()
        .title(" Records ")
        .padding(Padding::new(1, 1, 0, 0));

    if state.selected_widget == SelectableWidget::RecordList {
        record_list_block = record_list_block
            .border_type(BorderType::Thick)
            .border_style(Color::Cyan);
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
        "Partition".bold(),
        "Offset".bold(),
        "Key".bold(),
        "Timestamp".bold(),
    ]))
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
fn render_record_details(state: &State, frame: &mut Frame, area: Rect) {
    let record = state.selected.clone().expect("selected record exists");

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
        .padding(Padding::new(1, 1, 0, 0));

    let info_items = vec![
        ListItem::new(format!("Offset:    {}", record.offset)),
        ListItem::new(format!(
            "Key:       {}",
            record
                .key
                .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY))
        )),
        ListItem::new(format!("Partition: {}", record.partition)),
        ListItem::new(format!("Timestamp: {}", record.timestamp)),
    ];

    let info_list = List::new(info_items).block(info_block);

    let headers_block = Block::bordered()
        .title(" Headers ")
        .padding(Padding::new(1, 1, 0, 0));

    let header_rows: Vec<Row> = record
        .headers
        .into_iter()
        .map(|(k, v)| Row::new([k, v]))
        .collect();

    let headers_table = Table::new(header_rows, [Constraint::Min(1), Constraint::Fill(3)])
        .column_spacing(1)
        .header(Row::new(["Key".bold(), "Value".bold()]))
        .block(headers_block);

    let mut value_block = Block::bordered()
        .title(" Value ")
        .padding(Padding::new(1, 1, 0, 0));

    if state.selected_widget == SelectableWidget::RecordValue {
        value_block = value_block
            .border_type(BorderType::Thick)
            .border_style(Color::Cyan);
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

    let value_paragraph = Paragraph::new(value)
        .block(value_block)
        .wrap(Wrap { trim: false })
        .scroll(state.record_list_value_scroll);

    frame.render_widget(info_list, info_slice);
    frame.render_widget(headers_table, headers_slice);
    frame.render_widget(value_paragraph, value_slice);
}

/// Renders the record details panel when no active [`Record`] set.
fn render_record_empty(frame: &mut Frame, area: Rect) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let text_area = layout[1];

    let empty_text = Paragraph::new("No Record Selected")
        .block(Block::default())
        .centered();

    frame.render_widget(empty_text, text_area);
}

/// Renders the footer panel that contains the key bindings.
fn render_footer(app: &App, frame: &mut Frame, area: Rect) {
    let outer = Block::bordered().padding(Padding::new(1, 1, 0, 0));

    let inner_area = outer.inner(area);

    let inner_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner_area);

    let left_panel = inner_layout[0];
    let right_panel = inner_layout[1];

    let (consumer_mode_key_binding, stats_color) = match app.state.consumer_mode {
        ConsumerMode::Processing => (KEY_BINDING_PAUSE, Color::LightGreen),
        ConsumerMode::Paused => (KEY_BINDING_RESUME, Color::LightRed),
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
            key_bindings.push(KEY_BINDING_NEXT);
            key_bindings.push(KEY_BINDING_PREV);
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

    let key_bindings_paragraph = Paragraph::new(key_bindings.join(" | ")).right_aligned();

    frame.render_widget(outer, area);
    frame.render_widget(stats, left_panel);
    frame.render_widget(key_bindings_paragraph, right_panel);
}

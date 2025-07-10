use crate::{
    app::{App, Screen, State},
    kafka::Record,
};

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Stylize},
    widgets::{Block, List, ListItem, Paragraph, Row, Table, Widget},
};

/// Value displayed for the partition key field when one is not present in the Kafka record.
const EMPTY_PARTITION_KEY: &str = "<empty>";

impl Widget for &App {
    /// Renders the widgets that make up the application based on the current application state.
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self.screen {
            Screen::ConsumeTopic => render_consume_topic(&self.state, area, buf),
        }
    }
}

/// Renders the UI to the terminal for the [`Screen::ConsumeTopic`] screen.
fn render_consume_topic(state: &State, area: Rect, buf: &mut Buffer) {
    let containers = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let left_side = containers[0];
    let right_side = containers[1];

    render_record_list(state, left_side, buf);

    if let Some(record) = state.selected.as_ref() {
        render_record_details(record.clone(), right_side, buf);
    } else {
        render_record_empty(right_side, buf);
    }
}

/// Renders the table that contains the [`Record`]s that have been consumed from the topic.
fn render_record_list(state: &State, area: Rect, buf: &mut Buffer) {
    let record_list_block = Block::bordered()
        .title(" Records ")
        .border_style(Color::Cyan);

    let records_rows = state.records.iter().rev().map(|r| {
        let key = r
            .partition_key
            .clone()
            .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY));

        let partition = r.partition.to_string();

        let timestamp = r.timestamp.to_string();

        Row::new([key, partition, timestamp])
    });

    let records_table = Table::new(
        records_rows,
        [
            Constraint::Fill(2),
            Constraint::Fill(1),
            Constraint::Fill(2),
        ],
    )
    .column_spacing(1)
    .header(Row::new([
        "Key".bold(),
        "Partition".bold(),
        "Timestamp".bold(),
    ]))
    .block(record_list_block);

    records_table.render(area, buf);
}

/// Renders the record details panel when there is an active [`Record`] set.
fn render_record_details(record: Record, area: Rect, buf: &mut Buffer) {
    let layout_slices = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Fill(1),
            Constraint::Fill(3),
        ])
        .split(area);

    let info_slice = layout_slices[0];
    let headers_slice = layout_slices[1];
    let value_slice = layout_slices[2];

    let info_block = Block::bordered().title(" Info ").border_style(Color::Cyan);

    let info_items = vec![
        ListItem::new(format!("Topic:     {}", record.topic)),
        ListItem::new(format!("Partition: {}", record.partition)),
        ListItem::new(format!(
            "Key:       {}",
            record
                .partition_key
                .unwrap_or_else(|| String::from(EMPTY_PARTITION_KEY))
        )),
        ListItem::new(format!("Timestamp: {}", record.timestamp)),
    ];

    let info_list = List::new(info_items).block(info_block);

    info_list.render(info_slice, buf);

    let headers_block = Block::bordered()
        .title(format!(" Headers ({}) ", record.headers.len()))
        .border_style(Color::Cyan);

    let header_rows: Vec<Row> = record
        .headers
        .into_iter()
        .map(|(k, v)| Row::new([k, v]))
        .collect();

    let headers_table = Table::new(header_rows, [Constraint::Min(1), Constraint::Fill(3)])
        .column_spacing(1)
        .header(Row::new(["Key".bold(), "Value".bold()]))
        .block(headers_block);

    headers_table.render(headers_slice, buf);

    let value_block = Block::bordered()
        .title(" Value ".cyan())
        .border_style(Color::Cyan);

    let value = if record.value.is_empty() {
        String::from("")
    } else {
        match serde_json::from_str(&record.value)
            .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
        {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!("invalid JSON value: {}", e);
                record.value
            }
        }
    };

    let value_paragraph = Paragraph::new(value).block(value_block);

    value_paragraph.render(value_slice, buf);
}

/// Renders the record details panel when no active [`Record`] set.
fn render_record_empty(area: Rect, buf: &mut Buffer) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let text_area = layout[1];

    let empty_text = Paragraph::new("No Record Selected")
        .block(Block::default())
        .centered();

    empty_text.render(text_area, buf);
}

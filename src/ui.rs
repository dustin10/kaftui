use crate::app::{App, Screen, State};

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Stylize},
    widgets::{Block, List, ListItem, Paragraph, Row, Table, Widget},
};

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
    let record = state.record.clone().unwrap_or_default();

    let slices = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Fill(1),
            Constraint::Fill(3),
        ])
        .split(area);

    let info_block = Block::bordered().title(" Topic ").border_style(Color::Cyan);

    let info_items = vec![
        ListItem::new(format!("Topic:     {}", record.topic)),
        ListItem::new(format!("Partition: {}", record.partition)),
    ];

    let info_list = List::new(info_items).block(info_block);

    info_list.render(slices[0], buf);

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

    headers_table.render(slices[1], buf);

    let value_block = Block::bordered()
        .title(" Value ".cyan())
        .border_style(Color::Cyan);

    let json = match serde_json::from_str(&record.value)
        .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
    {
        Ok(json) => json,
        Err(e) => {
            tracing::error!("failed to format JSON: {}", e);
            record.value
        }
    };

    let value_paragraph = Paragraph::new(json).block(value_block);

    value_paragraph.render(slices[2], buf);
}

use crate::app::App;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, BorderType, List, ListItem, Paragraph, Widget},
};

impl Widget for &App {
    /// Renders the widgets that make up the application based on the application state.
    fn render(self, area: Rect, buf: &mut Buffer) {
        let slices = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Fill(1), Constraint::Fill(3)])
            .margin(1)
            .split(area);

        let info_block = Block::bordered()
            .title("Info")
            .border_type(BorderType::Rounded);

        let info = Paragraph::new("").block(info_block);

        info.render(slices[0], buf);

        let record = self.state.record.clone().unwrap_or_default();

        let record_slices = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Fill(1), Constraint::Fill(3)])
            .split(slices[1]);

        let headers_block = Block::bordered()
            .title("Headers")
            .border_type(BorderType::Rounded);

        let header_items: Vec<ListItem> = record
            .headers
            .into_iter()
            .map(|(k, v)| ListItem::new(format!("{k} -> {v}")))
            .collect();

        let headers_list = List::new(header_items).block(headers_block);

        headers_list.render(record_slices[0], buf);

        let value_block = Block::bordered()
            .title("Value")
            .border_type(BorderType::Rounded);

        let value_paragraph = Paragraph::new(record.value).block(value_block);

        value_paragraph.render(record_slices[1], buf);
    }
}

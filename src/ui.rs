use crate::app::App;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, BorderType, Paragraph, Widget},
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

        let record_slices = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Fill(1), Constraint::Fill(3)])
            .split(slices[1]);

        let headers_block = Block::bordered()
            .title("Headers")
            .border_type(BorderType::Rounded);

        let headers = Paragraph::new("").block(headers_block);

        headers.render(record_slices[0], buf);

        let value_block = Block::bordered()
            .title("Value")
            .border_type(BorderType::Rounded);

        let value =
            Paragraph::new("{\n    \"foo\":\"bar\",\n    \"biz\":\"baz\"\n}").block(value_block);

        value.render(record_slices[1], buf);
    }
}

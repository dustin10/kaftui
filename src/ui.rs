use crate::app::App;

use crossterm::style::Color;
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Rect},
    style::Stylize,
    widgets::{Block, BorderType, Paragraph, Widget},
};

impl Widget for &App {
    /// Renders the widgets that make up the application based on the application state.
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title("kaftui")
            .title_alignment(Alignment::Center)
            .border_type(BorderType::Rounded);

        let text = "Press `Esc`, `Ctrl-C` or `q` to stop running.";

        let paragraph = Paragraph::new(text)
            .block(block)
            .fg(Color::Cyan)
            .bg(Color::Black)
            .centered();

        paragraph.render(area, buf);
    }
}

mod logs;
mod records;
mod stats;
mod widget;

pub use crate::ui::{
    logs::{Logs, LogsConfig},
    records::{Records, RecordsConfig},
    stats::{Stats, StatsConfig},
};

use crate::{
    app::{App, BufferedKeyPress, NotificationStatus},
    event::Event,
};

use crossterm::event::KeyEvent;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Padding, Paragraph, Tabs},
    Frame,
};
use std::str::FromStr;

/// Text displayed to the user in the footer for the quit key binding.
const KEY_BINDING_QUIT: &str = "(esc) quit";

/// Text displayed to the user in the footer for the cycle widget key binding.
const KEY_BINDING_CHANGE_FOCUS: &str = "(tab) cycle focus";

/// Text displayed to the user in the footer for the pause key binding.
const KEY_BINDING_PAUSE: &str = "(p) pause";

/// Text displayed to the user in the footer for the resume key binding.
const KEY_BINDING_RESUME: &str = "(r) resume";

/// Text displayed to the user in the footer for the scroll down key binding.
const KEY_BINDING_SCROLL_DOWN: &str = "(j) down";

/// Text displayed to the user in the footer for the scroll up key binding.
const KEY_BINDING_SCROLL_UP: &str = "(k) up";

/// Text displayed to the user in the footer for the move to top key binding.
const KEY_BINDING_TOP: &str = "(gg) top";

/// Text displayed to the user in the footer for the next record key binding.
const KEY_BINDING_NEXT: &str = "(j) next";

/// Text displayed to the user in the footer for the previous record key binding.
const KEY_BINDING_PREV: &str = "(k) prev";

/// Text displayed to the user in the footer for the move to bottom key binding.
const KEY_BINDING_BOTTOM: &str = "(G) bottom";

/// A [`Component`] represents a top-level screen in the application that the user can view and
/// interact with. Each [`Component`] that is created and added to the [`App`] can be selected by
/// the user using the menu items.
pub trait Component {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str;
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect);
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &self,
        _event: KeyEvent,
        _buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        None
    }
    /// Allows the component to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, _event: &Event) {}
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, _frame: &mut Frame, _area: Rect) {}
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, _frame: &mut Frame, _area: Rect) {}
}

impl App {
    /// Draws the UI for the application to the given [`Frame`] based on the current screen the
    /// user is viewing.
    pub fn draw(&mut self, frame: &mut Frame) {
        if self.state.initializing {
            self.render_initialize(frame);
        } else {
            let [header_area, component_area, footer_area] = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(1),
                    Constraint::Length(3),
                ])
                .areas(frame.area());

            self.render_header(frame, header_area);
            self.render_component(frame, component_area);
            self.render_footer(frame, footer_area);
        }
    }
    /// Renders the UI to the terminal during application initialization.
    fn render_initialize(&self, frame: &mut Frame) {
        let border_color =
            Color::from_str(&self.config.theme.panel_border_color).expect("valid RGB color");

        let [empty_area, initializing_text_area] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(frame.area());

        let empty_text = Paragraph::default().block(
            Block::default()
                .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
                .border_style(border_color),
        );

        let initializing_text = Paragraph::new("Initializing...")
            .style(border_color)
            .block(
                Block::default()
                    .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
                    .border_style(border_color),
            )
            .centered();

        frame.render_widget(empty_text, empty_area);
        frame.render_widget(initializing_text, initializing_text_area);
    }
    /// Renders the header panel that contains the key bindings.
    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let border_color =
            Color::from_str(&self.config.theme.panel_border_color).expect("valid RGB color");

        let menu_items_color =
            Color::from_str(&self.config.theme.menu_item_text_color).expect("valid RGB color");

        let selected_menu_item_color =
            Color::from_str(&self.config.theme.selected_menu_item_text_color)
                .expect("valid RGB color");

        let outer = Block::bordered()
            .border_style(border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let inner_area = outer.inner(area);

        let [left_panel, right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(inner_area);

        let mut selected_menu_item = 0;

        let mut menu_items = Vec::new();
        for i in 0..self.components.len() {
            let component = self.components.get(i).expect("valid index");

            if component
                .borrow()
                .name()
                .eq(self.state.active_component.borrow().name())
            {
                selected_menu_item = i;
            }

            menu_items.push(format!("{} [{}]", component.borrow().name(), i + 1));
        }

        let menu = Tabs::new(menu_items)
            .divider("|")
            .style(menu_items_color)
            .highlight_style(Style::default().underlined().fg(selected_menu_item_color))
            .select(selected_menu_item);

        frame.render_widget(menu, left_panel);
        frame.render_widget(outer, area);

        if let Some(notification) = self.state.notification.as_ref() {
            let notification_color = match notification.status {
                NotificationStatus::Success => {
                    Color::from_str(&self.config.theme.notification_text_color_success)
                        .expect("valid RGB color")
                }
                NotificationStatus::Warn => {
                    Color::from_str(&self.config.theme.notification_text_color_warn)
                        .expect("valid RGB color")
                }
                NotificationStatus::Failure => {
                    Color::from_str(&self.config.theme.notification_text_color_failure)
                        .expect("valid RGB color")
                }
            };

            let notification_text = Paragraph::new(notification.summary.as_str())
                .style(notification_color)
                .right_aligned();

            frame.render_widget(notification_text, right_panel);
        }
    }
    /// Renders the active [`Component`] to the screen.
    fn render_component(&self, frame: &mut Frame, area: Rect) {
        self.state.active_component.borrow_mut().render(frame, area);
    }
    /// Renders the footer widgets using the status and key bindings from the active [`Component`].
    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        let border_color =
            Color::from_str(&self.config.theme.panel_border_color).expect("valid RGB color");

        let outer = Block::bordered()
            .border_style(border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let inner_area = outer.inner(area);

        let [left_panel, right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(inner_area);

        let component = self.state.active_component.borrow();

        frame.render_widget(outer, area);
        component.render_status_line(frame, left_panel);
        component.render_key_bindings(frame, right_panel);
    }
}

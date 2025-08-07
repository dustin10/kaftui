mod notifications;
mod records;
mod stats;

pub use crate::ui::{
    notifications::{Notifications, NotificationsConfig},
    records::{Records, RecordsConfig},
    stats::{Stats, StatsConfig},
};

use crate::{
    app::{App, BufferedKeyPress, NotificationStatus},
    event::AppEvent,
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
    /// Returns a [`Paragraph`] that will be used to render the current status line.
    fn status_line(&self) -> Paragraph<'_> {
        Paragraph::default()
    }
    /// Returns a [`Paragraph`] that will be used to render the active key bindings.
    fn key_bindings(&self) -> Paragraph<'_> {
        Paragraph::default()
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`AppEvent`] which will be published
    /// for processing.
    fn map_key_event(
        &self,
        _event: KeyEvent,
        _buffered: Option<&BufferedKeyPress>,
    ) -> Option<AppEvent> {
        None
    }
    /// Allows the component to handle any [`AppEvent`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &AppEvent);
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect);
}

impl App {
    /// Draws the UI for the application to the given [`Frame`] based on the current screen the
    /// user is viewing.
    pub fn draw(&mut self, frame: &mut Frame) {
        if self.state.initializing {
            render_initialize(self, frame);
        } else {
            let full_screen = frame.area();

            let [header_area, component_area, footer_area] = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(1),
                    Constraint::Length(3),
                ])
                .areas(full_screen);

            render_header(self, frame, header_area);

            let mut component = self.state.active_component.borrow_mut();
            component.render(frame, component_area);

            let status_line = component.status_line();
            let key_bindings = component.key_bindings();

            render_footer(self, status_line, key_bindings, frame, footer_area);
        }
    }
}

/// Renders the UI to the terminal for the [`Screen::Initialize`] screen.
fn render_initialize(app: &App, frame: &mut Frame) {
    let border_color =
        Color::from_str(&app.config.theme.panel_border_color).expect("valid RGB color");

    let [empty_area, initializing_text_area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .areas(frame.area());

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

/// Renders the header panel that contains the key bindings.
fn render_header(app: &App, frame: &mut Frame, area: Rect) {
    let border_color =
        Color::from_str(&app.config.theme.panel_border_color).expect("valid RGB color");

    let menu_items_color =
        Color::from_str(&app.config.theme.menu_item_text_color).expect("valid RGB color");

    let selected_menu_item_color =
        Color::from_str(&app.config.theme.selected_menu_item_text_color).expect("valid RGB color");

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
    for i in 0..app.components.len() {
        let component = app.components.get(i).expect("valid index");

        if component
            .borrow()
            .name()
            .eq(app.state.active_component.borrow().name())
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

    if let Some(notification) = app.state.notification_history.borrow().front()
        && !notification.is_expired()
    {
        let notification_color = match notification.status {
            NotificationStatus::Success => {
                Color::from_str(&app.config.theme.notification_text_color_success)
                    .expect("valid RGB color")
            }
            NotificationStatus::Warn => {
                Color::from_str(&app.config.theme.notification_text_color_warn)
                    .expect("valid RGB color")
            }
            NotificationStatus::Failure => {
                Color::from_str(&app.config.theme.notification_text_color_failure)
                    .expect("valid RGB color")
            }
        };

        let notification_text = Paragraph::new(notification.summary.as_str())
            .style(notification_color)
            .right_aligned();

        frame.render_widget(notification_text, right_panel);
    }
}

/// Renders the footer widgets using the status and key bindings from the active [`Component`].
fn render_footer(app: &App, status: Paragraph, keys: Paragraph, frame: &mut Frame, area: Rect) {
    let border_color =
        Color::from_str(app.config.theme.panel_border_color.as_str()).expect("valid RGB color");

    let outer = Block::bordered()
        .border_style(border_color)
        .padding(Padding::new(1, 1, 0, 0));

    let inner_area = outer.inner(area);

    let [left_panel, right_panel] = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .areas(inner_area);

    frame.render_widget(outer, area);
    frame.render_widget(status, left_panel);
    frame.render_widget(keys, right_panel);
}

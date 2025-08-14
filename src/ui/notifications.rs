use crate::{
    app::{config::Theme, BufferedKeyPress, Notification, NotificationStatus},
    event::AppEvent,
    ui::Component,
};

use bounded_vec_deque::BoundedVecDeque;
use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    widgets::{
        Block, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table, TableState,
    },
    Frame,
};
use std::{cell::RefCell, rc::Rc, str::FromStr};

/// Key bindings that are displayed to the user in the footer when viewing the notification history
/// screen.
const NOTIFICATION_HISTORY_KEY_BINDINGS: [&str; 5] = [
    super::KEY_BINDING_QUIT,
    super::KEY_BINDING_TOP,
    super::KEY_BINDING_SCROLL_DOWN,
    super::KEY_BINDING_SCROLL_UP,
    super::KEY_BINDING_BOTTOM,
];

/// Manages state related to notifications displayed to the user and the UI around them.
#[derive(Debug)]
pub struct NotificationsState {
    /// Stores the history of [`Notification`]s displayed to the user.
    history: Rc<RefCell<BoundedVecDeque<Notification>>>,
    /// [`TableState`] for the table that notifications from the history list are rendered into.
    list_state: TableState,
    /// [`ScrollbarState`] for the table that notifications from the history list are rendered
    /// into.
    list_scroll_state: ScrollbarState,
}

impl NotificationsState {
    /// Creates a new [`NotificationsState`] using the specified shared reference to the history
    /// containing all [`Notification`]s that were presented to the user.
    fn new(history: Rc<RefCell<BoundedVecDeque<Notification>>>) -> Self {
        Self {
            history,
            list_state: TableState::default(),
            list_scroll_state: ScrollbarState::default(),
        }
    }
    /// Moves the notification history scroll state to the top.
    fn scroll_list_top(&mut self) {
        self.list_state.select_first();
        self.list_scroll_state = self.list_scroll_state.position(0);
    }
    /// Moves the notification history scroll state up by one line.
    fn scroll_list_up(&mut self) {
        if self.history.borrow().is_empty() {
            return;
        }

        self.list_state.select_previous();

        let idx = self.list_state.selected().expect("notification selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
    }
    /// Moves the notification history scroll state down by one line.
    fn scroll_list_down(&mut self) {
        if self.history.borrow().is_empty() {
            return;
        }

        if let Some(curr_idx) = self.list_state.selected()
            && curr_idx == self.history.borrow().len() - 1
        {
            return;
        }

        self.list_state.select_next();

        let idx = self.list_state.selected().expect("notification selected");

        self.list_scroll_state = self.list_scroll_state.position(idx);
    }
    /// Moves the notification history scroll state to the bottom.
    fn scroll_list_bottom(&mut self) {
        let bottom = self.history.borrow().len() - 1;

        self.list_state.select(Some(bottom));
        self.list_scroll_state = self.list_scroll_state.position(bottom);
    }
    /// Pushes a new [`Notification`] onto the current list when a new one is generated.
    fn push_notification(&mut self, notification: Notification) {
        self.history.borrow_mut().push_front(notification);

        if let Some(i) = self.list_state.selected().as_mut() {
            let new_idx = *i + 1;
            self.list_state.select(Some(new_idx));
            self.list_scroll_state = self.list_scroll_state.position(new_idx);
        }
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Notifications`]
/// component.
#[derive(Debug)]
struct NotificationsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for the status text while the Kafka consumer is active.
    status_text_color_processing: Color,
    /// Color used for the key bindings text. Defaults to white.
    key_bindings_text_color: Color,
    /// Color used for the text in a successful notification message.
    notification_text_color_success: Color,
    /// Color used for the text in a warning notification message.
    notification_text_color_warn: Color,
    /// Color used for the text in a failure notification message.
    notification_text_color_failure: Color,
}

impl From<&Theme> for NotificationsTheme {
    /// Converts a reference to a [`Theme`] to a new [`RecordsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let status_text_color_processing =
            Color::from_str(value.status_text_color_processing.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        let notification_text_color_success =
            Color::from_str(value.notification_text_color_success.as_str())
                .expect("valid RGB color");

        let notification_text_color_warn =
            Color::from_str(value.notification_text_color_warn.as_str()).expect("valid RGB color");

        let notification_text_color_failure =
            Color::from_str(value.notification_text_color_failure.as_str())
                .expect("valid RGB color");

        Self {
            panel_border_color,
            label_color,
            status_text_color_processing,
            key_bindings_text_color,
            notification_text_color_success,
            notification_text_color_warn,
            notification_text_color_failure,
        }
    }
}

/// Configuration used to create a new [`Notifications`] component.
#[derive(Debug, Builder)]
pub struct NotificationsConfig<'a> {
    /// Stores the history of [`Notification`]s displayed to the user.
    history: Rc<RefCell<BoundedVecDeque<Notification>>>,
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> NotificationsConfig<'a> {
    /// Creates a new default [`NotificationsConfigBuilder`] which can be used to create a new
    /// [`NotificationsConfig`].
    pub fn builder() -> NotificationsConfigBuilder<'a> {
        NotificationsConfigBuilder::default()
    }
}

/// The application [`Component`] that is responsible for displaying the [`Notification`]s
/// displayed to the user.
#[derive(Debug)]
pub struct Notifications {
    /// Current state of the component and it's underlying widgets.
    state: NotificationsState,
    /// Color scheme for the component.
    theme: NotificationsTheme,
}

impl Notifications {
    /// Creates a new [`Notifications`] component using the specified [`NotificationsConfig`].
    pub fn new(config: NotificationsConfig) -> Self {
        let state = NotificationsState::new(config.history);

        let theme = config.theme.into();

        Self { state, theme }
    }
}

impl Component for Notifications {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Notifications"
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let status_line = Paragraph::new(format!("Total: {}", self.state.history.borrow().len()))
            .style(self.theme.status_text_color_processing);

        frame.render_widget(status_line, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let text = Paragraph::new(NOTIFICATION_HISTORY_KEY_BINDINGS.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`AppEvent`] which will be published
    /// for processing.
    fn map_key_event(
        &self,
        event: KeyEvent,
        buffered: Option<&BufferedKeyPress>,
    ) -> Option<AppEvent> {
        match event.code {
            KeyCode::Char(c) => match c {
                'g' if buffered.map(|kp| kp.is('g')).is_some() => {
                    Some(AppEvent::ScrollNotificationsTop)
                }
                'j' => Some(AppEvent::ScrollNotificationsDown),
                'k' => Some(AppEvent::ScrollNotificationsUp),
                'G' => Some(AppEvent::ScrollNotificationsBottom),
                _ => None,
            },
            _ => None,
        }
    }
    /// Allows the [`Component`] to handle any [`AppEvent`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &AppEvent) {
        match event {
            AppEvent::ScrollNotificationsTop => self.state.scroll_list_top(),
            AppEvent::ScrollNotificationsUp => self.state.scroll_list_up(),
            AppEvent::ScrollNotificationsDown => self.state.scroll_list_down(),
            AppEvent::ScrollNotificationsBottom => self.state.scroll_list_bottom(),
            AppEvent::DisplayNotification(notification) => {
                self.state.push_notification(notification.clone())
            }
            _ => {}
        }
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let table_block = Block::bordered()
            .title(" Notifications ")
            .border_style(self.theme.panel_border_color)
            .padding(ratatui::widgets::Padding::new(1, 1, 0, 0));

        let table_rows: Vec<Row> = self
            .state
            .history
            .borrow()
            .iter()
            .map(|n| {
                let timestamp = n.created.to_string();
                let status = format!("{:?}", n.status);

                let color = match n.status {
                    NotificationStatus::Success => self.theme.notification_text_color_success,
                    NotificationStatus::Warn => self.theme.notification_text_color_warn,
                    NotificationStatus::Failure => self.theme.notification_text_color_failure,
                };

                Row::new([timestamp, status, n.summary.clone(), n.details.clone()]).style(color)
            })
            .collect();

        let table = Table::new(
            table_rows,
            [
                Constraint::Fill(2),
                Constraint::Fill(1),
                Constraint::Fill(3),
                Constraint::Fill(8),
            ],
        )
        .column_spacing(1)
        .header(Row::new([
            "Timestamp".bold().style(self.theme.label_color),
            "Status".bold().style(self.theme.label_color),
            "Summary".bold().style(self.theme.label_color),
            "Details".bold().style(self.theme.label_color),
        ]))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .block(table_block);

        frame.render_stateful_widget(table, area, &mut self.state.list_state);

        self.state.list_scroll_state = self
            .state
            .list_scroll_state
            .content_length(self.state.history.borrow().len());

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

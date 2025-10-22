use crate::{
    app::config::{Config, Theme},
    kafka::SeekTo,
    ui::{BufferedKeyPress, Component, Event},
};

use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, HighlightSpacing, List, ListItem, ListState, Padding, Paragraph, Row, Table,
    },
};
use std::str::FromStr;
use std::{ops::Deref, rc::Rc};

/// Key bindings that are always displayed to the user in the footer when viewing the settings
/// screen.
const SETTINGS_KEY_BINDINGS: [&str; 2] = [super::KEY_BINDING_QUIT, super::KEY_BINDING_CHANGE_FOCUS];

/// Enumerates the widgets that can be focused in the [`Settings`] component.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum SettingsWidget {
    #[default]
    Menu,
}

/// Enumerates the items available for selection in the sidebar menu.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum SettingsMenuItem {
    /// When selected, the active application configuration will be displayed to the user.
    #[default]
    Active,
    /// When selected, the profile viewer will be displayed to the user where they can view
    /// any configured application profiles.
    Profile,
}

impl From<usize> for SettingsMenuItem {
    /// Converts a `usize` index to a corresponding [`SettingsMenuItem`].
    ///
    /// # Panics
    ///
    /// This function will panic if the index provided does not correspond to a valid menu item.
    fn from(value: usize) -> Self {
        match value {
            0 => SettingsMenuItem::Active,
            1 => SettingsMenuItem::Profile,
            _ => panic!("invalid settings menu item index"),
        }
    }
}

/// Configuration used to create a new [`Settings`] component.
#[derive(Builder, Debug)]
pub struct SettingsConfig<'a> {
    /// The current application [`Config`] that will be displayed to the user.
    config: Rc<Config>,
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> SettingsConfig<'a> {
    /// Creates new default [`SettingsConfigBuilder`].
    pub fn builder() -> SettingsConfigBuilder<'a> {
        SettingsConfigBuilder::default()
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the
/// [`Settings`] component.
#[derive(Debug)]
struct SettingsTheme {
    /// Color used for the borders of the main info panels. Defaults to white.
    panel_border_color: Color,
    /// Color used for the borders of the selected info panel. Defaults to cyan.
    selected_panel_border_color: Color,
    /// Color used for the status text while the Kafka consumer is active. Defaults to green.
    status_text_color_processing: Color,
    /// Color used for the status text while the Kafka consumer is paused. Defaults to red.
    status_text_color_paused: Color,
    /// Color used for the key bindings text. Defaults to white.
    key_bindings_text_color: Color,
    /// Color used for the label text in tables, etc. Defaults to white.
    label_color: Color,
    /// Color used for the text in the record list. Defaults to white.
    record_list_text_color: Color,
    /// Color used for the text in the record info. Defaults to white.
    record_info_text_color: Color,
    /// Color used for the text in the record value. Defaults to white.
    record_value_text_color: Color,
    /// Color used for the text in the record headers. Defaults to white.
    record_headers_text_color: Color,
    /// Color used for the text in the menu items. Defaults to white.
    menu_item_text_color: Color,
    /// Color used for the text in the currently selected menu item. Defaults to yellow.
    selected_menu_item_text_color: Color,
    /// Color used for the text in a successful notification message. Defaults to green.
    notification_text_color_success: Color,
    /// Color used for the text in a warning notification message. Defaults to yellow.
    notification_text_color_warn: Color,
    /// Color used for the text in an unsuccessful notification message. Defaults to red.
    notification_text_color_failure: Color,
    /// Color used for the text in the stats UI. Defaults to white.
    stats_text_color: Color,
    /// Primary color used for bars in a bar graph in the stats UI. Defaults to white.
    stats_bar_color: Color,
    /// Secondary color used for bars in a bar graph in the stats UI. Defaults to white.
    stats_bar_secondary_color: Color,
    /// Color used for the throughput chart in the stats UI. Defaults to white.
    stats_throughput_color: Color,
}

impl From<&Theme> for SettingsTheme {
    /// Converts a reference to a [`Theme`] to a new [`LogsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let selected_panel_border_color =
            Color::from_str(value.selected_panel_border_color.as_str()).expect("valid RGB hex");

        let status_text_color_processing =
            Color::from_str(value.status_text_color_processing.as_str()).expect("valid RGB hex");

        let status_text_color_paused =
            Color::from_str(value.status_text_color_paused.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let record_list_text_color =
            Color::from_str(value.record_list_text_color.as_str()).expect("valid RGB hex");

        let record_info_text_color =
            Color::from_str(value.record_info_text_color.as_str()).expect("valid RGB hex");

        let record_value_text_color =
            Color::from_str(value.record_value_text_color.as_str()).expect("valid RGB hex");

        let record_headers_text_color =
            Color::from_str(value.record_headers_text_color.as_str()).expect("valid RGB hex");

        let menu_item_text_color =
            Color::from_str(value.menu_item_text_color.as_str()).expect("valid RGB hex");

        let selected_menu_item_text_color =
            Color::from_str(value.selected_menu_item_text_color.as_str()).expect("valid RGB hex");

        let notification_text_color_success =
            Color::from_str(value.notification_text_color_success.as_str()).expect("valid RGB hex");

        let notification_text_color_warn =
            Color::from_str(value.notification_text_color_warn.as_str()).expect("valid RGB hex");

        let notification_text_color_failure =
            Color::from_str(value.notification_text_color_failure.as_str()).expect("valid RGB hex");

        let stats_text_color =
            Color::from_str(value.stats_text_color.as_str()).expect("valid RGB hex");

        let stats_bar_color =
            Color::from_str(value.stats_bar_color.as_str()).expect("valid RGB hex");

        let stats_bar_secondary_color =
            Color::from_str(value.stats_bar_secondary_color.as_str()).expect("valid RGB hex");

        let stats_throughput_color =
            Color::from_str(value.stats_throughput_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            selected_panel_border_color,
            status_text_color_processing,
            status_text_color_paused,
            key_bindings_text_color,
            label_color,
            record_list_text_color,
            record_info_text_color,
            record_value_text_color,
            record_headers_text_color,
            menu_item_text_color,
            selected_menu_item_text_color,
            notification_text_color_success,
            notification_text_color_warn,
            notification_text_color_failure,
            stats_text_color,
            stats_bar_color,
            stats_bar_secondary_color,
            stats_throughput_color,
        }
    }
}

/// Manages state related to settings and the UI that renders them to the user.
#[derive(Debug, Default)]
struct SettingsState {
    /// The widget that currently has focus in the component.
    active_widget: SettingsWidget,
    /// Contains the current state of the sidebar menu list.
    menu_list_state: ListState,
}

impl SettingsState {
    /// Gets the currently selected [`SettingsMenuItem`].
    fn selected_menu_item(&self) -> SettingsMenuItem {
        self.menu_list_state
            .selected()
            .map(Into::into)
            .unwrap_or_default()
    }
    /// Selects the first menu item in the list.
    fn select_menu_item_top(&mut self) {
        self.menu_list_state.select_first();
    }
    /// Selects the next menu item in the list.
    fn select_menu_item_next(&mut self) {
        self.menu_list_state.select_next();
    }
    /// Selects the previous menu item in the list.
    fn select_menu_item_prev(&mut self) {
        self.menu_list_state.select_previous();
    }
    /// Selects the last menu item in the list.
    fn select_menu_item_bottom(&mut self) {
        self.menu_list_state.select_last();
    }
}

/// The application [`Component`] that is responsible for displaying the current application
/// configuration to the user as JSON. This is primarily useful for debugging purposes to see what
/// the runtime configuration resolved to.
#[derive(Debug)]
pub struct Settings {
    /// Contains the internal state of the component.
    state: SettingsState,
    /// The current application [`Config`] that will be displayed to the user.
    config: Rc<Config>,
    /// Color scheme for the component.
    theme: SettingsTheme,
}

impl Settings {
    /// Creates a new [`Settings`] component using the specified [`SettingsConfig`].
    pub fn new(config: SettingsConfig<'_>) -> Self {
        let theme = config.theme.into();

        let mut state = SettingsState::default();
        state.menu_list_state.select_first();

        Self {
            state,
            config: config.config,
            theme,
        }
    }
    /// Renders the sidebar menu panel.
    fn render_sidebar(&mut self, frame: &mut Frame, area: Rect) {
        let mut menu_block = Block::bordered()
            .title(" Options ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == SettingsWidget::Menu {
            menu_block = menu_block.border_style(self.theme.selected_panel_border_color);
        }

        let menu_list_items = vec![ListItem::new("Active"), ListItem::new("Profiles")];

        let menu_list = List::new(menu_list_items)
            .block(menu_block)
            .highlight_style(Modifier::REVERSED)
            .highlight_symbol(">")
            .highlight_spacing(HighlightSpacing::Always);

        frame.render_stateful_widget(menu_list, area, &mut self.state.menu_list_state);
    }
    /// Renders the main panel based on the currently selected menu item.
    fn render_main_panel(&self, frame: &mut Frame, area: Rect) {
        match self.state.selected_menu_item() {
            SettingsMenuItem::Active => self.render_active_config(frame, area),
            SettingsMenuItem::Profile => self.render_profiles(frame, area),
        }
    }
    /// Renders the current applcation configuration to the main panel.
    fn render_active_config(&self, frame: &mut Frame, area: Rect) {
        let [left_panel, middle_panel, right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),
                Constraint::Fill(1),
                Constraint::Fill(1),
            ])
            .areas(area);

        let [left_top_panel, left_bottom_panel] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(left_panel);

        let [middle_top_panel, middle_bottom_panel] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(middle_panel);

        self.render_active_config_consumer(frame, left_top_panel);
        self.render_active_config_consumer_properties(frame, left_bottom_panel);
        self.render_active_config_schema(frame, middle_top_panel);
        self.render_active_config_misc(frame, middle_bottom_panel);
        self.render_active_config_theme(frame, right_panel);
    }
    /// Renders the active consumer configuration for the application.
    fn render_active_config_consumer(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(" Consumer ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let config = self.config.deref();

        let seek_to = match &config.seek_to {
            SeekTo::None => String::from("<none>"),
            SeekTo::Reset => String::from("RESET"),
            SeekTo::Custom(pos) => pos
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(", "),
        };

        let list_items = vec![
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Bootstrap Servers", self.theme.label_color)),
                Line::from(config.bootstrap_servers.clone()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Topic", self.theme.label_color)),
                Line::from(config.topic.clone()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Partitions", self.theme.label_color)),
                Line::from(
                    config
                        .partitions
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("ALL")),
                ),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Group ID", self.theme.label_color)),
                Line::from(config.group_id.clone()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Filter", self.theme.label_color)),
                Line::from(
                    config
                        .filter
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("<none>")),
                ),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Seek To", self.theme.label_color)),
                Line::from(seek_to),
            ])),
        ];

        let list = List::new(list_items).block(block);

        frame.render_widget(list, area);
    }
    /// Renders a table containing the consumer properties if any are configured.
    fn render_active_config_consumer_properties(&self, frame: &mut Frame, area: Rect) {
        let config = self.config.deref();

        if let Some(consumer_properties) = config.consumer_properties.as_ref() {
            let props_block = Block::bordered()
                .title(" Consumer Properties ")
                .border_style(self.theme.panel_border_color)
                .padding(Padding::new(1, 1, 0, 0));

            let props_rows: Vec<Row> = consumer_properties
                .iter()
                .map(|(k, v)| Row::new([k.as_str(), v.as_str()]))
                .collect();

            let props_table = Table::new(props_rows, [Constraint::Min(1), Constraint::Fill(3)])
                .column_spacing(1)
                .header(Row::new([
                    "Key".bold().style(self.theme.label_color),
                    "Value".bold().style(self.theme.label_color),
                ]))
                .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
                .block(props_block);

            frame.render_widget(props_table, area);
        } else {
            self.render_message(frame, area, "¯\\_(ツ)_/¯", Some(" Consumer Properties "));
        }
    }
    /// Renders the given message centered both vertically and horizontally in the given area.
    fn render_message(&self, frame: &mut Frame, area: Rect, msg: &str, title: Option<&str>) {
        let [empty_area, text_area] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

        let empty_text = Paragraph::default().block(
            Block::default()
                .title(title.unwrap_or_default().to_string())
                .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
                .border_style(self.theme.panel_border_color),
        );

        let message_block = Block::default()
            .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
            .border_style(self.theme.panel_border_color);

        let message_text = Paragraph::new(msg)
            .style(self.theme.panel_border_color)
            .block(message_block)
            .centered();

        frame.render_widget(empty_text, empty_area);
        frame.render_widget(message_text, text_area);
    }
    /// Renders the schema related configuration for the application.
    fn render_active_config_schema(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(" Schema ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let config = self.config.deref();

        let list_items = vec![
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Format", self.theme.label_color)),
                Line::from(config.format.to_string().to_uppercase()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Registry URL", self.theme.label_color)),
                Line::from(
                    config
                        .schema_registry_url
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("<none>")),
                ),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Registry Auth Token", self.theme.label_color)),
                Line::from(
                    config
                        .schema_registry_bearer_token
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("<none>")),
                ),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled(
                    "Registry Basic Auth User",
                    self.theme.label_color,
                )),
                Line::from(
                    config
                        .schema_registry_user
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("<none>")),
                ),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled(
                    "Registry Basic Auth Password",
                    self.theme.label_color,
                )),
                Line::from(
                    config
                        .schema_registry_pass
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| String::from("<none>")),
                ),
            ])),
        ];

        let list = List::new(list_items).block(block);

        frame.render_widget(list, area);
    }
    /// Renders the miscellaneous configuration for the application.
    fn render_active_config_misc(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(" Misc ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let config = self.config.deref();

        let list_items = vec![
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Export Directory", self.theme.label_color)),
                Line::from(config.export_directory.clone()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Enable Logs", self.theme.label_color)),
                Line::from(config.logs_enabled.to_string()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Max Records", self.theme.label_color)),
                Line::from(config.max_records.to_string()),
            ])),
            ListItem::new(""),
            ListItem::new(Text::from_iter([
                Line::from(Span::styled("Scroll Factory", self.theme.label_color)),
                Line::from(config.scroll_factor.to_string()),
            ])),
        ];

        let list = List::new(list_items).block(block);

        frame.render_widget(list, area);
    }
    /// Renders the current theme configuratio for the application.
    fn render_active_config_theme(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(" Theme ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let list_items = vec![
            ListItem::new(Text::from(Span::styled(
                "Panel Border",
                self.theme.panel_border_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Selected Panel Border",
                self.theme.selected_panel_border_color,
            ))),
            ListItem::new(Text::from(Span::styled("Label", self.theme.label_color))),
            ListItem::new(Text::from(Span::styled(
                "Key Bindings",
                self.theme.key_bindings_text_color,
            ))),
            ListItem::new(""),
            ListItem::new(Text::from(Span::styled(
                "Consumer Status Processing",
                self.theme.status_text_color_processing,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Consumer Status Paused",
                self.theme.status_text_color_paused,
            ))),
            ListItem::new(""),
            ListItem::new(Text::from(Span::styled(
                "Menu Item",
                self.theme.menu_item_text_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Selected Menu Item",
                self.theme.selected_menu_item_text_color,
            ))),
            ListItem::new(""),
            ListItem::new(Text::from(Span::styled(
                "Records List",
                self.theme.record_list_text_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Record Info",
                self.theme.record_info_text_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Record Headers",
                self.theme.record_headers_text_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Record Value",
                self.theme.record_value_text_color,
            ))),
            ListItem::new(""),
            ListItem::new(Text::from(Span::styled(
                "Notification Success",
                self.theme.notification_text_color_success,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Notification Warn",
                self.theme.notification_text_color_warn,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Notification Failure",
                self.theme.notification_text_color_failure,
            ))),
            ListItem::new(""),
            ListItem::new(Text::from(Span::styled(
                "Stats",
                self.theme.stats_text_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Stats Bar Primary",
                self.theme.stats_bar_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Stats Bar Secondary",
                self.theme.stats_bar_secondary_color,
            ))),
            ListItem::new(Text::from(Span::styled(
                "Stats Throughput",
                self.theme.stats_throughput_color,
            ))),
        ];

        let list = List::new(list_items).block(block);

        frame.render_widget(list, area);
    }
    /// Renders the prorfile viewer to the main panel.
    fn render_profiles(&self, frame: &mut Frame, area: Rect) {
        self.render_message(frame, area, "Under Construction", Some(" Profile Manager "));
    }
}

impl Component for Settings {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Settings"
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &mut self,
        event: KeyEvent,
        buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        match event.code {
            KeyCode::Char(c) => match self.state.active_widget {
                SettingsWidget::Menu => match c {
                    'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                        self.state.select_menu_item_top();
                        None
                    }
                    'j' => {
                        self.state.select_menu_item_next();
                        None
                    }
                    'k' => {
                        self.state.select_menu_item_prev();
                        None
                    }
                    'G' => {
                        self.state.select_menu_item_bottom();
                        None
                    }
                    _ => None,
                },
            },
            _ => None,
        }
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let [left_panel, right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(80)])
            .areas(area);

        self.render_sidebar(frame, left_panel);
        self.render_main_panel(frame, right_panel);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let mut key_bindings = Vec::from(SETTINGS_KEY_BINDINGS);

        match self.state.active_widget {
            SettingsWidget::Menu => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_NEXT);
                key_bindings.push(super::KEY_BINDING_PREV);
                key_bindings.push(super::KEY_BINDING_BOTTOM);
            }
        }

        let text = Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
}

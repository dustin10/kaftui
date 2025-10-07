use crate::{
    app::config::{Config, Theme},
    ui::Component,
};

use derive_builder::Builder;
use ratatui::{
    layout::Rect,
    style::Color,
    widgets::{Block, Paragraph, Wrap},
    Frame,
};
use std::rc::Rc;
use std::{ops::Deref, str::FromStr};

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
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
}

impl From<&Theme> for SettingsTheme {
    /// Converts a reference to a [`Theme`] to a new [`LogsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        Self { panel_border_color }
    }
}

/// The application [`Component`] that is responsible for displaying the current application
/// configuration to the user as JSON. This is primarily useful for debugging purposes to see what
/// the runtime configuration resolved to.
#[derive(Debug)]
pub struct Settings {
    /// The current application [`Config`] that will be displayed to the user.
    config: Rc<Config>,
    /// Color scheme for the component.
    theme: SettingsTheme,
}

impl Settings {
    /// Creates a new [`Settings`] component using the specified [`SettingsConfig`].
    pub fn new(config: SettingsConfig<'_>) -> Self {
        let theme = config.theme.into();

        Self {
            config: config.config,
            theme,
        }
    }
}

impl Component for Settings {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Settings"
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let config_block = Block::bordered()
            .title(" Application Settings ")
            .border_style(self.theme.panel_border_color)
            .padding(ratatui::widgets::Padding::new(1, 1, 0, 0));

        let config_json =
            serde_json::to_string_pretty(self.config.deref()).expect("config can be deserialized");

        let config_paragraph = Paragraph::new(config_json)
            .block(config_block)
            .wrap(Wrap { trim: false });

        frame.render_widget(config_paragraph, area);
    }
}

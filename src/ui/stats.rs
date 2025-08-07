use crate::{app::config::Theme, event::AppEvent, ui::Component};

use derive_builder::Builder;
use ratatui::{layout::Rect, style::Color, Frame};
use std::str::FromStr;

/// Manages state related to application statistics and the UI that renders them to the user.
#[derive(Debug, Default)]
struct StatsState {
    /// Count of the Kafka records that were consumed from the topic, were not filtered and
    /// presented to the user.
    received: u64,
    /// Count of the Kafka records that were consumed from the topic, but filtered.
    filtered: u64,
}

#[derive(Debug)]
struct StatsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the borders of the selected info panel.
    selected_panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
}

impl From<&Theme> for StatsTheme {
    /// Converts a reference to a [`Theme`] to a new [`StatsTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let selected_panel_border_color =
            Color::from_str(value.selected_panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            selected_panel_border_color,
            label_color,
        }
    }
}

/// Configuration used to create a new [`Stats`] component.
#[derive(Builder, Debug)]
pub struct StatsConfig<'a> {
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> StatsConfig<'a> {
    /// Creates a new default [`StatsConfigBuilder`] which can be used to create a new
    /// [`StatsConfig`].
    pub fn builder() -> StatsConfigBuilder<'a> {
        StatsConfigBuilder::default()
    }
}

/// The application [`Component`] that is responsible for displaying the statistics gathered by the
/// application while consuming records from the Kafka topic.
#[derive(Debug)]
pub struct Stats {
    /// Current state of the component and it's underlying widgets.
    state: StatsState,
    /// Color scheme for the component.
    theme: StatsTheme,
}

impl Stats {
    /// Creates a new [`Stats`] component using the specified [`StatsConfig`].
    pub fn new(config: StatsConfig) -> Self {
        let state = StatsState::default();

        let theme = config.theme.into();

        Self { state, theme }
    }
}

impl Component for Stats {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Stats"
    }
    /// Allows the component to handle any [`AppEvent`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &AppEvent) {
        // TODO: filtered count
        if let AppEvent::RecordReceived(_) = event {
            // TODO: received per partition
            self.state.received += 1;
        }
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, _frame: &mut Frame, _area: Rect) {}
}

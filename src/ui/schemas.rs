use crate::{app::config::Theme, event::Event, ui::Component};

use derive_builder::Builder;
use ratatui::{Frame, layout::Rect, style::Color};
use std::str::FromStr;

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Schemas`]
/// component.
#[derive(Debug)]
struct SchemasTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for the key bindings text. Defaults to white.
    key_bindings_text_color: Color,
}

impl From<&Theme> for SchemasTheme {
    /// Converts a reference to a [`Theme`] to a new [`SchemasTheme`].
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            label_color,
            key_bindings_text_color,
        }
    }
}

/// Configuration used to create a new [`Schemas`] component.
#[derive(Builder, Debug)]
pub struct SchemasConfig<'a> {
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> SchemasConfig<'a> {
    /// Creates a new default [`SchemasConfigBuilder`] which can be used to create a new
    /// [`Schemas`].
    pub fn builder() -> SchemasConfigBuilder<'a> {
        SchemasConfigBuilder::default()
    }
}

#[derive(Debug)]
pub struct Schemas {
    theme: SchemasTheme,
}

impl Schemas {
    pub fn new(config: SchemasConfig) -> Self {
        Self {
            theme: config.theme.into(),
        }
    }
}

impl Component for Schemas {
    fn name(&self) -> &'static str {
        "Schemas"
    }
    fn render(&mut self, _frame: &mut Frame, _area: Rect) {}
    fn on_app_event(&mut self, _event: &Event) {}
    fn render_key_bindings(&self, _frame: &mut Frame, _area: Rect) {}
}

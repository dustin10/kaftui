use crate::{app::config::Theme, event::AppEvent, kafka::Record, ui::Component};

use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Bar, BarChart, BarGroup, Block, Padding, Paragraph},
    Frame,
};
use std::{collections::BTreeMap, str::FromStr};

/// Manages state related to application statistics and the UI that renders them to the user.
#[derive(Debug, Default)]
struct StatsState {
    /// Count of the Kafka records that were consumed from the topic, were not filtered and
    /// presented to the user.
    received: u64,
    /// Count of the Kafka records that were consumed from the topic, but filtered out and not
    /// presented to the user.
    filtered: u64,
    /// A [`BTreeMap`] containing the total number of [`Records`]s consumed from the Kafka topic
    /// split up by partition number.
    partition_totals: BTreeMap<i32, u64>,
}

impl StatsState {
    /// Computes the total number of records consumed. The total is sum of the number of records
    /// recieved and the number of records filtered.
    fn total(&self) -> u64 {
        self.received + self.filtered
    }
    /// Invoked when a new [`Record`] is received from the Kafka consumer. At this point the
    /// [`Record`] has already passed the filtering process.
    fn on_record_received(&mut self, record: &Record) {
        self.received += 1;
        self.inc_total(record);
    }
    /// Invoked when a [`Record`] received from the Kafka consumer is filtered.
    fn on_record_filtered(&mut self, record: &Record) {
        self.filtered += 1;
        self.inc_total(record);
    }
    /// Increments the total number of [`Record`]s consumed on a partition.
    fn inc_total(&mut self, record: &Record) {
        self.partition_totals
            .entry(record.partition)
            .and_modify(|t| *t += 1)
            .or_insert(1);
    }
}

#[derive(Debug)]
struct StatsTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the label text in tables, etc.
    label_color: Color,
    /// Color used for normal text.
    text_color: Color,
    /// Primary color used for bars in a bar graph.
    bar_color: Color,
    /// Secondary color used for bars in a bar graph.
    bar_secondary_color: Color,
}

impl From<&Theme> for StatsTheme {
    /// Converts a reference to a [`Theme`] to a new [`StatsTheme`].
    ///
    /// # Panics
    ///
    /// If any of the hex RGB strings contained in the [`Theme`] are not in the valid format then a
    /// panic will occur.
    fn from(value: &Theme) -> Self {
        let panel_border_color =
            Color::from_str(value.panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let text_color = Color::from_str(value.stats_text_color.as_str()).expect("valid RGB hex");

        let bar_color = Color::from_str(value.stats_bar_color.as_str()).expect("valid RGB hex");

        let bar_secondary_color =
            Color::from_str(value.stats_bar_secondary_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            label_color,
            text_color,
            bar_color,
            bar_secondary_color,
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
    /// Renders the count of records received, filtered and the total.
    fn render_triptych(&self, frame: &mut Frame, area: Rect) {
        let [received_panel, filtered_panel, total_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),
                Constraint::Fill(1),
                Constraint::Fill(1),
            ])
            .areas(area);

        let received_block = Block::bordered()
            .title(" Received ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let received_paragraph = Paragraph::new(self.state.received.to_string())
            .block(received_block)
            .style(self.theme.text_color)
            .centered();

        let filtered_block = Block::bordered()
            .title(" Filtered ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let filtered_paragraph = Paragraph::new(self.state.filtered.to_string())
            .block(filtered_block)
            .style(self.theme.text_color)
            .centered();

        let total_block = Block::bordered()
            .title(" Total ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let total_paragraph = Paragraph::new(self.state.total().to_string())
            .block(total_block)
            .style(self.theme.text_color)
            .centered();

        frame.render_widget(received_paragraph, received_panel);
        frame.render_widget(filtered_paragraph, filtered_panel);
        frame.render_widget(total_paragraph, total_panel);
    }
    /// Renders the various charts for the stats UI.
    fn render_charts(&self, frame: &mut Frame, area: Rect) {
        let charts_block = Block::bordered()
            .title(" Total Per Partition ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let per_partition_bars: Vec<Bar> = self
            .state
            .partition_totals
            .iter()
            .enumerate()
            .map(|(i, (p, t))| {
                let style: Style = if i % 2 == 0 {
                    self.theme.bar_color.into()
                } else {
                    self.theme.bar_secondary_color.into()
                };

                Bar::default()
                    .value(*t)
                    .text_value(t.to_string())
                    .label(Line::from(format!("P{}", p)).style(self.theme.label_color))
                    .style(style)
                    .value_style(style.reversed())
            })
            .collect();

        // TODO: determine bar_width and bar_gap based on the area passed in and the number of
        // partitions that will be rendered to the chart

        let per_partition_chart = BarChart::default()
            .data(BarGroup::default().bars(&per_partition_bars))
            .bar_width(8)
            .bar_gap(2)
            .block(charts_block);

        frame.render_widget(per_partition_chart, area);
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
        match event {
            AppEvent::RecordReceived(record) => self.state.on_record_received(record),
            AppEvent::RecordFiltered(record) => self.state.on_record_filtered(record),
            _ => {}
        }
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        let [triptych_panel, charts_panel] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Max(3), Constraint::Min(1)])
            .areas(area);

        self.render_triptych(frame, triptych_panel);

        self.render_charts(frame, charts_panel);
    }
}

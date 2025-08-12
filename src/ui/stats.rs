use crate::{app::config::Theme, event::AppEvent, kafka::Record, ui::Component};

use bounded_vec_deque::BoundedVecDeque;
use chrono::Utc;
use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::Marker,
    text::Line,
    widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Chart, Dataset, GraphType, Padding, Paragraph,
    },
    Frame,
};
use std::{collections::BTreeMap, str::FromStr};

/// Number of columns to render between bars in a bar chart.
const BAR_GAP: u16 = 2;

/// Minimum width of a bar in the total per partition chart that is allowed to display the
/// percentage of total records alongside the total count.
const MIN_BAR_WIDTH_FOR_PERCENTAGE: u16 = 14;

/// Maximum number of timestamps corresponding to recrods being consumed from the Kafka topic that
/// will be kept in memory at any given time to be evaluated for the throughput chart.
const MAX_THROUGHPUT_CAPTURE: usize = 4096;

/// Manages state related to application statistics and the UI that renders them to the user.
#[derive(Debug)]
struct StatsState {
    /// Count of the Kafka records that were consumed from the topic, were not filtered and
    /// presented to the user.
    received: u64,
    /// Count of the Kafka records that were consumed from the topic, but filtered out and not
    /// presented to the user.
    filtered: u64,
    /// A [`BTreeMap`] containing the total number of [`Records`]s consumed from the Kafka topic
    /// split up by partition number. This type of map is used to keep the partitions ordered for
    /// display in the chart.
    partition_totals: BTreeMap<i32, u64>,
    /// Contains the timestamps corresponding to when [`Record`]s were consumed from the Kafka
    /// topic. These timestamps are used to display the throughput chart.
    timestamps: BoundedVecDeque<i64>,
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
        self.push_timestamp();
        self.inc_total_for_partition(record.partition);
    }
    /// Invoked when a [`Record`] received from the Kafka consumer is filtered.
    fn on_record_filtered(&mut self, record: &Record) {
        self.filtered += 1;
        self.push_timestamp();
        self.inc_total_for_partition(record.partition);
    }
    /// Increments the total number of [`Record`]s consumed on a partition.
    fn inc_total_for_partition(&mut self, partition: i32) {
        self.partition_totals
            .entry(partition)
            .and_modify(|t| *t += 1)
            .or_insert(1);
    }
    fn push_timestamp(&mut self) {
        self.timestamps.push_front(Utc::now().timestamp_millis());
    }
}

impl Default for StatsState {
    fn default() -> Self {
        Self {
            received: u64::default(),
            filtered: u64::default(),
            partition_totals: BTreeMap::default(),
            timestamps: BoundedVecDeque::new(MAX_THROUGHPUT_CAPTURE),
        }
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
    /// Color used for the throughput chart.
    throughput_color: Color,
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

        let throughput_color =
            Color::from_str(value.stats_throughput_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            label_color,
            text_color,
            bar_color,
            bar_secondary_color,
            throughput_color,
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
            .bold()
            .centered();

        let filtered_block = Block::bordered()
            .title(" Filtered ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let filtered_paragraph = Paragraph::new(self.state.filtered.to_string())
            .block(filtered_block)
            .style(self.theme.text_color)
            .bold()
            .centered();

        let total_block = Block::bordered()
            .title(" Total ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let total_paragraph = Paragraph::new(self.state.total().to_string())
            .block(total_block)
            .style(self.theme.text_color)
            .bold()
            .centered();

        frame.render_widget(received_paragraph, received_panel);
        frame.render_widget(filtered_paragraph, filtered_panel);
        frame.render_widget(total_paragraph, total_panel);
    }
    /// Renders the various charts for the stats UI.
    fn render_charts(&self, frame: &mut Frame, area: Rect) {
        let [top_panel, bottom_panel] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

        let [top_left_panel, top_right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(top_panel);

        let [_bottom_left_panel, _bottom_right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(bottom_panel);

        self.render_throughput(frame, top_left_panel);
        self.render_total_by_partition(frame, top_right_panel);
    }
    /// Renders the chart that displays the total throughput of records being consumed from the
    /// Kafka topic per second.
    fn render_throughput(&self, frame: &mut Frame, area: Rect) {
        let throughput_block = Block::bordered()
            .title(" Records/Second ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let now = Utc::now();
        let now_secs = now.timestamp_millis() / 1000;

        let mut partitioned: BTreeMap<u32, u32> = BTreeMap::new();
        for timestamp in self.state.timestamps.iter() {
            let timestamp_secs = timestamp / 1000;

            let seconds_past = now_secs - timestamp_secs;

            partitioned
                .entry(seconds_past as u32)
                .and_modify(|t| *t += 1)
                .or_insert(1);
        }

        let max = match partitioned.values().max() {
            Some(m) => *m,
            None => 0,
        };

        let data: Vec<(f64, f64)> = partitioned
            .into_iter()
            .map(|(secs_ago, total)| {
                let x = secs_ago.abs_diff(area.width as u32) as f64;
                let y = total as f64;
                (x, y)
            })
            .collect();

        let data_set = Dataset::default()
            .marker(Marker::HalfBlock)
            .style(self.theme.throughput_color)
            .graph_type(GraphType::Bar)
            .data(&data);

        let now_time = now.format("%H:%M:%S").to_string();

        let past_min = (area.width as f32 / 60.0).ceil();

        let x_axis = Axis::default()
            .style(self.theme.text_color)
            .labels([format!("-{}m", past_min).bold(), now_time.bold()])
            .bounds([0.0, area.width as f64]);

        let y_axis = Axis::default()
            .style(self.theme.text_color)
            .bounds([0.0, max as f64])
            .labels(["0".bold(), max.to_string().bold()]);

        let throughput_chart = Chart::new(vec![data_set])
            .block(throughput_block)
            .x_axis(x_axis)
            .y_axis(y_axis);

        frame.render_widget(throughput_chart, area);
    }
    /// Renders the bar chart that displays the total records consumed from the Kafka topic per
    /// partition.
    fn render_total_by_partition(&self, frame: &mut Frame, area: Rect) {
        let charts_block = Block::bordered()
            .title(" Total Per Partition ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let bar_width =
            calculate_bar_width(&area, self.state.partition_totals.len() as u16, BAR_GAP);

        let per_partition_bars: Vec<Bar> = self
            .state
            .partition_totals
            .iter()
            .enumerate()
            .map(|(i, (partition, total))| {
                let style: Style = if i % 2 == 0 {
                    self.theme.bar_color.into()
                } else {
                    self.theme.bar_secondary_color.into()
                };

                let text_value = if bar_width > MIN_BAR_WIDTH_FOR_PERCENTAGE {
                    let percentage = (*total as f32 / self.state.total() as f32) * 100.0;
                    format!("{} ({:.1}%)", total, percentage)
                } else {
                    format!("{}", total)
                };

                Bar::default()
                    .value(*total)
                    .text_value(text_value)
                    .label(Line::from(format!("P{}", partition)).style(self.theme.label_color))
                    .style(style)
                    .value_style(style.reversed())
            })
            .collect();

        let per_partition_chart = BarChart::default()
            .data(BarGroup::default().bars(&per_partition_bars))
            .bar_width(bar_width)
            .bar_gap(BAR_GAP)
            .block(charts_block);

        frame.render_widget(per_partition_chart, area);
    }
}

/// Calculates the bar width based on total number of partitions, gap between bars and the
/// width of the available area.
fn calculate_bar_width(area: &Rect, num_bars: u16, bar_gap: u16) -> u16 {
    if num_bars == 0 {
        return 1;
    }

    let total_gap = (num_bars + 1) * bar_gap;

    (area.width - total_gap) / num_bars
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

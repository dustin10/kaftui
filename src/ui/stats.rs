use crate::{
    app::{config::Theme, BufferedKeyPress},
    event::Event,
    kafka::{ConsumerMode, Record},
    ui::{widget::ConsumerStatusLine, Component},
};

use bounded_vec_deque::BoundedVecDeque;
use chrono::{Duration, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::Marker,
    text::{Line, Span, ToSpan},
    widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Borders, Chart, Dataset, GraphType, Padding,
        Paragraph, Row, Table,
    },
    Frame,
};
use rdkafka::{statistics::Partition, Statistics};
use std::{cell::Cell, collections::BTreeMap, rc::Rc, str::FromStr};

/// Number of columns to render between bars in a bar chart.
const BAR_GAP: u16 = 2;

/// Minimum width of a bar in the total per partition chart that is allowed to display the
/// percentage of total records alongside the total count.
const MIN_BAR_WIDTH_FOR_PERCENTAGE: u16 = 14;

/// Maximum number of timestamps corresponding to recrods being consumed from the Kafka topic that
/// will be kept in memory at any given time to be evaluated for the throughput chart.
const MAX_THROUGHPUT_CAPTURE: usize = 4096;

/// Key bindings that are displayed to the user in the footer no matter what the current state of
/// the application is when viewing the stats screen.
const STATS_STANDARD_KEY_BINDINGS: [&str; 1] = [super::KEY_BINDING_QUIT];

/// Columns that are rendered in the table that displays per-[`Partition`] statistics.
const PARTITION_COLS: [&str; 21] = [
    "ID", "Brkr", "Ldr", "Dsrd", "Unkwn", "FetCt", "FetSz", "FetSt", "NxtOf", "AppOf", "StdOf",
    "CmtOf", "EofOf", "LoOf", "HiOf", "StbOf", "Lag", "Msgs", "MsgBs", "Drpd", "InFlt",
];

/// Trait that allows for transformation of an arbitrary value to a [`Row`].
trait ToRow<'a> {
    /// Converts the value to a [`Row`].
    fn to_row(&'a self) -> Row<'a>;
}

impl<'a> ToRow<'a> for &Partition {
    /// Converts from a reference to a [`Partition`] to a [`Row`].
    fn to_row(&'a self) -> Row<'a> {
        Row::new([
            self.partition.to_span(),
            self.broker.to_span(),
            self.leader.to_span(),
            self.desired.to_span(),
            self.unknown.to_span(),
            self.fetchq_cnt.to_span(),
            self.fetchq_size.to_span(),
            self.fetch_state.to_span(),
            self.next_offset.to_span(),
            self.app_offset.to_span(),
            self.stored_offset.to_span(),
            self.committed_offset.to_span(),
            self.eof_offset.to_span(),
            self.lo_offset.to_span(),
            self.hi_offset.to_span(),
            self.ls_offset.to_span(),
            self.consumer_lag.to_span(),
            self.rxmsgs.to_span(),
            self.rxbytes.to_span(),
            self.rx_ver_drops.to_span(),
            self.msgs_inflight.to_span(),
        ])
    }
}

/// Manages state related to application statistics and the UI that renders them to the user.
#[derive(Debug)]
struct StatsState {
    /// Reference to the current [`ConsumerMode`].
    consumer_mode: Rc<Cell<ConsumerMode>>,
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
    /// [`Statistics`] emitted periodically from the librdkafka library which are displayed to the
    /// user.
    statistics: Option<Statistics>,
}

impl StatsState {
    /// Creates a new [`StatsState`].
    fn new(consumer_mode: Rc<Cell<ConsumerMode>>) -> Self {
        Self {
            consumer_mode,
            received: u64::default(),
            filtered: u64::default(),
            partition_totals: BTreeMap::default(),
            timestamps: BoundedVecDeque::new(MAX_THROUGHPUT_CAPTURE),
            statistics: None,
        }
    }
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
    /// Invoked when updated [`Statistics`] are received from the librdkafka library.
    fn on_statistics_received(&mut self, statistics: &Statistics) {
        self.statistics = Some(statistics.clone());
    }
    /// Increments the total number of [`Record`]s consumed on a partition.
    fn inc_total_for_partition(&mut self, partition: i32) {
        self.partition_totals
            .entry(partition)
            .and_modify(|t| *t += 1)
            .or_insert(1);
    }
    /// Pushes a the current timestamp onto the timestamps [`BoundedVecDeque`] which indicates that
    /// a [`Record`] was consumed from the Kafka topic.
    fn push_timestamp(&mut self) {
        self.timestamps.push_front(Utc::now().timestamp_millis());
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Stats`]
/// component.
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
    /// Color used for the status text while the Kafka consumer is active.
    processing_text_color: Color,
    /// Color used for the status text while the Kafka consumer is paused.
    paused_text_color: Color,
    /// Color used for the key bindings text.
    key_bindings_text_color: Color,
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

        let processing_text_color =
            Color::from_str(value.status_text_color_processing.as_str()).expect("valid RGB hex");

        let paused_text_color =
            Color::from_str(value.status_text_color_paused.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            label_color,
            text_color,
            bar_color,
            bar_secondary_color,
            throughput_color,
            processing_text_color,
            paused_text_color,
            key_bindings_text_color,
        }
    }
}

/// Configuration used to create a new [`Stats`] component.
#[derive(Builder, Debug)]
pub struct StatsConfig<'a> {
    /// Reference to the current [`ConsumerMode`].
    consumer_mode: Rc<Cell<ConsumerMode>>,
    /// Topic name that records are being consumed from.
    topic: String,
    /// Any filter that was configured by the user.
    filter: Option<String>,
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
pub struct Stats<'a> {
    /// Topic name that records are being consumed from.
    topic: String,
    /// Any filter that was configured by the user.
    filter: Option<String>,
    /// Current state of the component and it's underlying widgets.
    state: StatsState,
    /// Color scheme for the component.
    theme: StatsTheme,
    /// Pre-constructed labels that are rendered for the column headers of the partition stats
    /// table.
    partition_labels: Vec<Span<'a>>,
    /// Pre-constructed constraints that are used for the columns of the partition stats table.
    partition_constraints: Vec<Constraint>,
}

impl<'a> From<StatsConfig<'_>> for Stats<'a> {
    /// Converts from an owned [`StatsConfig`] to an owned [`Stats`].
    fn from(value: StatsConfig<'_>) -> Self {
        Self::new(value)
    }
}

impl<'a> Stats<'a> {
    /// Creates a new [`Stats`] component using the specified [`StatsConfig`].
    pub fn new(config: StatsConfig<'_>) -> Self {
        let state = StatsState::new(config.consumer_mode);

        let theme: StatsTheme = config.theme.into();

        let constraints: Vec<Constraint> =
            PARTITION_COLS.iter().map(|_| Constraint::Min(1)).collect();

        let labels: Vec<Span> = PARTITION_COLS
            .iter()
            .map(|h| h.bold().style(theme.label_color))
            .collect();

        Self {
            topic: config.topic,
            filter: config.filter,
            state,
            theme,
            partition_labels: labels,
            partition_constraints: constraints,
        }
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

        self.render_throughput(frame, top_left_panel);
        self.render_total_by_partition(frame, top_right_panel);

        let [bottom_left_panel, bottom_right_panel] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
            .areas(bottom_panel);

        if let Some(stats) = self.state.statistics.as_ref() {
            self.render_consumer_stats(stats, frame, bottom_left_panel);
            self.render_partition_stats(stats, frame, bottom_right_panel);
        } else {
            self.render_waiting_panel(frame, bottom_left_panel);
            self.render_waiting_panel(frame, bottom_right_panel);
        }
    }
    /// Renders the panel that displays the statistics relevant to the Kafka consumer that are
    /// emitted by the librdkafka library.
    fn render_consumer_stats(&self, stats: &Statistics, frame: &mut Frame, area: Rect) {
        let consumer_stats_block = Block::bordered()
            .title(" Consumer ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let age_secs = format!("{}s", (stats.age / 1000000));

        let mut consumer_stats_rows = vec![
            Row::new([
                String::from("Client ID")
                    .bold()
                    .style(self.theme.label_color),
                stats.client_id.to_span(),
            ]),
            Row::new([
                String::from("Client Type")
                    .bold()
                    .style(self.theme.label_color),
                stats.client_type.to_span(),
            ]),
            Row::new([
                String::from("Age").bold().style(self.theme.label_color),
                age_secs.to_span(),
            ]),
            Row::new([
                String::from("Queued Ops")
                    .bold()
                    .style(self.theme.label_color),
                stats.replyq.to_span(),
            ]),
            Row::new([
                String::from("Broker Reqs")
                    .bold()
                    .style(self.theme.label_color),
                stats.tx.to_span(),
            ]),
            Row::new([
                String::from("Broker Req Bytes")
                    .bold()
                    .style(self.theme.label_color),
                stats.tx_bytes.to_span(),
            ]),
            Row::new([
                String::from("Broker Resps")
                    .bold()
                    .style(self.theme.label_color),
                stats.rx.to_span(),
            ]),
            Row::new([
                String::from("Broker Resp Bytes")
                    .bold()
                    .style(self.theme.label_color),
                stats.rx_bytes.to_span(),
            ]),
            Row::new([
                String::from("Msg Consumed Bytes")
                    .bold()
                    .style(self.theme.label_color),
                stats.rxmsg_bytes.to_span(),
            ]),
        ];

        if let Some(group) = stats.cgrp.as_ref() {
            let state_age_secs = format!("{}s", (group.stateage / 1000));

            let group_rows = vec![
                Row::new([
                    String::from("State").bold().style(self.theme.label_color),
                    group.state.to_span(),
                ]),
                Row::new([
                    String::from("State Age")
                        .bold()
                        .style(self.theme.label_color),
                    Span::raw(state_age_secs),
                ]),
            ];

            consumer_stats_rows.extend(group_rows);
        }

        let consumer_stats_table = Table::new(
            consumer_stats_rows,
            [Constraint::Min(1), Constraint::Fill(3)],
        )
        .column_spacing(1)
        .style(self.theme.bar_color)
        .block(consumer_stats_block);

        frame.render_widget(consumer_stats_table, area);
    }
    /// Renders the panel that displays the statistics relevant to the topic partitions that are
    /// emitted by the librdkafka library.
    fn render_partition_stats(&self, stats: &Statistics, frame: &mut Frame, area: Rect) {
        let partition_stats_block = Block::bordered()
            .title(" Partitions ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let topic = stats.topics.values().next().expect("valid topic stats");

        let ordered = BTreeMap::from_iter(topic.partitions.iter());

        let partition_stats_row: Vec<Row> = ordered
            .values()
            .filter(|p| p.partition >= 0)
            .map(ToRow::to_row)
            .collect();

        // TODO: can this clone be avoided?
        let header = Row::new(self.partition_labels.clone());

        let partition_stats_table = Table::new(partition_stats_row, &self.partition_constraints)
            .column_spacing(1)
            .header(header)
            .style(self.theme.bar_color)
            .block(partition_stats_block);

        frame.render_widget(partition_stats_table, area);
    }
    /// Renders the chart that displays the total throughput of records being consumed from the
    /// Kafka topic per second.
    fn render_throughput(&self, frame: &mut Frame, area: Rect) {
        let throughput_block = Block::bordered()
            .title(" Records Per Second ")
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

        let max_x_label = now.format("%H:%M:%S").to_string();

        let min_x = now - Duration::seconds(area.width as i64);
        let min_x_label = min_x.format("%H:%M:%S").to_string();

        let mid = (area.width as f32 / 2.0).round() as i64;
        let mid_x = now - Duration::seconds(mid);
        let mid_x_label = mid_x.format("%H:%M:%S").to_string();

        let x_axis = Axis::default()
            .style(self.theme.text_color)
            .labels([
                min_x_label.bold().style(self.theme.label_color),
                mid_x_label.bold().style(self.theme.label_color),
                max_x_label.bold().style(self.theme.label_color),
            ])
            .bounds([0.0, area.width as f64]);

        let mid_y = max as f64 / 2.0;

        let y_axis = Axis::default()
            .style(self.theme.text_color)
            .bounds([0.0, max as f64])
            .labels([
                "0".bold().style(self.theme.label_color),
                mid_y
                    .round()
                    .to_string()
                    .bold()
                    .style(self.theme.label_color),
                max.to_string().bold().style(self.theme.label_color),
            ]);

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
    /// Renders a panel with text indicating data is being waited on with a border into the
    /// specified area.
    fn render_waiting_panel(&self, frame: &mut Frame, area: Rect) {
        let [empty_area, text_area] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(area);

        let empty_text = Paragraph::default().block(
            Block::default()
                .borders(Borders::LEFT | Borders::TOP | Borders::RIGHT)
                .border_style(self.theme.panel_border_color),
        );

        let waiting_text = Paragraph::new("Waiting...")
            .style(self.theme.panel_border_color)
            .block(
                Block::default()
                    .borders(Borders::LEFT | Borders::BOTTOM | Borders::RIGHT)
                    .border_style(self.theme.panel_border_color),
            )
            .centered();

        frame.render_widget(empty_text, empty_area);
        frame.render_widget(waiting_text, text_area);
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

impl<'a> Component for Stats<'a> {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Stats"
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let consumer_status_line = ConsumerStatusLine::builder()
            .consumer_mode(self.state.consumer_mode.get())
            .topic(self.topic.as_str())
            .filter(self.filter.as_ref())
            .processing_style(self.theme.processing_text_color)
            .paused_style(self.theme.paused_text_color)
            .build()
            .expect("valid consumer status line widget");

        frame.render_widget(consumer_status_line, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let consumer_mode_key_binding = match self.state.consumer_mode.get() {
            ConsumerMode::Processing => super::KEY_BINDING_PAUSE,
            ConsumerMode::Paused => super::KEY_BINDING_RESUME,
        };

        let mut key_bindings = Vec::from(STATS_STANDARD_KEY_BINDINGS);
        key_bindings.push(consumer_mode_key_binding);

        let text = Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &self,
        event: KeyEvent,
        _buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        match event.code {
            KeyCode::Char(c) => match c {
                'p' => Some(Event::PauseProcessing),
                'r' => Some(Event::ResumeProcessing),
                _ => None,
            },
            _ => None,
        }
    }
    /// Allows the [`Component`] to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &Event) {
        match event {
            Event::RecordReceived(record) => self.state.on_record_received(record),
            Event::RecordFiltered(record) => self.state.on_record_filtered(record),
            Event::StatisticsReceived(stats) => self.state.on_statistics_received(stats),
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

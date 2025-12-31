use crate::{
    app::{BufferedKeyPress, config::Theme},
    event::Event,
    kafka::schema::{Schema, Subject, Version},
    ui::Component,
};

use crossterm::event::{KeyCode, KeyEvent};
use derive_builder::Builder;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text, ToSpan},
    widgets::{
        Block, BorderType, Borders, HighlightSpacing, List, ListItem, ListState, Padding,
        Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table, Wrap,
    },
};
use std::str::FromStr;

/// Key bindings that are always displayed to the user in the footer when viewing the schemas
/// screen.
const SCHEMAS_KEY_BINDINGS: [&str; 2] = [super::KEY_BINDING_QUIT, super::KEY_BINDING_CHANGE_FOCUS];

/// Text displayed to the user in the footer for the filter key binding.
const KEY_BINDING_FILTER: &str = "(/) filter";

/// Text displayed to the user in the footer for the stop filtering key binding.
const KEY_BINDING_APPLY_FILTER: &str = "(enter) apply filter";

/// Text displayed to the user in the footer for the clear filter key binding.
const KEY_BINDING_CLEAR_FILTER: &str = "(c) clear filter";

/// Enumerates the possible network states of the [`Topics`] component.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum NetworkStatus {
    /// The component is idle and not performing any network operations.
    #[default]
    Idle,
    /// The component is currently loading the list of subjects from the schema registry.
    LoadingSubjects,
    /// The component is currently loading a schema from the schema registry.
    LoadingSchema,
}

/// Enumeration of the widgets in the [`Schemas`] component that can have focus.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum SchemasWidget {
    /// The subjects list widget.
    #[default]
    Subjects,
    /// The subjects filter input widget.
    FilterInput,
    /// The schema definition widget.
    Schema,
    /// The schema versions list widget.
    Versions,
    /// The schema references list widget.
    References,
}

/// Manages state related to schemas and the UI that renders them to the user.
#[derive(Debug, Default)]
struct SchemasState {
    /// Stores the widget that currently has focus.
    active_widget: SchemasWidget,
    /// Current subjects retrieved from the schema registry.
    subjects: Vec<Subject>,
    /// Indices into the subjects list of only the subjects currently visible to the user based on the
    /// filter value.
    visible_indices: Vec<usize>,
    /// Currently selected subject.
    selected_subject: Option<Subject>,
    /// Currently selected schema details.
    selected_schema: Option<Schema>,
    /// Available schema versions for the currently selected subject.
    available_versions: Vec<Version>,
    /// Manages state of the subjects list widget.
    subjects_list_state: ListState,
    /// Manages state of the subjects list scrollbar.
    subjects_scroll_state: ScrollbarState,
    /// Manages state of the schema versions list widget.
    versions_list_state: ListState,
    /// Manages state of the schema versions list scrollbar.
    versions_scroll_state: ScrollbarState,
    /// Contains the current scrolling state for the schema definition text.
    schema_definition_scroll: (u16, u16),
    /// Manages state of the schema references list widget.
    references_list_state: ListState,
    /// Manages state of the schema references list scrollbar.
    references_scroll_state: ScrollbarState,
    /// Current network status of the component.
    network_status: NetworkStatus,
    /// Current filter applied to the subjects list.
    subjects_filter: Option<String>,
}

impl SchemasState {
    /// Creates a new default [`SchemasState`].
    fn new() -> Self {
        Self::default()
    }
    /// Updates the list of visible subjects based on the current filter value.
    fn update_visible_subjects(&mut self) {
        let filter = self.subjects_filter.as_ref().map_or("", |f| f.as_str());

        self.visible_indices = self
            .subjects
            .iter()
            .enumerate()
            .filter(|(_, s)| s.as_ref().starts_with(filter))
            .map(|(i, _)| i)
            .collect::<Vec<usize>>();
    }
    /// Deselects the currently selected subject.
    fn deselect_subject(&mut self) {
        self.subjects_list_state.select(None);
        self.selected_subject = None;
    }
    /// Invoked when the user starts filtering subjects.
    fn on_start_filter(&mut self) {
        self.active_widget = SchemasWidget::FilterInput;
        self.deselect_subject();
    }
    /// Invoked when the user applies the subject filter.
    fn on_apply_filter(&mut self) {
        self.active_widget = SchemasWidget::Subjects;
    }
    /// Invoked when the user clears the subject filter.
    fn on_clear_filter(&mut self) {
        self.subjects_filter = None;
        self.deselect_subject();
        self.update_visible_subjects();
    }
    /// Cycles the focus to the next available widget based on the currently selected widget.
    fn select_next_widget(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref() {
            self.active_widget = match self.active_widget {
                SchemasWidget::Subjects => SchemasWidget::Schema,
                SchemasWidget::FilterInput => SchemasWidget::Subjects,
                SchemasWidget::Schema => SchemasWidget::Versions,
                SchemasWidget::Versions => {
                    if schema.references.is_some() {
                        SchemasWidget::References
                    } else {
                        SchemasWidget::Subjects
                    }
                }
                SchemasWidget::References => SchemasWidget::Subjects,
            }
        }
    }
    /// Selects the first subject in the list.
    fn select_first_subject(&mut self) -> Option<&Subject> {
        if self.visible_indices.is_empty() {
            return None;
        }

        self.subjects_list_state.select_first();
        self.subjects_scroll_state.first();

        self.versions_list_state.select(None);
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject_idx = self
            .visible_indices
            .first()
            .expect("visible indices is not empty");
        self.selected_subject = self.subjects.get(*subject_idx).cloned();

        self.selected_subject.as_ref()
    }
    /// Selects the next subject in the list.
    fn select_next_subject(&mut self) -> Option<&Subject> {
        if self.visible_indices.is_empty() {
            return None;
        }

        if let Some(curr_idx) = self.subjects_list_state.selected()
            && curr_idx == self.visible_indices.len() - 1
        {
            return None;
        }

        self.subjects_list_state.select_next();
        self.subjects_scroll_state.next();

        self.versions_list_state.select(None);
        self.versions_scroll_state = self.versions_scroll_state.position(0);

        self.references_list_state.select(None);
        self.references_scroll_state = self.references_scroll_state.position(0);

        self.schema_definition_scroll = (0, 0);

        let idx = self.subjects_list_state.selected().expect("subject selected");

        let subject_idx = self.visible_indices.get(idx).expect("visible index exists");
        self.selected_subject = self.subjects.get(*subject_idx).cloned();

        self.selected_subject.as_ref()
    }
    /// Selects the previous subject in the list.
    fn select_prev_subject(&mut self) -> Option<&Subject> {
        if self.visible_indices.is_empty() {
            return None;
        }

        self.subjects_list_state.select_previous();
        self.subjects_scroll_state.prev();

        self.versions_list_state.select(None);

        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let idx = self.subjects_list_state.selected().expect("subject selected");

        let subject_idx = self.visible_indices.get(idx).expect("visible index exists");
        self.selected_subject = self.subjects.get(*subject_idx).cloned();

        self.selected_subject.as_ref()
    }
    /// Selects the last subject in the list.
    fn select_last_subject(&mut self) -> Option<&Subject> {
        if self.visible_indices.is_empty() {
            return None;
        }

        self.subjects_list_state.select_last();
        self.subjects_scroll_state.last();

        self.versions_list_state.select(None);
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject_idx = self
            .visible_indices
            .last()
            .expect("visible indices is not empty");
        self.selected_subject = self.subjects.get(*subject_idx).cloned();

        self.selected_subject.as_ref()
    }
    /// Selects the first subject schema version in the list.
    fn select_first_schema_version(&mut self) -> Option<(&Subject, Version)> {
        let current_idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        if current_idx == 0 {
            return None;
        }

        self.versions_list_state.select_first();
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject = self.selected_subject.as_ref().expect("subject selected");
        let version = self.available_versions.last().expect("version exists");

        Some((subject, *version))
    }
    /// Selects the next subject schema version in the list.
    fn select_next_schema_version(&mut self) -> Option<(&Subject, Version)> {
        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        if idx == self.available_versions.len() - 1 {
            return None;
        }

        self.versions_list_state.select_next();
        self.versions_scroll_state.next();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject = self.selected_subject.as_ref().expect("subject selected");

        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        let version_idx = self.available_versions.len() - 1 - idx;

        let version = self
            .available_versions
            .get(version_idx)
            .expect("version exists");

        Some((subject, *version))
    }
    /// Selects the previous subject schema version in the list.
    fn select_prev_schema_version(&mut self) -> Option<(&Subject, Version)> {
        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        if idx == 0 {
            return None;
        }

        self.versions_list_state.select_previous();
        self.versions_scroll_state.prev();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject = self.selected_subject.as_ref().expect("subject selected");

        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        let version_idx = self.available_versions.len() - 1 - idx;

        let version = self
            .available_versions
            .get(version_idx)
            .expect("version exists");

        Some((subject, *version))
    }
    /// Selects the last subject schema version in the list.
    fn select_last_schema_version(&mut self) -> Option<(&Subject, Version)> {
        let current_idx = self
            .versions_list_state
            .selected()
            .expect("version always selected");

        if current_idx == self.available_versions.len() - 1 {
            return None;
        }

        self.versions_list_state.select_last();
        self.versions_scroll_state.last();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let subject = self.selected_subject.as_ref().expect("subject selected");
        let version = self.available_versions.first().expect("version exists");

        Some((subject, *version))
    }
    /// Moves the schema definition scroll state to the top.
    fn scroll_schema_definition_top(&mut self) {
        self.schema_definition_scroll.0 = 0;
    }
    /// Moves the schema definitionscroll state down by `n` number of lines.
    fn scroll_schema_definition_down(&mut self, n: u16) {
        self.schema_definition_scroll.0 += n;
    }
    /// Moves the schema definition scroll state up by `n` number of lines.
    fn scroll_schema_definition_up(&mut self, n: u16) {
        if self.schema_definition_scroll.0 >= n {
            self.schema_definition_scroll.0 -= n;
        }
    }
    /// Moves the schema references scroll state to the top.
    fn scroll_references_top(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref()
            && let Some(refs) = schema.references.as_ref()
        {
            if refs.is_empty() {
                return;
            }

            self.references_list_state.select_first();
            self.references_scroll_state.first();
        }
    }
    /// Moves the schema references scroll state down.
    fn scroll_references_down(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref()
            && let Some(refs) = schema.references.as_ref()
        {
            if refs.is_empty() {
                return;
            }

            self.references_list_state.select_next();
            self.references_scroll_state.next();
        }
    }
    /// Moves the schema references scroll state up.
    fn scroll_references_up(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref()
            && let Some(refs) = schema.references.as_ref()
        {
            if refs.is_empty() {
                return;
            }

            self.references_list_state.select_previous();
            self.references_scroll_state.prev();
        }
    }
    /// Moves the schema references scroll state to the bottom.
    fn scroll_references_bottom(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref()
            && let Some(refs) = schema.references.as_ref()
        {
            if refs.is_empty() {
                return;
            }

            self.references_list_state.select_last();
            self.references_scroll_state.last();
        }
    }
}

/// Contains the [`Color`]s from the application [`Theme`] required to render the [`Schemas`]
/// component.
#[derive(Debug)]
struct SchemasTheme {
    /// Color used for the borders of the main info panels.
    panel_border_color: Color,
    /// Color used for the borders of the selected info panel.
    selected_panel_border_color: Color,
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

        let selected_panel_border_color =
            Color::from_str(value.selected_panel_border_color.as_str()).expect("valid RGB hex");

        let label_color = Color::from_str(value.label_color.as_str()).expect("valid RGB hex");

        let key_bindings_text_color =
            Color::from_str(value.key_bindings_text_color.as_str()).expect("valid RGB hex");

        Self {
            panel_border_color,
            selected_panel_border_color,
            label_color,
            key_bindings_text_color,
        }
    }
}

/// Configuration used to create a new [`Schemas`] component.
#[derive(Builder)]
pub struct SchemasConfig<'a> {
    /// Controls how many lines each press of a key scrolls the schema definition text.
    scroll_factor: u16,
    /// Reference to the application [`Theme`].
    theme: &'a Theme,
}

impl<'a> SchemasConfig<'a> {
    /// Creates a new default [`SchemasConfigBuilder`] which can be used to create a new
    /// [`SchemasConfig`].
    pub fn builder() -> SchemasConfigBuilder<'a> {
        SchemasConfigBuilder::default()
    }
}

impl<'a> From<SchemasConfig<'a>> for Schemas {
    /// Converts from an owned [`SchemasConfig`] to an owned [`Schemas`].
    fn from(value: SchemasConfig<'a>) -> Self {
        Self::new(value)
    }
}

/// The application [`Component`] that is responsible for displaying data from the Schema Registry
/// if one is configured.
pub struct Schemas {
    /// Current state of the component and it's underlying widgets.
    state: SchemasState,
    /// Controls how many lines each press of a key scrolls the schema definition text.
    scroll_factor: u16,
    /// Color scheme for the component.
    theme: SchemasTheme,
}

impl Schemas {
    /// Creates a new [`Schemas`] component using the specified [`SchemasConfig`].
    fn new(config: SchemasConfig<'_>) -> Self {
        Self {
            state: SchemasState::new(),
            scroll_factor: config.scroll_factor,
            theme: config.theme.into(),
        }
    }
    /// Invoked when the list of subjects has been loaded from the schema registry.
    fn on_subjects_loaded(&mut self, subjects: Vec<Subject>) {
        self.state.network_status = NetworkStatus::Idle;
        self.state.subjects = subjects;
        self.state.update_visible_subjects();
    }
    /// Renders the filter input box for filtering subjects.
    fn render_filter_input(&mut self, frame: &mut Frame, area: Rect) {
        let filter_block = Block::bordered()
            .title(" Filter ")
            .border_type(BorderType::Thick)
            .border_style(self.theme.selected_panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        let filter = self.state.subjects_filter.as_ref().map_or("", |f| f.as_str());

        let filter_text = Paragraph::new(filter).block(filter_block);

        frame.render_widget(filter_text, area);
    }
    /// Renders the list of subjects.
    fn render_subjects(&mut self, frame: &mut Frame, area: Rect) {
        if self.state.network_status == NetworkStatus::LoadingSubjects {
            self.render_message(frame, area, "Loading subjects...", None);
            return;
        } else if self.state.visible_indices.is_empty() {
            self.render_message(frame, area, "No subjects found", None);
            return;
        }

        let mut subjects_block = Block::bordered()
            .title(" Subjects ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == SchemasWidget::Subjects {
            subjects_block = subjects_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let list_items: Vec<ListItem> = self
            .state
            .visible_indices
            .iter()
            .map(|i| self.state.subjects.get(*i).expect("valid subject index"))
            .map(|s| ListItem::new(s.as_ref()))
            .collect();

        let list = List::new(list_items)
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .highlight_symbol(">")
            .highlight_spacing(HighlightSpacing::Always)
            .block(subjects_block);

        frame.render_stateful_widget(list, area, &mut self.state.subjects_list_state);

        if self.state.selected_schema.is_some() {
            self.state.subjects_scroll_state = self
                .state
                .subjects_scroll_state
                .content_length(self.state.visible_indices.len());

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
                &mut self.state.subjects_scroll_state,
            );
        }
    }
    /// Renders the schema definition for the selected subject.
    fn render_schema(&self, frame: &mut Frame, area: Rect) {
        if self.state.network_status == NetworkStatus::LoadingSchema {
            self.render_message(frame, area, "Loading schema...", None);
            return;
        }

        let mut schema_block = Block::bordered()
            .title(" Schema ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == SchemasWidget::Schema {
            schema_block = schema_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        let schema = self
            .state
            .selected_schema
            .as_ref()
            .expect("schema is selected");

        let schema_paragraph = Paragraph::new(schema.schema.clone())
            .block(schema_block)
            .wrap(Wrap { trim: false })
            .scroll(self.state.schema_definition_scroll);

        frame.render_widget(schema_paragraph, area);
    }
    /// Renders the versions available for the selected subject.
    fn render_versions(&mut self, frame: &mut Frame, area: Rect) {
        let mut versions_block = Block::bordered()
            .title(" Versions ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == SchemasWidget::Versions {
            versions_block = versions_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        if !self.state.available_versions.is_empty() {
            let list_items: Vec<ListItem> = self
                .state
                .available_versions
                .iter()
                .rev()
                .map(|v| ListItem::new(v.to_string()))
                .collect();

            let versions_list = List::new(list_items)
                .block(versions_block)
                .highlight_style(Modifier::REVERSED)
                .highlight_symbol(">")
                .highlight_spacing(HighlightSpacing::Always);

            frame.render_stateful_widget(versions_list, area, &mut self.state.versions_list_state);

            self.state.versions_scroll_state = self
                .state
                .versions_scroll_state
                .content_length(self.state.available_versions.len());

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
                &mut self.state.versions_scroll_state,
            );
        } else {
            frame.render_widget(versions_block, area);
        }
    }
    /// Renders details for the selected subject.
    fn render_info(&self, frame: &mut Frame, area: Rect) {
        let info_block = Block::bordered()
            .title(" Info ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if let Some(schema) = self.state.selected_schema.as_ref() {
            let id = schema.id;
            let guid = schema.guid.clone();
            let schema_type = schema.kind.clone();
            let version = schema.version;

            let info_rows = vec![
                Row::new(["ID".bold().style(self.theme.label_color), id.to_span()]),
                Row::new(["GUID".bold().style(self.theme.label_color), guid.to_span()]),
                Row::new([
                    "Version".bold().style(self.theme.label_color),
                    version.to_span(),
                ]),
                Row::new([
                    "Type".bold().style(self.theme.label_color),
                    schema_type.to_span(),
                ]),
            ];

            let info_table = Table::new(info_rows, [Constraint::Fill(2), Constraint::Fill(8)])
                .column_spacing(1)
                .block(info_block);

            frame.render_widget(info_table, area);
        } else {
            frame.render_widget(info_block, area);
        }
    }
    /// Renders any references contained in the selected subject.
    fn render_references(&mut self, frame: &mut Frame, area: Rect) {
        let mut refs_block = Block::bordered()
            .title(" References ")
            .border_style(self.theme.panel_border_color)
            .padding(Padding::new(1, 1, 0, 0));

        if self.state.active_widget == SchemasWidget::References {
            refs_block = refs_block
                .border_type(BorderType::Thick)
                .border_style(self.theme.selected_panel_border_color);
        }

        match self.state.selected_schema.as_ref() {
            Some(schema) => match schema.references.as_ref() {
                Some(refs) => {
                    let list_items: Vec<ListItem> = refs
                        .iter()
                        .map(|r| {
                            let name = r.name.clone();
                            let subject = r.subject.clone();
                            let version = r.version.to_string();

                            let text = Text::from_iter([
                                Line::from_iter([
                                    Span::styled("Name: ", self.theme.label_color),
                                    Span::raw(name),
                                ]),
                                Line::from_iter([
                                    Span::styled("Subject: ", self.theme.label_color),
                                    Span::raw(subject),
                                ]),
                                Line::from_iter([
                                    Span::styled("Version: ", self.theme.label_color),
                                    Span::raw(version),
                                ]),
                            ]);

                            ListItem::new(text)
                        })
                        .collect();

                    let list = List::new(list_items).block(refs_block);

                    frame.render_stateful_widget(list, area, &mut self.state.references_list_state);

                    self.state.references_scroll_state = self
                        .state
                        .references_scroll_state
                        .content_length(refs.len());

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
                        &mut self.state.references_scroll_state,
                    );
                }
                None => {
                    self.render_message(frame, area, "No references", Some(" References "));
                }
            },
            None => frame.render_widget(refs_block, area),
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
    /// Invoked when the latest schema version and all available versions have been loaded from the
    /// schema registry.
    fn on_latest_schema_loaded(&mut self, schema: Option<Schema>, versions: Vec<Version>) {
        self.state.network_status = NetworkStatus::Idle;
        self.state.selected_schema = schema;

        self.state.available_versions = versions;

        self.state.versions_list_state.select_first();
    }
    /// Invoked when a schema version has been loaded from the schema registry.
    fn on_schema_version_loaded(&mut self, schema: Option<Schema>) {
        self.state.network_status = NetworkStatus::Idle;
        self.state.selected_schema = schema;
    }
}

impl Component for Schemas {
    /// Returns the name of the [`Component`] which is displayed to the user as a menu item.
    fn name(&self) -> &'static str {
        "Schemas"
    }
    /// Renders the component-specific widgets to the terminal.
    fn render(&mut self, frame: &mut Frame, area: Rect) {
        if self.state.selected_schema.is_none() {
            let [left_panel, right_panel] = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(20), Constraint::Percentage(80)])
                .areas(area);

            let subjects_panel = if self.state.active_widget == SchemasWidget::FilterInput {
                let [filter_panel, subjects_panel] = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Max(3), Constraint::Min(1)])
                    .areas(left_panel);

                self.render_filter_input(frame, filter_panel);

                subjects_panel
            } else {
                left_panel
            };

            self.render_subjects(frame, subjects_panel);
            self.render_message(frame, right_panel, "No subject selected", None);
        } else {
            let [left_panel, middle_panel, right_panel] = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(60),
                    Constraint::Percentage(20),
                ])
                .areas(area);

            let subjects_panel = if self.state.active_widget == SchemasWidget::FilterInput {
                let [filter_panel, subjects_panel] = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Max(3), Constraint::Min(1)])
                    .areas(left_panel);

                self.render_filter_input(frame, filter_panel);

                subjects_panel
            } else {
                left_panel
            };

            self.render_subjects(frame, subjects_panel);

            let [right_top_panel, right_middle_panel, right_bottom_panel] = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(25),
                    Constraint::Percentage(15),
                    Constraint::Percentage(60),
                ])
                .areas(right_panel);

            self.render_schema(frame, middle_panel);
            self.render_versions(frame, right_top_panel);
            self.render_info(frame, right_middle_panel);
            self.render_references(frame, right_bottom_panel);
        }
    }
    /// Allows the [`Component`] to map a [`KeyEvent`] to an [`Event`] which will be published
    /// for processing.
    fn map_key_event(
        &mut self,
        event: KeyEvent,
        buffered: Option<&BufferedKeyPress>,
    ) -> Option<Event> {
        match event.code {
            KeyCode::Enter => {
                self.state.on_apply_filter();
                Some(Event::Void)
            }
            KeyCode::Backspace | KeyCode::Delete => {
                if self.state.active_widget == SchemasWidget::FilterInput
                    && let Some(filter) = self.state.subjects_filter.as_mut()
                {
                    filter.pop();
                    self.state.update_visible_subjects();
                }

                if let Some(filter) = self.state.subjects_filter.as_ref()
                    && filter.is_empty()
                {
                    self.state.subjects_filter = None;
                }

                Some(Event::Void)
            }
            KeyCode::Char(c) => match self.state.active_widget {
                    SchemasWidget::Subjects => {
                        let mapped_event = match c {
                            'e' => self
                                .state
                                .selected_schema
                                .as_ref()
                                .map(|s| Event::ExportSchema(s.clone())),
                            '/' => {
                                self.state.on_start_filter();
                                Some(Event::Void)
                            }
                            'c' if self.state.subjects_filter.is_some() => {
                                self.state.on_clear_filter();
                                Some(Event::Void)
                            }
                            'g' if buffered.filter(|kp| kp.is('g')).is_some() => self
                                .state
                                .select_first_subject()
                                .map(|s| Event::LoadLatestSchema(s.clone())),
                            'j' => self
                                .state
                                .select_next_subject()
                                .map(|s| Event::LoadLatestSchema(s.clone())),
                            'k' => self
                                .state
                                .select_prev_subject()
                                .map(|s| Event::LoadLatestSchema(s.clone())),
                            'G' => self
                                .state
                                .select_last_subject()
                                .map(|s| Event::LoadLatestSchema(s.clone())),
                            _ => None,
                        };

                        if let Some(Event::LoadLatestSchema(_)) = mapped_event {
                            self.state.network_status = NetworkStatus::LoadingSchema;
                        }

                        mapped_event
                    }
                    SchemasWidget::FilterInput => {
                        if let Some(filter) = self.state.subjects_filter.as_mut() {
                            filter.push(c);
                        } else {
                            self.state.subjects_filter = Some(c.to_string());
                        }

                        self.state.update_visible_subjects();

                        Some(Event::Void)
                    }
                    SchemasWidget::Schema => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            self.state.scroll_schema_definition_top();
                            Some(Event::Void)
                        }
                        'j' => {
                            self.state.scroll_schema_definition_down(self.scroll_factor);
                            Some(Event::Void)
                        }
                        'k' => {
                            self.state.scroll_schema_definition_up(self.scroll_factor);
                            Some(Event::Void)
                        }
                        _ => None,
                    },
                    SchemasWidget::Versions => {
                        let mapped_event = match c {
                            'g' if buffered.filter(|kp| kp.is('g')).is_some() => self
                                .state
                                .select_first_schema_version()
                                .map(|(s, v)| Event::LoadSchemaVersion(s.clone(), v)),
                            'j' => self
                                .state
                                .select_next_schema_version()
                                .map(|(s, v)| Event::LoadSchemaVersion(s.clone(), v)),
                            'k' => self
                                .state
                                .select_prev_schema_version()
                                .map(|(s, v)| Event::LoadSchemaVersion(s.clone(), v)),
                            'G' => self
                                .state
                                .select_last_schema_version()
                                .map(|(s, v)| Event::LoadSchemaVersion(s.clone(), v)),
                            _ => None,
                        };

                        if mapped_event.is_some() {
                            self.state.network_status = NetworkStatus::LoadingSchema;
                        }

                        mapped_event
                    }
                    SchemasWidget::References => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            self.state.scroll_references_top();
                            Some(Event::Void)
                        }
                        'j' => {
                            self.state.scroll_references_down();
                            Some(Event::Void)
                        }
                        'k' => {
                            self.state.scroll_references_up();
                            Some(Event::Void)
                        }
                        'G' => {
                            self.state.scroll_references_bottom();
                            Some(Event::Void)
                        }
                        _ => None,
                    },
                },
            _ => None,
        }
    }
    /// Allows the [`Component`] to handle any [`Event`] that was not handled by the main
    /// application.
    fn on_app_event(&mut self, event: &Event) {
        match event {
            Event::SelectNextWidget => self.state.select_next_widget(),
            Event::SubjectsLoaded(subjects) => self.on_subjects_loaded(subjects.to_vec()),
            Event::LatestSchemaLoaded(schema, versions) => {
                self.on_latest_schema_loaded(schema.clone(), versions.to_vec())
            }
            Event::SchemaVersionLoaded(schema) => self.on_schema_version_loaded(schema.clone()),
            _ => {}
        }
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let filter_value = self
            .state
            .subjects_filter
            .as_ref()
            .map_or("<none>", |f| f.as_str());

        let line = Line::from_iter([
            Span::styled("Total: ", Style::from(self.theme.label_color).bold()),
            Span::raw(self.state.subjects.len().to_string()),
            Span::raw(" | "),
            Span::styled("Visible: ", Style::from(self.theme.label_color).bold()),
            Span::raw(self.state.visible_indices.len().to_string()),
            Span::raw(format!(" (Filter: {})", filter_value)),
        ]);

        let text = Paragraph::new(line).left_aligned();

        frame.render_widget(text, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let mut key_bindings = Vec::from(SCHEMAS_KEY_BINDINGS);

        match self.state.active_widget {
            SchemasWidget::Subjects | SchemasWidget::Versions | SchemasWidget::References => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_NEXT);
                key_bindings.push(super::KEY_BINDING_PREV);
                key_bindings.push(super::KEY_BINDING_BOTTOM);
            }
            SchemasWidget::Schema => {
                key_bindings.push(super::KEY_BINDING_TOP);
                key_bindings.push(super::KEY_BINDING_SCROLL_DOWN);
                key_bindings.push(super::KEY_BINDING_SCROLL_UP);
            }
            SchemasWidget::FilterInput => {}
        }

        match (self.state.active_widget, self.state.subjects_filter.as_ref()) {
            (SchemasWidget::Subjects, None) => {
                key_bindings.push(KEY_BINDING_FILTER);
            }
            (SchemasWidget::Subjects, Some(_)) => {
                key_bindings.push(KEY_BINDING_FILTER);
                key_bindings.push(KEY_BINDING_CLEAR_FILTER);
            }
            (SchemasWidget::FilterInput, _) => {
                key_bindings.push(KEY_BINDING_APPLY_FILTER);
            }
            _ => {}
        }

        if self.state.selected_schema.is_some() {
            key_bindings.push(super::KEY_BINDING_EXPORT);
        }

        let text = Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Hook for the [`Component`] to run any logic required when it becomes active. The
    /// [`Component`] can also return an optional [`Event`] that will be dispatched.
    fn on_activate(&mut self) -> Option<Event> {
        if self.state.subjects.is_empty() {
            self.state.network_status = NetworkStatus::LoadingSubjects;
            Some(Event::LoadSubjects)
        } else {
            None
        }
    }
}

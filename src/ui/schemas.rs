use crate::{
    app::{BufferedKeyPress, config::Theme},
    event::{Event, ScrollPosition},
    ui::Component,
};

use anyhow::Context;
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
use schema_registry_client::rest::{
    client_config::ClientConfig,
    models::{RegisteredSchema, SchemaReference},
    schema_registry_client::{Client, SchemaRegistryClient},
};
use std::str::FromStr;

/// String presented to the user when a schema-releated value is missing or not known.
const UNKNOWN: &str = "<unknown>";

/// Key bindings that are always displayed to the user in the footer when viewing the schemas
/// screen.
const SCHEMAS_KEY_BINDINGS: [&str; 2] = [super::KEY_BINDING_QUIT, super::KEY_BINDING_CHANGE_FOCUS];

/// Enumeration of the widgets in the [`Schemas`] component that can have focus.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
enum SchemasWidget {
    #[default]
    Subjects,
    Schema,
    Versions,
    References,
}

/// Represents a reference to another schema contained in a schema retrieved from the schema
/// registry.
#[derive(Debug)]
struct SchemaRef {
    /// Name of the referenced schema.
    name: String,
    /// Subject the referenced schema belongs to.
    subject: String,
    /// Version of the referenced schema.
    version: i32,
}

impl From<SchemaReference> for SchemaRef {
    /// Converts from a [`SchemaReference`] fetched from the schema registry to a new
    /// [`SchemaRef`].
    fn from(value: SchemaReference) -> Self {
        Self {
            name: value.name.unwrap_or_else(|| UNKNOWN.to_string()),
            subject: value.subject.unwrap_or_else(|| UNKNOWN.to_string()),
            version: value.version.unwrap_or_default(),
        }
    }
}

/// A schema retrieved from the schema registry along with all available versions for the subject
/// it is associated with.
#[derive(Debug)]
struct Schema {
    /// Identifier of the schema.
    id: i32,
    /// Globally unique identifier of the schema.
    guid: String,
    /// Version of the schema.
    version: i32,
    /// The schema type, i.e. AVRO, JSON, PROTOBUF.
    kind: String,
    /// The schema definition.
    schema: String,
    /// References to other schemas contained in this schema.
    references: Option<Vec<SchemaRef>>,
    /// All available versions for the subject this schema belongs to.
    available_versions: Vec<i32>,
}

impl Schema {
    /// Creates a new [`Schema`] from the given [`RegisteredSchema`] fetched from the schema
    /// registry and the list of all available schema versions for the subject.
    fn new(registered_schema: RegisteredSchema, versions: Vec<i32>) -> Self {
        let id = registered_schema.id.unwrap_or_default();

        let guid = registered_schema
            .guid
            .unwrap_or_else(|| UNKNOWN.to_string());

        let version = registered_schema.version.unwrap_or_default();

        let kind = registered_schema
            .schema_type
            .unwrap_or_else(|| UNKNOWN.to_string());

        let schema = registered_schema
            .schema
            .unwrap_or_else(|| UNKNOWN.to_string());

        let references = registered_schema
            .references
            .map(|refs| refs.into_iter().map(|r| r.into()).collect());

        Self {
            id,
            guid,
            version,
            kind,
            schema,
            references,
            available_versions: versions,
        }
    }
}

/// Manages state related to schemas and the UI that renders them to the user.
#[derive(Debug, Default)]
struct SchemasState {
    /// Stores the widget that the currently has focus.
    active_widget: SchemasWidget,
    /// Current subjects retrieved from the schema registry.
    subjects: Vec<String>,
    /// Currently selected subject.
    selected_subject: Option<String>,
    /// Currently selected schema details.
    selected_schema: Option<Schema>,
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
}

impl SchemasState {
    /// Creates a new default [`SchemasState`].
    fn new() -> Self {
        Self::default()
    }
    /// Cycles the focus to the next available widget based on the currently selected widget.
    fn select_next_widget(&mut self) {
        if let Some(schema) = self.selected_schema.as_ref() {
            self.active_widget = match self.active_widget {
                SchemasWidget::Subjects => SchemasWidget::Schema,
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
    fn select_first_subject(&mut self) {
        if self.subjects.is_empty() {
            return;
        }

        self.subjects_list_state.select_first();
        self.subjects_scroll_state.first();

        self.versions_list_state.select(None);
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        self.selected_subject = self.subjects.first().cloned();
    }
    /// Selects the next subject in the list.
    fn select_next_subject(&mut self) {
        if self.subjects.is_empty() {
            return;
        }

        if let Some(curr_idx) = self.subjects_list_state.selected()
            && curr_idx == self.subjects.len() - 1
        {
            return;
        }

        self.subjects_list_state.select_next();
        self.subjects_scroll_state.next();

        self.versions_list_state.select(None);
        self.versions_scroll_state = self.versions_scroll_state.position(0);

        self.references_list_state.select(None);
        self.references_scroll_state = self.references_scroll_state.position(0);

        self.schema_definition_scroll = (0, 0);

        let idx = self
            .subjects_list_state
            .selected()
            .expect("subject selected");

        self.selected_subject = self.subjects.get(idx).cloned();
    }
    /// Selects the previous subject in the list.
    fn select_prev_subject(&mut self) {
        if self.subjects.is_empty() {
            return;
        }

        self.subjects_list_state.select_previous();
        self.subjects_scroll_state.last();

        self.versions_list_state.select(None);
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let idx = self
            .subjects_list_state
            .selected()
            .expect("subject selected");

        self.selected_subject = self.subjects.get(idx).cloned();
    }
    /// Selects the last subject in the list.
    fn select_last_subject(&mut self) {
        if self.subjects.is_empty() {
            return;
        }

        self.subjects_list_state.select_last();
        self.subjects_scroll_state.last();

        self.versions_list_state.select(None);
        self.versions_scroll_state.first();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        self.selected_subject = self.subjects.last().cloned();
    }
    /// Selects the first subject schema version in the list.
    fn select_first_schema_version(&mut self) -> Option<i32> {
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

        let schema = self.selected_schema.as_ref().expect("schema selected");

        let version = schema.available_versions.last().expect("version exists");

        Some(*version)
    }
    /// Selects the next subject schema version in the list.
    fn select_next_schema_version(&mut self) -> Option<i32> {
        let schema = self.selected_schema.as_ref().expect("schema selected");

        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        if idx == schema.available_versions.len() - 1 {
            return None;
        }

        self.versions_list_state.select_next();
        self.versions_scroll_state.next();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        let version_idx = schema.available_versions.len() - 1 - idx;

        let version = schema
            .available_versions
            .get(version_idx)
            .expect("version exists");

        Some(*version)
    }
    /// Selects the previous subject schema version in the list.
    fn select_prev_schema_version(&mut self) -> Option<i32> {
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

        let schema = self.selected_schema.as_ref().expect("schema selected");

        let idx = self
            .versions_list_state
            .selected()
            .expect("version selected");

        let version_idx = schema.available_versions.len() - 1 - idx;

        let version = schema
            .available_versions
            .get(version_idx)
            .expect("version exists");

        Some(*version)
    }
    /// Selects the last subject schema version in the list.
    fn select_last_schema_version(&mut self) -> Option<i32> {
        let schema = self.selected_schema.as_ref().expect("schema selected");

        let current_idx = self
            .versions_list_state
            .selected()
            .expect("version always selected");

        if current_idx == schema.available_versions.len() - 1 {
            return None;
        }

        self.versions_list_state.select_last();
        self.versions_scroll_state.last();

        self.references_list_state.select(None);
        self.references_scroll_state.first();

        self.schema_definition_scroll = (0, 0);

        let version = schema.available_versions.first().expect("version exists");

        Some(*version)
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
    /// Specifies the URL of the Schema Registry that should be used to validate data when
    /// deserializing records from the Kafka topic.
    schema_registry_url: String,
    /// Specifies bearer authentication token used to connect to the the Schema Registry.
    schema_registry_bearer_token: Option<String>,
    /// Specifies the basic auth user used to connect to the the Schema Registry.
    schema_registry_user: Option<String>,
    /// Specifies the basic auth password used to connect to the the Schema Registry.
    schema_registry_pass: Option<String>,
    /// Controls how many lines each press of a key scrolls the schema definition text.
    scroll_factor: u16,
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

impl From<SchemasConfig<'_>> for Schemas {
    /// Converts from an owned [`SchemasConfig`] to an owned [`Schemas`].
    fn from(value: SchemasConfig<'_>) -> Self {
        Self::new(value)
    }
}

/// The application [`Component`] that is responsible for displaying data from the Schema Registry
/// if one is configured.
pub struct Schemas {
    /// Client used to query the schema registry.
    client: SchemaRegistryClient,
    /// Current state of the component and it's underlying widgets.
    state: SchemasState,
    /// Controls how many lines each press of a key scrolls the schema definition text.
    scroll_factor: u16,
    /// Color scheme for the component.
    theme: SchemasTheme,
}

// TODO: extract schema registry interactions into a seperate module outside of the UI component?
// If we did not want to use futures::executor::block_on() in the UI component, then the Component
// trait would need to be an async trait. That is not a requirement today.

impl Schemas {
    /// Creates a new [`Schemas`] component using the specified [`SchemasConfig`].
    pub fn new(config: SchemasConfig) -> Self {
        // TODO: share client with deserializer
        let mut client_config = ClientConfig::new(vec![config.schema_registry_url]);
        if let Some(bearer) = config.schema_registry_bearer_token.as_ref() {
            tracing::info!("configuring bearer token auth for schema registry client");
            client_config.bearer_access_token = Some(bearer.clone());
        }

        if let Some(user) = config.schema_registry_user.as_ref() {
            tracing::info!("configuring basic auth for schema registry client");
            client_config.basic_auth = Some((user.clone(), config.schema_registry_pass.clone()));
        }

        let client = SchemaRegistryClient::new(client_config);

        Self {
            client,
            state: SchemasState::new(),
            scroll_factor: config.scroll_factor,
            theme: config.theme.into(),
        }
    }
    /// Loads the non-deleted subjects from the schema registry.
    fn load_subjects(&mut self) {
        let result =
            futures::executor::block_on(async { self.client.get_all_subjects(false).await });

        match result {
            Ok(subjects) => {
                tracing::debug!(
                    "loaded {} subjects from the schema registry",
                    subjects.len()
                );
                self.state.subjects = subjects;
            }
            Err(e) => {
                tracing::error!("error loading subjects from schema registry: {}", e);
                self.state.subjects = Vec::default();
            }
        }
    }
    /// Renders the list of subjects.
    fn render_subjects(&mut self, frame: &mut Frame, area: Rect) {
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
            .subjects
            .iter()
            .map(|s| ListItem::new(s.as_str()))
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
                .content_length(self.state.subjects.len());

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

        let schema_value: serde_json::Value =
            serde_json::from_str(&schema.schema).expect("valid schema JSON");

        let schema_pretty = serde_json::to_string_pretty(&schema_value).expect("valid schema");

        let schema_paragraph = Paragraph::new(schema_pretty)
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

        if let Some(schema) = self.state.selected_schema.as_ref() {
            let list_items: Vec<ListItem> = schema
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

            if let Some(schema) = self.state.selected_schema.as_ref()
                && schema.available_versions.len() > 1
            {
                self.state.versions_scroll_state = self
                    .state
                    .versions_scroll_state
                    .content_length(schema.available_versions.len());

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
            }
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
    /// Selects the first subject in the list and loads it's latest schema details.
    fn select_first_subject(&mut self) {
        self.state.select_first_subject();
        self.load_selected_subject_latest();
    }
    /// Selects the next subject in the list and loads it's latest schema details.
    fn select_next_subject(&mut self) {
        self.state.select_next_subject();
        self.load_selected_subject_latest();
    }
    /// Selects the previous subject in the list and loads it's latest schema details.
    fn select_prev_subject(&mut self) {
        self.state.select_prev_subject();
        self.load_selected_subject_latest();
    }
    /// Selects the last subject in the list and loads it's latest schema details.
    fn select_last_subject(&mut self) {
        self.state.select_last_subject();
        self.load_selected_subject_latest();
    }
    /// Selects the first schema version in the list and loads the details.
    fn select_first_schema_version(&mut self) {
        if let Some(version) = self.state.select_first_schema_version() {
            self.load_selected_subject_at_version(version);
        }
    }
    /// Selects the next schema version in the list and loads the details.
    fn select_next_schema_version(&mut self) {
        if let Some(version) = self.state.select_next_schema_version() {
            self.load_selected_subject_at_version(version);
        }
    }
    /// Selects the prev schema version in the list and loads the details.
    fn select_prev_schema_version(&mut self) {
        if let Some(version) = self.state.select_prev_schema_version() {
            self.load_selected_subject_at_version(version);
        }
    }
    /// Selects the last schema version in the list and loads the details.
    fn select_last_schema_version(&mut self) {
        if let Some(version) = self.state.select_last_schema_version() {
            self.load_selected_subject_at_version(version);
        }
    }
    /// Loads the schema details for the latest version of the currently selected subject.
    fn load_selected_subject_latest(&mut self) {
        if let Some(selected) = self.state.selected_subject.as_ref() {
            tracing::info!("loading latest schema version for subject {}", selected);

            // TODO: error handling
            let registered_schema = self
                .load_subject_at_version(selected, None)
                .expect("subject can be loaded");

            let versions = self
                .load_versions(selected)
                .expect("versions can be loaded");

            let schema = Schema::new(registered_schema, versions);

            // TODO: cache schema version data with a TTL or keep reloading it every time?

            self.state.selected_schema = Some(schema);
            self.state.versions_list_state.select_first();
        }
    }
    /// Loads the schema details for the specified version of the currently selected subject.
    fn load_selected_subject_at_version(&mut self, version: i32) {
        if let Some(selected) = self.state.selected_subject.as_ref() {
            tracing::info!(
                "loading schema version {} for subject {}",
                selected,
                version
            );

            // TODO: error handling
            let registered_schema = self
                .load_subject_at_version(selected, Some(version))
                .expect("subject can be loaded");

            let schema = Schema::new(
                registered_schema,
                self.state
                    .selected_schema
                    .take()
                    .expect("schema selected")
                    .available_versions,
            );

            // TODO: cache schema version data with a TTL or keep reloading it every time?

            self.state.selected_schema = Some(schema);
        }
    }
    /// Loads all available versions for the specified subject from the schema registry.
    fn load_versions(&self, subject: impl AsRef<str>) -> anyhow::Result<Vec<i32>> {
        futures::executor::block_on(async {
            self.client
                .get_all_versions(subject.as_ref())
                .await
                .context(format!("load versions for subject {}", subject.as_ref()))
        })
    }
    /// Loads the schema details for the specified version of the given subject from the schema
    /// registry. If no version is specified, the latest version is fetched.
    fn load_subject_at_version(
        &self,
        subject: impl AsRef<str>,
        version: Option<i32>,
    ) -> anyhow::Result<RegisteredSchema> {
        match version {
            Some(version) => futures::executor::block_on(async {
                self.client
                    .get_version(subject.as_ref(), version, false, None)
                    .await
                    .context(format!(
                        "load schema version {} for subject {}",
                        version,
                        subject.as_ref()
                    ))
            }),
            None => futures::executor::block_on(async {
                self.client
                    .get_latest_version(subject.as_ref(), None)
                    .await
                    .context(format!(
                        "load latest schema version for subject {}",
                        subject.as_ref()
                    ))
            }),
        }
    }
}

impl Component for Schemas {
    // Returns the name of the [`Component`] which is displayed to the user as a menu item.
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

            self.render_subjects(frame, left_panel);
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

            self.render_subjects(frame, left_panel);

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
    fn map_key_event(&self, event: KeyEvent, buffered: Option<&BufferedKeyPress>) -> Option<Event> {
        match event.code {
            KeyCode::Char(c) => match c {
                // TODO: implement schema export
                'e' => None,
                _ => match self.state.active_widget {
                    SchemasWidget::Subjects => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollSubjects(ScrollPosition::Top))
                        }
                        'j' => Some(Event::ScrollSubjects(ScrollPosition::Down)),
                        'k' => Some(Event::ScrollSubjects(ScrollPosition::Up)),
                        'G' => Some(Event::ScrollSubjects(ScrollPosition::Bottom)),
                        _ => None,
                    },
                    SchemasWidget::Schema => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollSchemaDefinition(ScrollPosition::Top))
                        }
                        'j' => Some(Event::ScrollSchemaDefinition(ScrollPosition::Down)),
                        'k' => Some(Event::ScrollSchemaDefinition(ScrollPosition::Up)),
                        _ => None,
                    },
                    SchemasWidget::Versions => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollSchemaVersions(ScrollPosition::Top))
                        }
                        'j' => Some(Event::ScrollSchemaVersions(ScrollPosition::Down)),
                        'k' => Some(Event::ScrollSchemaVersions(ScrollPosition::Up)),
                        'G' => Some(Event::ScrollSchemaVersions(ScrollPosition::Bottom)),
                        _ => None,
                    },
                    SchemasWidget::References => match c {
                        'g' if buffered.filter(|kp| kp.is('g')).is_some() => {
                            Some(Event::ScrollSchemaReferences(ScrollPosition::Top))
                        }
                        'j' => Some(Event::ScrollSchemaReferences(ScrollPosition::Down)),
                        'k' => Some(Event::ScrollSchemaReferences(ScrollPosition::Up)),
                        'G' => Some(Event::ScrollSchemaReferences(ScrollPosition::Bottom)),
                        _ => None,
                    },
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
            Event::ScrollSubjects(scroll_position) => match scroll_position {
                ScrollPosition::Top => self.select_first_subject(),
                ScrollPosition::Down => self.select_next_subject(),
                ScrollPosition::Up => self.select_prev_subject(),
                ScrollPosition::Bottom => self.select_last_subject(),
            },
            Event::ScrollSchemaDefinition(scroll_position) => match scroll_position {
                ScrollPosition::Top => self.state.scroll_schema_definition_top(),
                ScrollPosition::Down => {
                    self.state.scroll_schema_definition_down(self.scroll_factor)
                }
                ScrollPosition::Up => self.state.scroll_schema_definition_up(self.scroll_factor),
                ScrollPosition::Bottom => {}
            },
            Event::ScrollSchemaVersions(scroll_position) => match scroll_position {
                ScrollPosition::Top => self.select_first_schema_version(),
                ScrollPosition::Down => self.select_next_schema_version(),
                ScrollPosition::Up => self.select_prev_schema_version(),
                ScrollPosition::Bottom => self.select_last_schema_version(),
            },
            Event::ScrollSchemaReferences(scroll_position) => match scroll_position {
                ScrollPosition::Top => self.state.scroll_references_top(),
                ScrollPosition::Down => self.state.scroll_references_down(),
                ScrollPosition::Up => self.state.scroll_references_up(),
                ScrollPosition::Bottom => self.state.scroll_references_bottom(),
            },
            _ => {}
        }
    }
    /// Allows the [`Component`] to render the status line text into the footer.
    fn render_status_line(&self, frame: &mut Frame, area: Rect) {
        let line = Line::from_iter([
            Span::styled("Subjects: ", Style::from(self.theme.label_color).bold()),
            Span::raw(self.state.subjects.len().to_string()),
        ]);

        let text = Paragraph::new(line).left_aligned();

        frame.render_widget(text, area);
    }
    /// Allows the [`Component`] to render the key bindings text into the footer.
    fn render_key_bindings(&self, frame: &mut Frame, area: Rect) {
        let mut key_bindings = Vec::from(SCHEMAS_KEY_BINDINGS);

        if self.state.selected_schema.is_some() {
            key_bindings.push(super::KEY_BINDING_EXPORT);
        }

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
        }

        let text = Paragraph::new(key_bindings.join(" | "))
            .style(self.theme.key_bindings_text_color)
            .right_aligned();

        frame.render_widget(text, area);
    }
    /// Hook for the [`Component`] to run any logic required when it becomes active.
    fn on_activate(&mut self) {
        self.load_subjects();
    }
}

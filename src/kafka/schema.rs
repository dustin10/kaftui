use anyhow::Context;
use async_trait::async_trait;
use schema_registry_client::rest::{
    models::{RegisteredSchema, SchemaReference},
    schema_registry_client::{Client, SchemaRegistryClient},
};
use serde::Serialize;
use std::fmt::Display;

/// String presented to the user when a schema-releated value is missing or not known.
const UNKNOWN: &str = "<unknown>";

/// Represents a reference to another schema contained in a schema retrieved from the schema
/// registry.
#[derive(Debug, Serialize)]
pub struct SchemaRef {
    /// Name of the referenced schema.
    pub name: String,
    /// Subject the referenced schema belongs to.
    pub subject: String,
    /// Version of the referenced schema.
    pub version: i32,
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

/// Represents a schema retrieved from the schema registry.
#[derive(Debug, Serialize)]
pub struct Schema {
    /// Identifier of the schema.
    pub id: i32,
    /// Globally unique identifier of the schema.
    pub guid: String,
    /// Version of the schema.
    pub version: Version,
    /// The schema type, i.e. AVRO, JSON, PROTOBUF.
    pub kind: String,
    /// The schema definition.
    pub schema: String,
    /// References to other schemas contained in this schema.
    pub references: Option<Vec<SchemaRef>>,
}

impl From<RegisteredSchema> for Schema {
    /// Converts from a [`RegisteredSchema`] fetched from the schema registry to a new [`Schema`].
    fn from(value: RegisteredSchema) -> Self {
        Self::new(value)
    }
}

impl Schema {
    /// Creates a new [`Schema`] from the given [`RegisteredSchema`] fetched from the schema
    /// registry.
    pub fn new(registered_schema: RegisteredSchema) -> Self {
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
            version: version.into(),
            kind,
            schema,
            references,
        }
    }
}

/// Represents a subject in the schema registry.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Subject(String);

impl From<String> for Subject {
    /// Converts from a `String` to a new [`Subject`].
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<Subject> for String {
    /// Converts from a [`Subject`] to its inner `String` representation.
    fn from(value: Subject) -> Self {
        value.0
    }
}

impl AsRef<str> for Subject {
    /// Returns a reference to the inner `String` representation of the [`Subject`].
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for Subject {
    /// Writes the inner `String` representation of the [`Subject`] to the given formatter.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents a version of a schema in the schema registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Version(i32);

impl From<i32> for Version {
    /// Converts from an `i32` to a new [`Version`].
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<Version> for i32 {
    /// Converts from a [`Version`] to its inner `i32` representation.
    fn from(value: Version) -> Self {
        value.0
    }
}

impl Display for Version {
    /// Writes the inner `i32` representation of the [`Version`] to the given formatter.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The [`SchemaClient`] trait defines the behavior required to interact with a schema registry
/// to retrieve subjects and schemas.
#[async_trait]
pub trait SchemaClient {
    /// Loads all of the non-deleted subjects from the schema registry.
    async fn get_subjects(&self) -> anyhow::Result<Vec<Subject>>;
    /// Loads the schema for the specified version of the given subject from the schema registry.
    /// If no version is specified, then the latest version is retrieved.
    async fn get_schema(
        &self,
        subject: &Subject,
        version: Option<Version>,
    ) -> anyhow::Result<Schema>;
    /// Loads all available versions for the specified subject from the schema registry.
    async fn get_schema_versions(&self, subject: &Subject) -> anyhow::Result<Vec<Version>>;
}

/// An implementation of the [`SchemaClient`] trait which interacts with the schema registry over
/// HTTP using a pre-configured [`SchemaRegistryClient`].
#[derive(Clone)]
pub struct RestSchemaRegistry {
    /// The schema registry client used to interact with the schema registry.
    client: SchemaRegistryClient,
}

impl RestSchemaRegistry {
    /// Creates a new [`RestSchemaRegistry`] which uses the provided schema registry client to
    /// interact with the schema registry over HTTP.
    pub fn new(client: SchemaRegistryClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SchemaClient for RestSchemaRegistry {
    /// Loads all of the non-deleted subjects from the schema registry.
    async fn get_subjects(&self) -> anyhow::Result<Vec<Subject>> {
        self.client
            .get_all_subjects(false)
            .await
            .context("load subjects from registry")
            .map(|ss| ss.into_iter().map(Into::into).collect::<Vec<Subject>>())
    }
    /// Loads the schema for the specified version of the given subject from the schema registry.
    /// If no version is specified, then the latest version is retrieved.
    async fn get_schema(
        &self,
        subject: &Subject,
        version: Option<Version>,
    ) -> anyhow::Result<Schema> {
        match version {
            Some(version) => self
                .client
                .get_version(subject.as_ref(), version.into(), false, None)
                .await
                .context(format!(
                    "load schema version {} for subject {} from registry",
                    version,
                    subject.as_ref()
                ))
                .map(Into::into),
            None => self
                .client
                .get_latest_version(subject.as_ref(), None)
                .await
                .context(format!(
                    "load latest schema version for subject {} from registry",
                    subject.as_ref()
                ))
                .map(Into::into),
        }
    }
    /// Loads all available versions for the specified subject from the schema registry.
    async fn get_schema_versions(&self, subject: &Subject) -> anyhow::Result<Vec<Version>> {
        self.client
            .get_all_versions(subject.as_ref())
            .await
            .context("load schema versions from registry")
            .map(|vs| vs.into_iter().map(Into::into).collect::<Vec<Version>>())
    }
}

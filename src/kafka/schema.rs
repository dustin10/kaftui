use anyhow::Context;
use schema_registry_client::rest::{
    models::{RegisteredSchema, SchemaReference},
    schema_registry_client::Client,
};
use serde::Serialize;
use std::{borrow::Cow, collections::HashMap, fmt::Display, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::time::Instant;

/// String presented to the user when a schema-releated value is missing or not known.
const UNKNOWN_SCHEMA_KIND: &str = "<unknown>";

/// String that maps to the type value for an Avro schema returned from the schema registry.
const AVRO_SCHEMA_KIND: &str = "AVRO";

/// String that maps to the type value for a JSON schema returned from the schema registry.
const JSON_SCHEMA_KIND: &str = "JSON";

/// Default time-to-live for cached schema registry responses - 5 minutes.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Represents a reference to another schema contained in a schema retrieved from the schema
/// registry.
#[derive(Clone, Debug, Serialize)]
pub struct SchemaRef {
    /// Name of the referenced schema.
    pub name: String,
    /// Subject the referenced schema belongs to.
    pub subject: Subject,
    /// Version of the referenced schema.
    pub version: Version,
}

impl From<SchemaReference> for SchemaRef {
    /// Converts from a [`SchemaReference`] fetched from the schema registry to a new
    /// [`SchemaRef`].
    fn from(value: SchemaReference) -> Self {
        Self {
            name: value
                .name
                .unwrap_or_else(|| UNKNOWN_SCHEMA_KIND.to_string()),
            subject: value
                .subject
                .unwrap_or_else(|| UNKNOWN_SCHEMA_KIND.to_string())
                .into(),
            version: value.version.unwrap_or_default().into(),
        }
    }
}

/// Represents a schema retrieved from the schema registry.
#[derive(Clone, Debug, Serialize)]
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
            .unwrap_or_else(|| UNKNOWN_SCHEMA_KIND.to_string());

        let version = registered_schema.version.unwrap_or_default();

        let kind = registered_schema
            .schema_type
            .unwrap_or_else(|| UNKNOWN_SCHEMA_KIND.to_string());

        let schema = match registered_schema.schema {
            Some(s) => match kind.as_str() {
                AVRO_SCHEMA_KIND | JSON_SCHEMA_KIND => serde_json::from_str(&s)
                    .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
                    .unwrap_or_else(|e| {
                        tracing::warn!("failed to pretty-print schema JSON: {}", e);
                        s
                    }),
                _ => s,
            },
            None => UNKNOWN_SCHEMA_KIND.to_string(),
        };

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

impl<'a> From<Subject> for Cow<'a, str> {
    /// Converts from a [`Subject`] to a [`Cow`] containing its inner `String` representation.
    fn from(value: Subject) -> Self {
        Cow::Owned(value.0)
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

/// A cached value paired with the time it was inserted, used to determine cache expiration.
struct CacheEntry<T> {
    /// Value of the cache entry.
    value: T,
    /// Timestamp at which that the entry was cached.
    inserted_at: Instant,
}

impl<T> CacheEntry<T> {
    /// Creates a new cache entry with the given value and the current time as the insertion
    /// timestamp.
    fn new(value: T) -> Self {
        Self {
            value,
            inserted_at: Instant::now(),
        }
    }
    /// Returns true if the entry has been cached for longer than the specified time-to-live
    /// duration.
    fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() >= ttl
    }
}

/// Caches data fetched from API calls made to the schema registry to fetch subject and schema
/// details.
struct SchemaCache {
    /// Caches the resolved [`Schema`] for a given [`Subject`] and [`Version`] if applicable.
    schemas: HashMap<(Subject, Option<Version>), CacheEntry<Schema>>,
    /// Caches the set of [`Version`]s for a given [`Subject`].
    versions: HashMap<Subject, CacheEntry<Vec<Version>>>,
}

impl SchemaCache {
    /// Creates an empty [`SchemaCache`].
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            versions: HashMap::new(),
        }
    }
}

/// Interacts with the schema registry over HTTP using a pre-configured [`SchemaRegistryClient`].
/// Caches responses with a configurable time-to-live (TTL) to reduce redundant network calls as,
/// in general, data in the schema registry tends to not be updated at a high frequency.
#[derive(Clone)]
pub struct SchemaClient<C>
where
    C: Client,
{
    /// A shared reference to the schema registry [`Client`] used to interact with the schema
    /// registry.
    client: Arc<C>,
    /// A shared, async cache of data fetched from the schema registry.
    cache: Arc<RwLock<SchemaCache>>,
    /// Duration of time for which cached entries are considered valid.
    ttl: Duration,
}

impl<C> SchemaClient<C>
where
    C: Client + Send + Sync,
{
    /// Creates a new [`SchemaClient`] which uses the provided [`Client`] to interact with the
    /// schema registry over HTTP. Responses are cached for the specified time-to-live duration.
    pub fn new(client: Arc<C>, ttl: Duration) -> Self {
        Self {
            client,
            cache: Arc::new(RwLock::new(SchemaCache::new())),
            ttl,
        }
    }
    /// Loads all of the non-deleted subjects from the schema registry.
    pub async fn get_subjects(&self) -> anyhow::Result<Vec<Subject>> {
        let subjects = self
            .client
            .get_all_subjects(false)
            .await
            .context("load subjects from registry")
            .map(|ss| ss.into_iter().map(Into::into).collect::<Vec<Subject>>())?;

        Ok(subjects)
    }
    /// Loads the schema for the specified version of the given subject from the schema registry.
    /// If no version is specified, then the latest version is retrieved.
    pub async fn get_schema(
        &self,
        subject: &Subject,
        version: Option<Version>,
    ) -> anyhow::Result<Schema> {
        let cache_key = (subject.clone(), version);

        let cached = {
            let cache = self.cache.read().await;
            cache
                .schemas
                .get(&cache_key)
                .filter(|e| !e.is_expired(self.ttl))
                .map(|e| e.value.clone())
        };

        if let Some(schema) = cached {
            return Ok(schema);
        }

        let schema: Schema = match version {
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
        }?;

        {
            let mut cache = self.cache.write().await;
            if cache
                .schemas
                .get(&cache_key)
                .is_none_or(|e| e.is_expired(self.ttl))
            {
                cache
                    .schemas
                    .insert(cache_key, CacheEntry::new(schema.clone()));
            }
        }

        Ok(schema)
    }
    /// Loads all available versions for the specified subject from the schema registry.
    pub async fn get_schema_versions(&self, subject: &Subject) -> anyhow::Result<Vec<Version>> {
        let cached = {
            let cache = self.cache.read().await;
            cache
                .versions
                .get(subject)
                .filter(|e| !e.is_expired(self.ttl))
                .map(|e| e.value.clone())
        };

        if let Some(versions) = cached {
            return Ok(versions);
        }

        let versions = self
            .client
            .get_all_versions(subject.as_ref())
            .await
            .context("load schema versions from registry")
            .map(|vs| vs.into_iter().map(Into::into).collect::<Vec<Version>>())?;

        {
            let mut cache = self.cache.write().await;
            if cache
                .versions
                .get(subject)
                .is_none_or(|e| e.is_expired(self.ttl))
            {
                cache
                    .versions
                    .insert(subject.clone(), CacheEntry::new(versions.clone()));
            }
        }

        Ok(versions)
    }
}

use crate::kafka::Consumer;

use anyhow::Context;
use derive_builder::Builder;
use rdkafka::{
    ClientConfig, ClientContext,
    admin::{AdminClient as RDAdminClient, AdminOptions, ConfigEntry, ResourceSpecifier},
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    metadata::{MetadataPartition, MetadataTopic},
};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Represents a partition of a Kafka topic including the IDs of the current leader and replica
/// brokers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Partition {
    /// ID of the partition.
    pub id: i32,
    /// ID of the leader broker for the partition.
    pub leader: i32,
    /// IDs of the replica brokers for the partition.
    pub replicas: Vec<i32>,
}

impl From<&MetadataPartition> for Partition {
    /// Converts from a reference to a [`MetadataPartition`] to an owned [`Partition`].
    fn from(value: &MetadataPartition) -> Self {
        Self {
            id: value.id(),
            leader: value.leader(),
            replicas: value.replicas().into(),
        }
    }
}

/// Represents a Kafka topic including it's name and partitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Topic {
    /// Name of the topic.
    pub name: String,
    /// Partition details for the topic.
    pub partitions: Vec<Partition>,
}

impl PartialOrd for Topic {
    /// Compares two [`Topic`] instances for ordering.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Topic {
    /// Compares two [`Topic`] instances based on their names for ordering.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl From<&MetadataTopic> for Topic {
    /// Converts from a reference to a [`MetadataTopic`] to an owned [`Topic`].
    fn from(value: &MetadataTopic) -> Self {
        let name = String::from(value.name());
        let partitions = value.partitions().iter().map(Into::into).collect();

        Self { name, partitions }
    }
}

/// Represents the configuration details for a Kafka topic.
#[derive(Clone, Debug)]
pub struct TopicConfig(Vec<TopicConfigEntry>);

impl TopicConfig {
    /// Returns a slice of all [`TopicConfigEntry`] values for the topic.
    pub fn entries(&self) -> &[TopicConfigEntry] {
        &self.0
    }
}

impl IntoIterator for TopicConfig {
    type Item = TopicConfigEntry;
    type IntoIter = std::vec::IntoIter<TopicConfigEntry>;

    /// Creates an iterator that consumes the [`TopicConfig`] and yields owned
    /// [`TopicConfigEntry`] items.
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Represents a single configuration entry for a Kafka topic.
#[derive(Clone, Debug)]
pub struct TopicConfigEntry {
    /// Key of the configuration entry.
    pub key: String,
    /// Values of the configuration entry.
    pub value: Option<String>,
    /// Indicates if the configuration entry is a default value.
    pub is_default: bool,
}

impl From<ConfigEntry> for TopicConfigEntry {
    /// Converts from an owned [`ConfigEntry`] to an owned [`TopicConfigEntry`].
    fn from(value: ConfigEntry) -> Self {
        Self {
            key: value.name,
            value: value.value,
            is_default: value.is_default,
        }
    }
}

/// Custom client context for the admin client to handle logging.
struct AdminClientContext;

impl ClientContext for AdminClientContext {
    /// Logs messages from the underlying rdkafka library using the tracing crate.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                tracing::error!("{} {}", fac, log_message);
            }
            RDKafkaLogLevel::Warning => tracing::warn!("{} {}", fac, log_message),
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!("{} {}", fac, log_message);
            }
            RDKafkaLogLevel::Debug => tracing::debug!("{} {}", fac, log_message),
        }
    }
}

/// Defines the configuration used to create a new instance of [`AdminClient`].
#[derive(Builder, Clone)]
pub struct AdminClientConfig {
    /// Configuration properties used to bootstrap the underlying admin client.
    properties: HashMap<String, String>,
    /// Optional request timeout for admin operations.
    #[builder(setter(into, strip_option), default)]
    request_timeout: Option<Duration>,
    /// Optional operation timeout for admin operations.
    #[builder(setter(into, strip_option), default)]
    operation_timeout: Option<Duration>,
    /// Shared reference to the Kafka consumer used to fetch topic metadata.
    consumer: Arc<Consumer>,
}

impl AdminClientConfig {
    /// Creates a new default [`AdminClientConfigBuilder`] which can be used to create a new
    /// [`AdminClientConfig`].
    pub fn builder() -> AdminClientConfigBuilder {
        AdminClientConfigBuilder::default()
    }
}

// TODO: add a cache layer with TTL?

/// The Kafka admin client used to perform administrative operations on the Kafka cluster.
pub struct AdminClient {
    /// Underlying rdkafka admin client.
    client: RDAdminClient<AdminClientContext>,
    /// Admin options used for rdkafka client operations.
    admin_options: AdminOptions,
    /// Request timeout duration for admin operations.
    request_timeout: Duration,
    /// Shared reference to the Kafka consumer used to list topics.
    consumer: Arc<Consumer>,
}

impl AdminClient {
    /// Creates a new instance of [`AdminClient`] using the specified [`AdminClientConfig`].
    pub fn new(config: AdminClientConfig) -> anyhow::Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config.extend(config.properties);

        let client = RDAdminClient::from_config_and_context(&client_config, AdminClientContext)
            .context("create rdkafka admin client")?;

        let admin_options = AdminOptions::new()
            .request_timeout(config.request_timeout)
            .operation_timeout(config.operation_timeout);

        Ok(Self {
            client,
            admin_options,
            request_timeout: config
                .request_timeout
                .unwrap_or_else(|| Duration::from_secs(30)),
            consumer: config.consumer,
        })
    }
    /// Loads all known topics from the Kafka cluster.
    pub async fn load_topics(&self) -> anyhow::Result<Vec<Topic>> {
        // TODO: maybe this should not be here as it is really just a pass-through?
        self.consumer
            .fetch_topic_metadata(None, self.request_timeout)
    }
    /// Loads the configuration details for the specified topic from the Kafka cluster.
    pub async fn load_topic_config(
        &self,
        topic: impl AsRef<str>,
    ) -> anyhow::Result<Option<TopicConfig>> {
        let resource = ResourceSpecifier::Topic(topic.as_ref());

        let result = self
            .client
            .describe_configs(&[resource], &self.admin_options)
            .await
            .context("load topic config")?
            .into_iter()
            .next();

        match result {
            None => Ok(None),
            Some(Err(e)) => Err(e).context("load topic config"),
            Some(Ok(config)) => {
                let entries = config
                    .entries
                    .into_iter()
                    .map(TopicConfigEntry::from)
                    .collect::<Vec<TopicConfigEntry>>();

                Ok(Some(TopicConfig(entries)))
            }
        }
    }
}

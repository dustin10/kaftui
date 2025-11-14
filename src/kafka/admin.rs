use anyhow::Context;
use derive_builder::Builder;
use rdkafka::{
    admin::{AdminClient as RDAdminClient, AdminOptions, ConfigEntry, ResourceSpecifier},
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    ClientConfig, ClientContext,
};
use std::{collections::HashMap, time::Duration};

#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct TopicConfig(Vec<TopicConfigEntry>);

#[derive(Clone, Debug)]
pub struct TopicMetadata {}

#[derive(Clone, Debug)]
pub struct TopicConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub is_default: bool,
}

impl From<ConfigEntry> for TopicConfigEntry {
    fn from(value: ConfigEntry) -> Self {
        Self {
            name: value.name,
            value: value.value,
            is_default: value.is_default,
        }
    }
}

struct AdminClientContext;

impl ClientContext for AdminClientContext {
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

#[derive(Builder, Clone, Debug)]
pub struct AdminClientConfig {
    properties: HashMap<String, String>,
    request_timeout: Option<Duration>,
    operation_timeout: Option<Duration>,
}

impl AdminClientConfig {
    pub fn builder() -> AdminClientConfigBuilder {
        AdminClientConfigBuilder::default()
    }
}

pub struct AdminClient {
    client: RDAdminClient<AdminClientContext>,
    admin_options: AdminOptions,
}

impl AdminClient {
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
        })
    }
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

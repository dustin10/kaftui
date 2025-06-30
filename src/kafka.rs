use std::collections::HashMap;

use anyhow::Context;
use futures::TryStreamExt;
use rdkafka::{
    consumer::{
        BaseConsumer, CommitMode, Consumer as RDKafkaConsumer, ConsumerContext, Rebalance,
        StreamConsumer,
    },
    error::KafkaResult,
    ClientConfig, ClientContext, TopicPartitionList,
};

/// Contains the data in the record consumed from a Kafka topic.
#[derive(Clone, Debug, Default)]
pub struct Record {
    /// Name of the topic that the record was consumed from.
    pub topic: String,
    /// Partition number the record was assigned in the topic.
    pub partition: u16,
    /// Contains any headers from the Kafka record.
    pub headers: HashMap<String, String>,
    /// Value of the Kafka record.
    pub value: String,
}

/// The [`ConsumeContext`] is a struct that is used to implement a custom Kafka consumer context
/// to hook into key events in the lifecycle of a Kafka consumer.
struct ConsumeContext;

impl ClientContext for ConsumeContext {}

impl ConsumerContext for ConsumeContext {
    /// Hook invoked right before the consumer begins rebalancing.
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        tracing::debug!("rebalance initiated: {:?}", rebalance);
    }
    /// Hook invoked after the consumer rebalancing has been completed.
    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                tpl.elements().iter().for_each(|e| {
                    tracing::info!("assigned partition {} on {}", e.partition(), e.topic())
                });
            }
            Rebalance::Revoke(tpl) => {
                tpl.elements().iter().for_each(|e| {
                    tracing::info!("revoked partition {} on {}", e.partition(), e.topic())
                });
            }
            Rebalance::Error(err) => tracing::error!("error during rebalance: {}", err),
        }
    }
    /// Hook invoked after the consumer has attempted to commit offsets.
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => {
                if tracing::event_enabled!(tracing::Level::DEBUG) {
                    offsets.elements().iter().for_each(|e| {
                        tracing::debug!(
                            "committed offset {:?} on partition {} in topic {}",
                            e.offset(),
                            e.partition(),
                            e.topic()
                        )
                    });
                }
            }
            Err(e) => {
                tracing::error!("error committing consumer offsets: {}", e);
            }
        }
    }
}

/// Contains the data used to initialize and configure the Kafka consumer.
#[derive(Debug)]
pub struct ConsumerConfig {
    /// Kafka broker host.
    pub bootstrap_servers: String,
    /// Name of the consumer group used when reading a topic.
    pub group_id: Option<String>,
}

pub struct Consumer {
    consumer: StreamConsumer<ConsumeContext>,
}

impl Consumer {
    pub fn with_config(config: ConsumerConfig) -> anyhow::Result<Self> {
        let group_id = config
            .group_id
            .unwrap_or_else(|| String::from("kaftui-consumer"));

        let mut client_config = ClientConfig::new();
        client_config.set("group.id", group_id);
        client_config.set("bootstrap.servers", config.bootstrap_servers);
        client_config.set("statistics.interval.ms", "60000");
        client_config.set("auto.offset.reset", "latest");
        client_config.set("enable.auto.commit", "false");

        // TODO
        client_config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);

        let consumer: StreamConsumer<ConsumeContext> =
            client_config.create_with_context(ConsumeContext)?;

        // TODO
        let topics = vec!["kaftui-test"];

        consumer
            .subscribe(&topics)
            .context(format!("subscribe to Kafka topic(s): {:?}", topics))?;

        Ok(Self { consumer })
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        let stream_procesor = self.consumer.stream().try_for_each(|msg| async move {
            if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                tracing::error!("error committing message: {}", err);
            }

            Ok(())
        });

        stream_procesor.await.context("process Kafka record stream")
    }
}

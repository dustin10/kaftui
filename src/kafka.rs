use crate::event::{AppEvent, EventBus};

use anyhow::Context;
use derive_builder::Builder;
use futures::TryStreamExt;
use rdkafka::{
    consumer::{
        BaseConsumer, CommitMode, Consumer as RDKafkaConsumer, ConsumerContext, Rebalance,
        StreamConsumer,
    },
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Contains the data in the record consumed from a Kafka topic.
#[derive(Clone, Debug, Default)]
pub struct Record {
    /// Name of the topic that the record was consumed from.
    pub topic: String,
    /// Partition number the record was assigned in the topic.
    pub partition: i32,
    /// Partition key for the record if one was set.
    pub partition_key: Option<String>,
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
#[derive(Builder, Clone, Debug)]
pub struct ConsumerConfig {
    /// Kafka server hosts.
    bootstrap_servers: String,
    /// Id of the group used when consuming a topic.
    group_id: String,
}

impl ConsumerConfig {
    /// Creates a new default instance of [`ConsumerConfig`].
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }
}

impl Default for ConsumerConfig {
    /// Creates a new instance of [`ConsumerConfig`] initialized to default values.
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from("localhost:29092"),
            group_id: String::from("kaftui-consumer"),
        }
    }
}

pub struct Consumer {
    /// Configuration for the underlying Kafka consumer.
    consumer_config: ConsumerConfig,
    /// Bus that Kafka-related application events are published to.
    event_bus: Arc<Mutex<EventBus>>,
}

impl Consumer {
    /// Creates a new [`Consumer`] with the specified dependencies.
    pub fn new(consumer_config: ConsumerConfig, event_bus: Arc<Mutex<EventBus>>) -> Self {
        Self {
            consumer_config,
            event_bus,
        }
    }
    /// Starts the consumption of messages from the specified topic.
    pub async fn start(&self, topic: String) -> anyhow::Result<()> {
        let task = ConsumerTask::new(self.consumer_config.clone(), Arc::clone(&self.event_bus))?;

        tokio::spawn(async move { task.run(topic).await });

        Ok(())
    }
}

impl From<&BorrowedMessage<'_>> for Record {
    /// Converts from a reference to a [`BorrowedMessage`] to a [`Record`].
    fn from(msg: &BorrowedMessage<'_>) -> Self {
        let partition_key = msg
            .key()
            .and_then(|k| std::str::from_utf8(k).ok())
            .map(ToString::to_string);

        let headers: HashMap<String, String> = match msg.headers() {
            Some(hs) => {
                let mut headers = HashMap::new();
                for h in hs.iter() {
                    let value = match std::str::from_utf8(h.value.unwrap()) {
                        Ok(s) => String::from(s),
                        Err(e) => {
                            tracing::warn!("invalid UTF8 header value: {}", e);
                            String::from("")
                        }
                    };

                    headers.insert(String::from(h.key), value);
                }

                headers
            }
            None => HashMap::new(),
        };

        let value = match msg.payload_view::<str>() {
            Some(Ok(data)) => String::from(data),
            Some(Err(e)) => {
                tracing::error!("invalid string value in message: {}", e);
                String::from("")
            }
            None => String::from(""),
        };

        Self {
            partition: msg.partition(),
            topic: String::from(msg.topic()),
            partition_key,
            headers,
            value,
        }
    }
}

/// A task which is executed in a background thread that handles consuming messages from a Kafka
/// topic.
struct ConsumerTask {
    /// Raw Kafka consumer.
    consumer: StreamConsumer<ConsumeContext>,
    /// Bus that Kafka-related application events are published to.
    event_bus: Arc<Mutex<EventBus>>,
}

impl ConsumerTask {
    /// Creates a new [`ConsumerTask`] with the specified dependencies.
    fn new(
        consumer_config: ConsumerConfig,
        event_bus: Arc<Mutex<EventBus>>,
    ) -> anyhow::Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config.set("group.id", consumer_config.group_id);
        client_config.set("bootstrap.servers", consumer_config.bootstrap_servers);
        client_config.set("statistics.interval.ms", "60000");
        client_config.set("auto.offset.reset", "latest");
        client_config.set("enable.auto.commit", "false");

        // TODO
        client_config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);

        let consumer: StreamConsumer<ConsumeContext> =
            client_config.create_with_context(ConsumeContext)?;

        Ok(Self {
            consumer,
            event_bus,
        })
    }
    /// Runs the task by subscribing to the specified topic and then consuming messages from it.
    async fn run(&self, topic: String) -> anyhow::Result<()> {
        let topics = vec![topic.as_str()];

        self.consumer
            .subscribe(&topics)
            .context(format!("subscribe to Kafka topic: {}", topic))?;

        let stream_procesor = self.consumer.stream().try_for_each(|msg| async move {
            let record: Record = (&msg).into();

            let mut event_bus_guard = self.event_bus.lock().await;
            event_bus_guard.send(AppEvent::RecordReceived(record));
            std::mem::drop(event_bus_guard);

            if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                tracing::error!("error committing message: {}", err);
            }

            Ok(())
        });

        stream_procesor.await.context("process Kafka record stream")
    }
}

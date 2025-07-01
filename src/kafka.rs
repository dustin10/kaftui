use crate::event::{AppEvent, EventBus};

use anyhow::Context;
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
    /// Kafka server hosts.
    pub bootstrap_servers: String,
    /// Id of the group used when consuming a topic.
    pub group_id: Option<String>,
}

impl ConsumerConfig {
    /// Creates a new default instance of [`ConsumerConfig`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ConsumerConfig {
    /// Creates a new instance of [`ConsumerConfig`] initialized to default values.
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from("localhost:29092"),
            group_id: None,
        }
    }
}

pub struct Consumer {
    /// Underlying Kafka consumer.
    consumer: StreamConsumer<ConsumeContext>,
    /// Bus that Kafka-related application events are published to.
    event_bus: Arc<Mutex<EventBus>>,
}

impl Consumer {
    pub fn new(config: ConsumerConfig, event_bus: Arc<Mutex<EventBus>>) -> anyhow::Result<Self> {
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

        Ok(Self {
            consumer,
            event_bus,
        })
    }
    pub async fn start(&self, topic: &str) -> anyhow::Result<()> {
        let topics = vec![topic];

        self.consumer
            .subscribe(&topics)
            .context(format!("subscribe to Kafka topic: {}", topic))?;

        let stream_procesor = self.consumer.stream().try_for_each(|msg| async move {
            let record: Record = (&msg).into();

            let mut event_bus = self.event_bus.lock().await;
            event_bus.send(AppEvent::RecordReceived(record));

            if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                tracing::error!("error committing message: {}", err);
            }

            Ok(())
        });

        stream_procesor.await.context("process Kafka record stream")
    }
}

impl From<&BorrowedMessage<'_>> for Record {
    /// Converts from a reference to a [`BorrowedMessage`] to a [`Record`].
    fn from(msg: &BorrowedMessage<'_>) -> Self {
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
            headers,
            value,
        }
    }
}

struct ConsumerTask {
    event_bus: Arc<Mutex<EventBus>>,
}

impl ConsumerTask {
    fn new(event_bus: Arc<Mutex<EventBus>>) -> Self {
        Self { event_bus }
    }
    async fn run(self) -> anyhow::Result<()> {
        todo!()
    }
}

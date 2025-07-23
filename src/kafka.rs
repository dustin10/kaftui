use crate::event::{AppEvent, EventBus};

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use rdkafka::{
    consumer::{
        BaseConsumer, CommitMode, Consumer as RDConsumer, ConsumerContext as RDConsumerContext,
        Rebalance, StreamConsumer,
    },
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

/// Contains the data in the record consumed from a Kafka topic.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    /// Name of the topic that the record was consumed from.
    pub topic: String,
    /// Partition number the record was assigned in the topic.
    pub partition: i32,
    /// Offset of the record in the topic.
    pub offset: i64,
    /// Partition key for the record if one was set.
    pub key: Option<String>,
    /// Contains any headers from the Kafka record.
    pub headers: HashMap<String, String>,
    /// Value of the Kafka record.
    pub value: String,
    /// UTC timestamp represeting when the event was created.
    pub timestamp: DateTime<Utc>,
}

/// The [`ConsumerContext`] is a struct that is used to implement a custom Kafka consumer context
/// to hook into key events in the lifecycle of a Kafka consumer.
struct ConsumerContext;

impl ClientContext for ConsumerContext {}

impl RDConsumerContext for ConsumerContext {
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

pub struct Consumer {
    /// Underlying Kafka consumer.
    consumer: Arc<StreamConsumer<ConsumerContext>>,
    /// Bus that Kafka-related application events are published to.
    event_bus: Arc<EventBus>,
}

impl Consumer {
    /// Creates a new [`Consumer`] with the specified dependencies.
    pub fn new(config: HashMap<String, String>, event_bus: Arc<EventBus>) -> anyhow::Result<Self> {
        let mut client_config = ClientConfig::new();

        // apply default config
        client_config.set("auto.offset.reset", "latest");
        client_config.set("statistics.interval.ms", "60000");

        // apply user config
        client_config.extend(config);

        // apply enforced config
        client_config.set("enable.auto.commit", "false");

        let consumer: StreamConsumer<ConsumerContext> = client_config
            .create_with_context(ConsumerContext)
            .context("create Kafka consumer")?;

        Ok(Self {
            consumer: Arc::new(consumer),
            event_bus,
        })
    }
    /// Starts the consumption of records from the specified topic.
    pub fn start(&self, topic: String, filter: Option<String>) -> anyhow::Result<()> {
        let task = ConsumerTask::new(
            Arc::clone(&self.consumer),
            topic,
            filter,
            Arc::clone(&self.event_bus),
        )
        .context("create background consumer task")?;

        tokio::spawn(async move { task.run().await });

        Ok(())
    }
    /// Pauses the consumption of records from the topic.
    pub fn pause(&self) -> anyhow::Result<()> {
        let assignment = self
            .consumer
            .assignment()
            .context("get consumer partition assignments")?;

        self.consumer
            .pause(&assignment)
            .context("pause consumer assignments")
    }
    /// Resumes the consumption of records from the topic.
    pub fn resume(&self) -> anyhow::Result<()> {
        let assignment = self
            .consumer
            .assignment()
            .context("get consumer partition assignments")?;

        self.consumer
            .resume(&assignment)
            .context("resume consumer assignments")
    }
}

impl From<&BorrowedMessage<'_>> for Record {
    /// Converts from a reference to a [`BorrowedMessage`] to a [`Record`].
    fn from(msg: &BorrowedMessage<'_>) -> Self {
        let key = msg
            .key()
            .and_then(|k| std::str::from_utf8(k).ok())
            .map(ToString::to_string);

        let headers: HashMap<String, String> = match msg.headers() {
            Some(hs) => {
                let mut headers = HashMap::new();
                for h in hs.iter() {
                    let value = match std::str::from_utf8(h.value.expect("header value exists")) {
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

        // TODO: refactor value to Option
        let value = match msg.payload_view::<str>() {
            Some(Ok(data)) => String::from(data),
            Some(Err(e)) => {
                tracing::error!("non-UTF8 string value in message: {}", e);
                String::from("")
            }
            None => String::from(""),
        };

        let timestamp = DateTime::from_timestamp_millis(
            msg.timestamp()
                .to_millis()
                .expect("Kafka message has valid timestamp"),
        )
        .expect("DateTime created from millis");

        Self {
            partition: msg.partition(),
            topic: String::from(msg.topic()),
            key,
            headers,
            value,
            timestamp,
            offset: msg.offset(),
        }
    }
}

/// A view of a [`Record`] that can be more easily filtered using a JSONPath query.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FilterableRecord {
    /// Filterable info data.
    info: Vec<HashMap<String, String>>,
    /// Filterable header data.
    headers: Vec<HashMap<String, String>>,
    /// Filterable value data.
    value: serde_json::Value,
}

impl From<&Record> for FilterableRecord {
    /// Creates a new [`FilterableRecord`] from the given [`Record`] reference.
    fn from(record: &Record) -> Self {
        let mut info = Vec::new();

        if let Some(pk) = record.key.as_ref() {
            let mut pk_map = HashMap::new();
            pk_map.insert(String::from("key"), pk.clone());

            info.push(pk_map);
        }

        let mut offset_map = HashMap::new();
        offset_map.insert(String::from("offset"), record.offset.to_string());
        info.push(offset_map);

        let mut partition_map = HashMap::new();
        partition_map.insert(String::from("partition"), record.partition.to_string());
        info.push(partition_map);

        let mut headers = Vec::new();
        for (k, v) in record.headers.iter() {
            let mut record_headers = HashMap::new();
            record_headers.insert(k.clone(), v.clone());

            headers.push(record_headers);
        }

        let value = serde_json::to_value(&record.value).expect("value serializes to JSON");

        Self {
            info,
            headers,
            value,
        }
    }
}

/// A task which is executed in a background thread that handles consuming messages from a Kafka
/// topic.
struct ConsumerTask {
    /// Raw Kafka consumer.
    consumer: Arc<StreamConsumer<ConsumerContext>>,
    /// Name of the topic to consume records from.
    topic: String,
    /// Any filter to apply to the record.
    filter: Option<String>,
    /// Bus that Kafka-related application events are published to.
    event_bus: Arc<EventBus>,
}

impl ConsumerTask {
    /// Creates a new [`ConsumerTask`] with the specified dependencies.
    fn new(
        consumer: Arc<StreamConsumer<ConsumerContext>>,
        topic: String,
        filter: Option<String>,
        event_bus: Arc<EventBus>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            consumer,
            topic,
            filter,
            event_bus,
        })
    }
    /// Runs the task by subscribing to the specified topic and then consuming messages from it.
    async fn run(&self) -> anyhow::Result<()> {
        let topics = vec![self.topic.as_str()];

        self.consumer
            .subscribe(&topics)
            .context(format!("subscribe to Kafka topic: {}", self.topic))?;

        let stream_procesor = self.consumer.stream().try_for_each(|msg| async move {
            let record = Record::from(&msg);

            if let Some(f) = self.filter.as_ref() {
                let filterable_record = FilterableRecord::from(&record);

                let json_value = serde_json::to_value(filterable_record)
                    .expect("FilterableRecord serializes to JSON");

                let json_path =
                    serde_json_path::JsonPath::parse(f).expect("valid JSONPath expression");

                if json_path.query(&json_value).is_empty() {
                    tracing::debug!("ignoring Kafka message based on filter");
                    return Ok(());
                }
            }

            self.event_bus.send(AppEvent::RecordReceived(record));

            if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                tracing::error!("error committing Kafka message: {}", err);
            }

            Ok(())
        });

        stream_procesor.await.context("process Kafka record stream")
    }
}

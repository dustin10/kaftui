use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, BaseConsumer, CommitMode, Consumer as RDConsumer,
        ConsumerContext as RDConsumerContext, Rebalance, StreamConsumer,
    },
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;

/// Enumerates the different states that the Kafka consumer can be in.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ConsumerMode {
    /// Consumer is paused and not processing records from the topic.
    Paused,
    /// Consumer is processing records from the topic.
    Processing,
}

/// A tuple struct that contains a partition and an offset.
#[derive(Debug)]
pub struct PartitionOffset {
    /// Partition number.
    partition: i32,
    /// Offset on the partition.
    offset: i64,
}

impl From<&str> for PartitionOffset {
    /// Converts a string slice to a [`PartitionOffset`].
    ///
    /// # Panics
    ///
    /// This function will panic if the string is not of the correct format.
    fn from(value: &str) -> Self {
        let mut pair_itr = value.split(":");

        let partition = pair_itr
            .next()
            .map(|p| p.parse::<i32>().expect("valid partition value"))
            .expect("partition value set");

        let offset = pair_itr
            .next()
            .map(|o| o.parse::<i64>().expect("valid offset value"))
            .expect("offset value set");

        Self { partition, offset }
    }
}

impl From<&String> for PartitionOffset {
    /// Converts a reference to a [`String`] to a [`PartitionOffset`].
    ///
    /// # Panics
    ///
    /// This function will panic if the string is not of the correct format.
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<String> for PartitionOffset {
    /// Converts a [`String`] to a [`PartitionOffset`].
    ///
    /// # Panics
    ///
    /// This function will panic if the string is not of the correct format.
    fn from(value: String) -> Self {
        Self::from(&value)
    }
}

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
    /// Value of the Kafka record, if one exists.
    pub value: Option<String>,
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

/// Enumeration of the states of a [`Record`] that was consumed from the Kafka topic.
#[derive(Clone, Debug)]
pub enum RecordState {
    /// A [`Record`] was consumed and it should be displayed to the user.
    Received(Record),
    /// A [`Record`] was consumed but it does not match the configured JSONPath filter.
    Filtered(Record),
}

/// High-level Kafka consumer. Through this struct the application can easily start, pause and
/// resume the underlying Kafka consumer.
pub struct Consumer {
    /// Underlying Kafka consumer.
    consumer: Arc<StreamConsumer<ConsumerContext>>,
    /// Sender for the Kafka consumer channel.
    consumer_tx: Sender<RecordState>,
}

impl Consumer {
    /// Creates a new [`Consumer`] with the specified dependencies.
    pub fn new(
        config: HashMap<String, String>,
        consumer_tx: Sender<RecordState>,
    ) -> anyhow::Result<Self> {
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
            consumer_tx,
        })
    }
    /// Starts the consumption of records from the specified topic.
    pub fn start(
        &self,
        topic: String,
        mut partitions: Vec<i32>,
        seek_to: Vec<PartitionOffset>,
        filter: Option<String>,
    ) -> anyhow::Result<()> {
        if partitions.is_empty() {
            let topic_metadata = self
                .consumer
                .fetch_metadata(Some(topic.as_str()), Duration::from_secs(10))
                .context("fetch topic metadata from broker")?;

            let topic_partitions = topic_metadata
                .topics()
                .first()
                .expect("topic metadata exists")
                .partitions()
                .iter()
                .map(|mp| mp.id());

            partitions.extend(topic_partitions);
        }

        let mut assignments_list = TopicPartitionList::with_capacity(partitions.len());

        for partition in partitions.iter() {
            match seek_to.iter().find(|po| po.partition == *partition) {
                Some(po) => assignments_list
                    .add_partition_offset(topic.as_str(), *partition, Offset::Offset(po.offset))
                    .context("add partition offset")?,
                None => {
                    let _ = assignments_list.add_partition(topic.as_str(), *partition);
                }
            }
        }

        self.consumer
            .assign(&assignments_list)
            .context("assign partitions to consumer")?;

        for partition in partitions.iter() {
            let partition_queue = self
                .consumer
                .split_partition_queue(topic.as_str(), *partition)
                .expect("partition queue created");

            let task = PartitionConsumerTask::new(
                Arc::clone(&self.consumer),
                Arc::new(partition_queue),
                filter.clone(),
                self.consumer_tx.clone(),
            );

            tokio::spawn(async move { task.run().await });
        }

        let task_consumer = Arc::clone(&self.consumer);

        // according to the crate docs, the main StreamConsumer must be awaited periodically even
        // if all partitions queues have been split off in order to receive events. See the
        // documentation linked below for details.
        //
        // https://docs.rs/rdkafka/latest/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html
        tokio::spawn(async move {
            let message = task_consumer.recv().await;
            panic!(
                "main stream consumer queue unexpectedly received message: {:?}",
                message
            );
        });

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

        let value = match msg.payload_view::<str>() {
            Some(Ok(data)) => Some(String::from(data)),
            Some(Err(e)) => {
                tracing::error!("non-UTF8 string value in message: {}", e);
                None
            }
            None => None,
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
struct PartitionConsumerTask {
    /// Raw Kafka consumer.
    consumer: Arc<StreamConsumer<ConsumerContext>>,
    /// The partition queue that the task is handling Kafka records for.
    partition_queue: Arc<StreamPartitionQueue<ConsumerContext>>,
    /// Any filter to apply to the record.
    filter: Option<String>,
    /// Sender for the Kafka consumer channel.
    consumer_tx: Sender<RecordState>,
}

impl PartitionConsumerTask {
    /// Creates a new [`ConsumerTask`] with the specified dependencies.
    fn new(
        consumer: Arc<StreamConsumer<ConsumerContext>>,
        partition_queue: Arc<StreamPartitionQueue<ConsumerContext>>,
        filter: Option<String>,
        consumer_tx: Sender<RecordState>,
    ) -> Self {
        Self {
            consumer,
            partition_queue,
            filter,
            consumer_tx,
        }
    }
    /// Runs the task by subscribing to the specified topic and then consuming messages from it.
    async fn run(&self) -> anyhow::Result<()> {
        let stream_procesor = self
            .partition_queue
            .stream()
            .try_for_each(|msg| async move {
                let record = Record::from(&msg);

                if let Some(f) = self.filter.as_ref() {
                    let filterable_record = FilterableRecord::from(&record);

                    let json_value = serde_json::to_value(filterable_record)
                        .expect("FilterableRecord serializes to JSON");

                    let json_path =
                        serde_json_path::JsonPath::parse(f).expect("valid JSONPath expression");

                    if json_path.query(&json_value).is_empty() {
                        tracing::debug!("ignoring Kafka record based on filter");

                        // TODO: clean up this duplication
                        if let Err(e) = self.consumer_tx.send(RecordState::Filtered(record)).await {
                            tracing::error!("failed to send record over consumer channel: {}", e);
                        }

                        if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                            tracing::error!("error committing Kafka message: {}", err);
                        }

                        return Ok(());
                    }
                }

                if let Err(e) = self.consumer_tx.send(RecordState::Received(record)).await {
                    tracing::error!("failed to send record over consumer channel: {}", e);
                }

                if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                    tracing::error!("error committing Kafka message: {}", err);
                }

                Ok(())
            });

        stream_procesor.await.context("process Kafka record stream")
    }
}

use anyhow::Context;
use chrono::{DateTime, Local};
use futures::TryStreamExt;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{
        stream_consumer::StreamPartitionQueue, BaseConsumer, CommitMode, Consumer as RDConsumer,
        ConsumerContext as RDConsumerContext, Rebalance, StreamConsumer,
    },
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
    ClientConfig, ClientContext, Message, Offset, Statistics, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;

/// String representation of the [`SeekTo::None`] enum variant. Used in serialization and
/// deserialization operations.
const SEEK_TO_NONE: &str = "none";

/// String representation of the [`SeekTo::Reset`] enum variant. Used in serialization and
/// deserialization operations.
const SEEK_TO_RESET: &str = "reset";

/// String representation of the [`RecordFormat::None`] enum variant. Used in serialization and
/// deserialization operations.
const RECORD_FORMAT_NONE: &str = "none";

/// String representation of the [`RecordFormat::Json`] enum variant. Used in serialization and
/// deserialization operations.
const RECORD_FORMAT_JSON: &str = "json";

/// Enumerates the different states that the Kafka consumer can be in.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConsumerMode {
    /// Consumer is paused and not processing records from the topic.
    Paused,
    /// Consumer is processing records from the topic.
    Processing,
}

/// Enumerates the well-known formats for the data in a Kafka topic.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RecordFormat {
    /// Records in the topic pare produced with no particular format.
    None,
    /// Records in the topic are produced in JSON format.
    Json,
}

impl Default for RecordFormat {
    /// Returns the default value for a value of [`RecordFormat`].
    fn default() -> Self {
        Self::None
    }
}

impl Display for RecordFormat {
    /// Writes a string representation of the [`RecordFormat`] value to the
    /// [`std::fmt::Formatter`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => RECORD_FORMAT_NONE,
            Self::Json => RECORD_FORMAT_JSON,
        };

        f.write_str(s)
    }
}

impl<T> From<T> for RecordFormat
where
    T: AsRef<str>,
{
    /// Converts the value to the corresponding [`RecordFormat`].
    fn from(value: T) -> Self {
        match value.as_ref() {
            RECORD_FORMAT_JSON => Self::Json,
            _ => Self::None,
        }
    }
}

impl<'de> serde::Deserialize<'de> for RecordFormat {
    /// Deserialize this value into the given [`serde::Deserializer`].
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(FromStrVisitor::default())
    }
}

impl serde::Serialize for RecordFormat {
    /// Serialize this value into the given [`serde::Serializer`].
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = self.to_string();
        serializer.serialize_str(&str)
    }
}

/// A tuple struct that contains a partition and an offset.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionOffset {
    /// Partition number.
    partition: i32,
    /// Offset on the partition.
    offset: i64,
}

impl<T> From<T> for PartitionOffset
where
    T: AsRef<str>,
{
    /// Converts the value to a [`PartitionOffset`].
    ///
    /// # Panics
    ///
    /// This function will panic if the string is not in the correct format.
    fn from(value: T) -> Self {
        let mut pair_itr = value.as_ref().split(":");

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

impl Display for PartitionOffset {
    /// Writes a string representation of the [`PartitionOffset`] value to the
    /// [`std::fmt::Formatter`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.partition, self.offset))
    }
}

/// Enumerates the available ways which the user can configure seeking the consumer to offsets on
/// the partitions that make up the topic.
#[derive(Clone, Debug)]
pub enum SeekTo {
    /// Do no seek to an offset for any partition.
    None,
    /// Reset offset to 0 on ALL partitions for the topic.
    Reset,
    /// Reset offsets to the values for partitions on the topic specified by the user.
    Custom(Vec<PartitionOffset>),
}

impl Default for SeekTo {
    /// Returns the default value for a value of [`SeekTo`].
    fn default() -> Self {
        Self::None
    }
}

impl<T> From<T> for SeekTo
where
    T: AsRef<str>,
{
    /// Converts the value to the corresponding [`SeekTo`].
    ///
    /// # Panics
    ///
    /// This function will panic if the string is not in the correct format for parsing the
    /// partition and offset pairs.
    fn from(value: T) -> Self {
        let s = value.as_ref();

        if s.is_empty() || s.eq_ignore_ascii_case(SEEK_TO_NONE) {
            Self::None
        } else if s.eq_ignore_ascii_case(SEEK_TO_RESET) {
            Self::Reset
        } else {
            let partitions = s.split(",").map(Into::into).collect();
            Self::Custom(partitions)
        }
    }
}

impl Display for SeekTo {
    /// Writes a string representation of the [`SeekTo`] value to the [`std::fmt::Formatter`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str(SEEK_TO_NONE),
            Self::Reset => f.write_str(SEEK_TO_RESET),
            Self::Custom(partition_offsets) => {
                let po_strs: Vec<String> =
                    partition_offsets.iter().map(ToString::to_string).collect();

                let csv = po_strs.join(",");

                f.write_str(&csv)
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for SeekTo {
    /// Deserialize this value into the given [`serde::Deserializer`].
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(FromStrVisitor::default())
    }
}

impl serde::Serialize for SeekTo {
    /// Serialize this value into the given [`serde::Serializer`].
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = self.to_string();
        serializer.serialize_str(&str)
    }
}

/// A simple [`serde::de::Visitor`] implementation that is capable of deserializing any value as
/// long as it has a [`From`] implementation for a [`str`] reference.
#[derive(Debug, Default)]
struct FromStrVisitor<T>
where
    T: for<'a> From<&'a str>,
{
    _data: PhantomData<T>,
}

impl<'de, T> serde::de::Visitor<'de> for FromStrVisitor<T>
where
    T: for<'a> From<&'a str>,
{
    type Value = T;

    /// Format a message stating what data this [`serde::de::Visitor`] expects to receive.
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid string representation")
    }
    /// Attempts to convert the [`str`] reference to a valid `T` based on it's contents.
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.into())
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
    /// Local timestamp represeting when the event was created.
    pub timestamp: DateTime<Local>,
}

impl Record {
    /// Determines if this [`Record`] matches the specified JSONPath filter.
    fn matches(&self, filter: impl AsRef<str>) -> bool {
        let filterable_record = FilterableRecord::from(self);

        let json_value =
            serde_json::to_value(filterable_record).expect("FilterableRecord serializes to JSON");

        let json_path =
            serde_json_path::JsonPath::parse(filter.as_ref()).expect("valid JSONPath expression");

        !json_path.query(&json_value).is_empty()
    }
}

/// The [`ConsumerContext`] is a struct that is used to implement a custom Kafka consumer context
/// to hook into key events in the lifecycle of a Kafka consumer.
#[derive(Debug)]
struct ConsumerContext {
    /// [`Sender`] that is used to publish the [`ConsumerEvent::Statistics`] event when the latest
    /// statistics are received from the librdkafka library.
    consumer_tx: Sender<ConsumerEvent>,
}

impl ConsumerContext {
    /// Creates a new [`ConsumerContext`] which uses the specified [`Sender`] to publish events to
    /// the consumer channel.
    fn new(consumer_tx: Sender<ConsumerEvent>) -> Self {
        Self { consumer_tx }
    }
}

impl ClientContext for ConsumerContext {
    /// Receives log lines from the underlying librdkafka library.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
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
    /// Receives the decoded statistics from the librdkafka client at the configured interval.
    fn stats(&self, statistics: Statistics) {
        let boxed_stats = statistics.into();

        let tx = self.consumer_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = tx.send(ConsumerEvent::Statistics(boxed_stats)).await {
                tracing::error!("failed to send statistics event consumer channel: {}", e);
            }
        });
    }
}

impl RDConsumerContext for ConsumerContext {
    /// Hook invoked right before the consumer begins rebalancing.
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        tracing::debug!("rebalance initiated: {:?}", rebalance);
    }
    /// Hook invoked after the consumer rebalancing has been completed.
    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
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
pub enum ConsumerEvent {
    /// A [`Record`] was consumed and it should be displayed to the user.
    Received(Record),
    /// A [`Record`] was consumed but it does not match the configured JSONPath filter.
    Filtered(Record),
    /// Updated [`Statistics`] were emitted by the Kafka consumer.
    Statistics(Box<Statistics>),
}

/// High-level Kafka consumer. Through this struct the application can easily start, pause and
/// resume the underlying Kafka consumer.
pub struct Consumer {
    /// Underlying Kafka consumer.
    consumer: Arc<StreamConsumer<ConsumerContext>>,
    /// Sender for the Kafka consumer channel.
    consumer_tx: Sender<ConsumerEvent>,
}

impl Consumer {
    /// Creates a new [`Consumer`] with the specified dependencies.
    pub fn new(
        config: HashMap<String, String>,
        consumer_tx: Sender<ConsumerEvent>,
    ) -> anyhow::Result<Self> {
        let mut client_config = ClientConfig::new();

        // apply default config
        client_config.set("auto.offset.reset", "latest");
        client_config.set("statistics.interval.ms", "5000");

        // apply user config
        client_config.extend(config);

        // apply enforced config
        client_config.set("enable.auto.commit", "false");

        if tracing::enabled!(tracing::Level::DEBUG) {
            for (k, v) in client_config.config_map().iter() {
                tracing::debug!("consumer property {} set to {}", k, v,);
            }
        }

        let context = ConsumerContext::new(consumer_tx.clone());

        let consumer: StreamConsumer<ConsumerContext> = client_config
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .context("create Kafka consumer")?;

        Ok(Self {
            consumer: Arc::new(consumer),
            consumer_tx,
        })
    }
    /// Starts the consumption of records from the specified Kafka topic.
    pub fn start(
        &self,
        topic: impl AsRef<str>,
        partitions: Vec<i32>,
        format: RecordFormat,
        seek_to: SeekTo,
        filter: Option<String>,
    ) -> anyhow::Result<()> {
        let to_assign = if partitions.is_empty() {
            let topic = topic.as_ref();

            tracing::debug!("fetching metadata for topic {} from broker", topic);

            let topic_metadata = self
                .consumer
                .fetch_metadata(Some(topic), Duration::from_secs(10))
                .context("fetch topic metadata from broker")?;

            topic_metadata
                .topics()
                .first()
                .expect("topic metadata exists")
                .partitions()
                .iter()
                .map(|mp| mp.id())
                .collect()
        } else {
            tracing::debug!("partition assignments specified by user");
            partitions
        };

        tracing::info!("assigning partitions to Kafka consumer: {:?}", to_assign);

        let mut assignments_list = TopicPartitionList::with_capacity(to_assign.len());

        for partition in to_assign.iter() {
            match seek_to {
                SeekTo::None => {
                    let _ = assignments_list.add_partition(topic.as_ref(), *partition);
                }
                SeekTo::Reset => assignments_list
                    .add_partition_offset(topic.as_ref(), *partition, Offset::Offset(0))
                    .context("add partition offset")?,
                SeekTo::Custom(ref partition_offsets) => {
                    match partition_offsets
                        .iter()
                        .find(|po| po.partition == *partition)
                    {
                        Some(po) => assignments_list
                            .add_partition_offset(
                                topic.as_ref(),
                                *partition,
                                Offset::Offset(po.offset),
                            )
                            .context("add partition offset")?,
                        None => {
                            let _ = assignments_list.add_partition(topic.as_ref(), *partition);
                        }
                    }
                }
            }
        }

        self.consumer
            .assign(&assignments_list)
            .context("assign partitions to consumer")?;

        for partition in to_assign.iter() {
            let partition_queue = self
                .consumer
                .split_partition_queue(topic.as_ref(), *partition)
                .expect("partition queue created");

            let task = PartitionConsumerTask {
                consumer: Arc::clone(&self.consumer),
                partition_queue: Arc::new(partition_queue),
                format,
                filter: filter.clone(),
                consumer_tx: self.consumer_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(e) = task.run().await {
                    tracing::error!("error during partition consumer task: {}", e);
                }
            });
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
                "StreamConsumer unexpectedly received message: {:?}",
                message
            );
        });

        Ok(())
    }
    /// Pauses the consumption of records from the topic.
    pub fn pause(&self) -> anyhow::Result<()> {
        tracing::debug!("attemping to pause Kafka consumer");

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
        tracing::debug!("attemping to resume Kafka consumer");

        let assignment = self
            .consumer
            .assignment()
            .context("get consumer partition assignments")?;

        self.consumer
            .resume(&assignment)
            .context("resume consumer assignments")
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
    /// Creates a new [`FilterableRecord`] from a [`Record`] reference.
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
struct PartitionConsumerTask<Con, Ctx>
where
    Con: RDConsumer<Ctx>,
    Ctx: RDConsumerContext,
{
    /// Raw Kafka consumer.
    consumer: Arc<Con>,
    /// The partition queue that the task is handling Kafka records for.
    partition_queue: Arc<StreamPartitionQueue<Ctx>>,
    /// Specifies the format of the records contained in the Kafka topic.
    format: RecordFormat,
    /// Any filter to apply to the record.
    filter: Option<String>,
    /// Sender for the Kafka consumer channel.
    consumer_tx: Sender<ConsumerEvent>,
}

impl<Con, Ctx> PartitionConsumerTask<Con, Ctx>
where
    Con: RDConsumer<Ctx>,
    Ctx: RDConsumerContext,
{
    /// Runs the task by subscribing to the paritition queue and then consuming messages from it.
    async fn run(&self) -> anyhow::Result<()> {
        let stream_procesor = self
            .partition_queue
            .stream()
            .try_for_each(|msg| async move {
                let record = self.create_record(&msg);

                let consumer_event = match &self.filter {
                    Some(filter) if !record.matches(filter) => ConsumerEvent::Filtered(record),
                    _ => ConsumerEvent::Received(record),
                };

                if let Err(e) = self.consumer_tx.send(consumer_event).await {
                    tracing::error!("failed to send consumer event over channel: {}", e);
                }

                if let Err(err) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                    tracing::error!("error committing Kafka message: {}", err);
                }

                Ok(())
            });

        stream_procesor.await.context("process Kafka record stream")
    }
    /// Creates a new [`Record`] from the [`BorrowedMessage`] read from the Kafka topic.
    fn create_record(&self, msg: &BorrowedMessage) -> Record {
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

        let mut value = match msg.payload_view::<str>() {
            Some(Ok(data)) => Some(String::from(data)),
            Some(Err(e)) => {
                tracing::error!("non-UTF8 string value in message: {}", e);
                None
            }
            None => None,
        };

        if let Some(ref v) = value
            && self.format == RecordFormat::Json
        {
            match serde_json::from_str(v)
                .and_then(|v: serde_json::Value| serde_json::to_string_pretty(&v))
            {
                Ok(json) => {
                    let _ = value.replace(json);
                }
                Err(e) => tracing::error!("invalid JSON value: {}", e),
            }
        }

        let timestamp_millis = msg
            .timestamp()
            .to_millis()
            .expect("Kafka message has valid timestamp");

        let local_date_time = DateTime::from_timestamp_millis(timestamp_millis)
            .expect("DateTime created from millis")
            .into();

        Record {
            partition: msg.partition(),
            topic: String::from(msg.topic()),
            key,
            headers,
            value,
            timestamp: local_date_time,
            offset: msg.offset(),
        }
    }
}

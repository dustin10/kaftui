use std::collections::HashMap;

/// Data contained in the record consumed from a Kafka topic.
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

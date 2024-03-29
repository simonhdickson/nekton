use std::{collections::HashMap, str};

use failure::Error;
use futures::{future::ok, Future, Stream};
use log::debug;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxFn, Message, MessageBatch, Sink, Source, Transaction, WriteHandler};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        debug!("Committing offsets: {:?}", result);
    }
}

#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
struct KafkaIn {
    topics: Vec<String>,
    config: HashMap<String, String>,
    #[serde(skip)]
    consume_count: u32,
}

#[typetag::serde(name = "kafka")]
impl Source for KafkaIn {
    fn start(&self, f: BoxFn<Transaction, Error>) -> Result<(), Error> {
        let mut config = &mut ClientConfig::new();

        for (k, v) in &self.config {
            config = config.set(k, v);
        }

        let consumer: StreamConsumer<CustomContext> = config
            .create_with_context(CustomContext)
            .expect("Consumer creation failed");

        let topics: Vec<&str> = self.topics.iter().map(|s| &**s).collect();

        consumer
            .subscribe(&topics)
            .expect("Can't subscribe to specified topics");

        let message_stream = consumer.start();

        let mut consumed_messages = 0;
        for message in message_stream.wait() {
            match message {
                Err(_) => panic!("Error while reading from stream."),
                Ok(Err(e)) => panic!("Kafka error: {}", e),
                Ok(Ok(m)) => {
                    match m.payload_view::<[u8]>() {
                        None => (),
                        Some(Ok(payload)) => {
                            let mut batch = MessageBatch::default();
                            batch.messages.push(Message {
                                data: payload.into(),
                                ..Default::default()
                            });

                            f(Transaction { batch }).wait().unwrap();

                            consumer.commit_message(&m, CommitMode::Sync).unwrap();
                            if self.consume_count != 0 {
                                consumed_messages += 1;
                                if consumed_messages >= self.consume_count {
                                    break;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            panic!("Error while deserializing message payload: {:?}", e);
                        }
                    };
                }
            };
        }

        Ok(())
    }
}

#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
struct KafkaOut {
    topic: String,
    config: HashMap<String, String>,
}

#[typetag::serde(name = "kafka")]
impl Sink for KafkaOut {
    fn create(&self) -> WriteHandler {
        let mut config = &mut ClientConfig::new();

        for (k, v) in &self.config {
            config = config.set(k, v);
        }

        let producer: FutureProducer = config.create().expect("producer creation error");
        let topic = self.topic.to_owned();

        Box::new(move |batches| {
            let (producer, topic) = (producer.clone(), topic.to_owned());

            let result = batches.for_each(move |batch| {
                for m in batch.messages {
                    producer
                        .send(
                            FutureRecord::to(&topic)
                                .payload(&m.data)
                                .key(m.metadata.get("partition_key").unwrap_or(&"0".to_owned())),
                            -1,
                        )
                        .map(move |delivery_status| {
                            debug!("Delivery status for message {:?} received", delivery_status);
                            delivery_status
                        })
                        .wait()
                        .unwrap()
                        .unwrap();
                }

                ok(())
            });

            Box::new(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::mpsc::channel;

    use crate::{no_metdata_batches, no_metdata_messages};

    macro_rules! source {
        ( $topics:expr, $config:expr, $consume_count:expr ) => {{
            $crate::run_source!(
                KafkaIn {
                    topics: $topics.into_iter().map(|t|t.to_string()).collect(),
                    config: $config
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    consume_count: $consume_count,
                }
            )
        }};
    }

    macro_rules! sink {
        ( $topic:expr, $config:expr, $input:expr ) => {{
            $crate::run_sink!(
                KafkaOut {
                    topic: $topic.to_string(),
                    config: $config
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                },
                $input
            )
        }};
    }

    const SOURCE_CONFIG: [(&str, &str); 4] = [
        ("bootstrap.servers", "localhost:9092"),
        ("message.timeout.ms", "5000"),
        ("auto.offset.reset", "earliest"),
        ("group.id", "test-consumer"),
    ];

    const SINK_CONFIG: [(&str, &str); 2] = [
        ("bootstrap.servers", "localhost:9092"),
        ("message.timeout.ms", "5000"),
    ];

    #[test]
    fn sink_source_kafka_message_test() {
        let topic = uuid::Uuid::new_v4();

        let messages = no_metdata_batches![no_metdata_messages![b"hello,world,cheese"]];

        sink!(topic, SINK_CONFIG, messages.clone());

        assert_eq!(
            source!([topic], SOURCE_CONFIG, 1),
            messages,
        )
    }

    #[test]
    fn sink_source_kafka_message_test_no_equal() {
        let topic = uuid::Uuid::new_v4();

        sink!(topic, SINK_CONFIG, no_metdata_batches![no_metdata_messages![b"hello,world,cheese"]]);

        assert_ne!(
            source!([topic], SOURCE_CONFIG, 1),
            no_metdata_batches![no_metdata_messages![b"cheese"]],
        )
    }
}

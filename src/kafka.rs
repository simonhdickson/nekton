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

use crate::{BoxFn, BoxFuture, BoxStream, Message, MessageBatch, Sink, Source, Transaction};

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
}

#[typetag::serde(name = "kafka")]
impl Source for KafkaIn {
    fn start(&self, mut f: BoxFn<Transaction, Error>) -> Result<(), Error> {
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
    #[serde(skip)]
    producer: Option<FutureProducer>,
}

#[typetag::serde(name = "kafka")]
impl Sink for KafkaOut {
    fn init(&mut self) {
        let mut config = &mut ClientConfig::new();

        for (k, v) in &self.config {
            config = config.set(k, v);
        }

        self.producer
            .replace(config.create().expect("producer creation error"));
    }

    fn write(&mut self, batches: BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error> {
        let producer = self.producer.clone().unwrap();
        let topic = self.topic.to_owned();

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
    }
}

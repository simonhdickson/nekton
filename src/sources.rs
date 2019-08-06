use std::{
    collections::HashMap,
    io,
    str,
    sync::mpsc::{self, Receiver},
    thread
};

use log::debug;
use serde::{Serialize, Deserialize};
use typetag::serde;

use crate::{Message, MessagePart, Transaction};

#[typetag::serde(tag = "type")]
pub trait Source : Send {
    fn start(&self) -> Receiver<Option<Transaction>>;
}

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde(name = "stdin")]
impl Source for StdIn {
    fn start(&self) -> Receiver<Option<Transaction>> {            
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let mut buffer = String::new();
                match io::stdin().read_line(&mut buffer) {
                    Ok(0) => {
                        sender.send(None).unwrap();
                        break
                    },
                    Ok(_) => {
                        let mut message = Message::default();
                        message.parts.push(MessagePart { data: buffer[..buffer.len()-1].into(),..Default::default() });
                        let (ack, receiver) = mpsc::channel();
                        sender.send(Some(Transaction { message, ack })).unwrap();
                        receiver.recv().unwrap();
                    }
                    Err(error) => panic!(error),
                }

            }
        });
        receiver
    }
}

#[cfg(feature = "kafka")]
#[derive(Default, Deserialize, Serialize)]
struct KafkaIn {
    topics: Vec<String>,
    config: HashMap<String, String>
}

#[cfg(feature = "kafka")]
#[typetag::serde(name = "kafka")]
impl Source for KafkaIn {
    fn start(&self) -> Receiver<Option<Transaction>> {
        use futures::Stream;
        use rdkafka::message::Message as _;
        use rdkafka::client::ClientContext;
        use rdkafka::consumer::{Consumer, ConsumerContext, CommitMode, Rebalance};
        use rdkafka::consumer::stream_consumer::StreamConsumer;
        use rdkafka::config::ClientConfig;
        use rdkafka::error::KafkaResult;

        struct CustomContext;

        impl ClientContext for CustomContext {}

        impl ConsumerContext for CustomContext {
            fn pre_rebalance(&self, rebalance: &Rebalance) {
                debug!("Pre rebalance {:?}", rebalance);
            }

            fn post_rebalance(&self, rebalance: &Rebalance) {
                debug!("Post rebalance {:?}", rebalance);
            }

            fn commit_callback(&self, result: KafkaResult<()>, _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList) {
                debug!("Committing offsets: {:?}", result);
            }
        }

        let (sender, receiver) = mpsc::channel();

        let mut config = &mut ClientConfig::new();

        for (k, v) in &self.config {
            config = config.set(k, v);
        }
        
        let consumer: StreamConsumer<CustomContext> =
            config
                .create_with_context(CustomContext)
                .expect("Consumer creation failed");

        let topics: Vec<&str> = self.topics.iter().map(|s| &**s).collect();
        
        consumer.subscribe(&topics)
            .expect("Can't subscribe to specified topics");

        thread::spawn(move || {
            let message_stream = consumer.start();

            for message in message_stream.wait() {
                match message {
                    Err(_) => panic!("Error while reading from stream."),
                    Ok(Err(e)) => panic!("Kafka error: {}", e),
                    Ok(Ok(m)) => {
                        match m.payload_view::<[u8]>() {
                            None => (),
                            Some(Ok(payload)) => {
                                let mut message = Message::default();
                                message.parts.push(MessagePart { data: payload.into(),..Default::default() });
                                
                                let (ack, receiver) = mpsc::channel();
                                sender.send(Some(Transaction { message, ack })).unwrap();
                                receiver.recv().unwrap();
                                consumer.commit_message(&m, CommitMode::Sync).unwrap();
                            },
                            Some(Err(e)) => {
                                panic!("Error while deserializing message payload: {:?}", e);
                            },
                        };
                    },
                };
            }
        });
        
        receiver
    }
}

#[cfg(feature = "http_server")]
#[derive(Default, Deserialize, Serialize)]
struct HttpServer{
    address: String,
    path: String
}

#[cfg(feature = "http_server")]
#[typetag::serde(name = "http_server")]
impl Source for HttpServer {
    fn start(&self) -> Receiver<Option<Transaction>> {
        use tiny_http::{Method, Response, Server};

        let (sender, receiver) = mpsc::channel();
        
        let server = Server::http(&self.address).unwrap();

        let path = self.path.clone();

        thread::spawn(move || {
            for mut request in server.incoming_requests() {
                if request.method() != &Method::Post {
                    let response = Response::empty(405);
                    request.respond(response).unwrap();
                    continue
                }
                if request.url() != &path {
                    let response = Response::empty(404);
                    request.respond(response).unwrap();
                    continue
                }
                let mut message = Message::default();
                let mut buffer = Vec::new();
                request.as_reader().read_to_end(&mut buffer).unwrap();
                message.parts.push(MessagePart { data: buffer,..Default::default() });

                let (ack, receiver) = mpsc::channel();
                sender.send(Some(Transaction { message, ack })).unwrap();
                receiver.recv().unwrap();

                let response = Response::empty(201);
                request.respond(response).unwrap();
            }
        });

        receiver
    }
}

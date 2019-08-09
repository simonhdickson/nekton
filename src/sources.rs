use std::{
    collections::HashMap,
    io::{self, BufRead},
    str, 
    sync::mpsc::{self, Receiver},
    thread,
};

use failure::{Error, format_err};
use futures::{future::{self, Either}, Future, Sink, Stream};
use futures::stream::BoxStream;
use log::debug;
use futures::sync::mpsc::channel;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Message, MessageBatch, Source};

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde(name = "stdin")]
impl Source for StdIn {
    fn start(&self) -> Box<Stream<Item = MessageBatch, Error = Error> + Send> {
        let (mut tx, rx) = channel(1);
        thread::spawn(move || {
            let input = io::stdin();
            for line in input.lock().lines() {
                let mut message = MessageBatch::default();
                message.messages.push(Message {
                    data: line.unwrap().into_bytes(),
                    ..Default::default()
                });
                match tx.send(message).wait() {
                    Ok(s) => tx = s,
                    Err(_) => break,
                }
            }
        });
        Box::new(rx.map_err(|_|format_err!("failed to read")))
    }
}

// #[cfg(feature = "kafka")]
// #[derive(Default, Deserialize, Serialize)]
// struct KafkaIn {
//     topics: Vec<String>,
//     config: HashMap<String, String>,
// }

// #[cfg(feature = "kafka")]
// #[typetag::serde(name = "kafka")]
// impl Source for KafkaIn {
//     fn start(&self) -> Receiver<Option<Transaction>> {
//         use futures::Stream;
//         use rdkafka::client::ClientContext;
//         use rdkafka::config::ClientConfig;
//         use rdkafka::consumer::stream_consumer::StreamConsumer;
//         use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
//         use rdkafka::error::KafkaResult;
//         use rdkafka::message::Message as _;

//         struct CustomContext;

//         impl ClientContext for CustomContext {}

//         impl ConsumerContext for CustomContext {
//             fn pre_rebalance(&self, rebalance: &Rebalance) {
//                 debug!("Pre rebalance {:?}", rebalance);
//             }

//             fn post_rebalance(&self, rebalance: &Rebalance) {
//                 debug!("Post rebalance {:?}", rebalance);
//             }

//             fn commit_callback(
//                 &self,
//                 result: KafkaResult<()>,
//                 _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
//             ) {
//                 debug!("Committing offsets: {:?}", result);
//             }
//         }

//         let (sender, receiver) = mpsc::channel();

//         let mut config = &mut ClientConfig::new();

//         for (k, v) in &self.config {
//             config = config.set(k, v);
//         }

//         let consumer: StreamConsumer<CustomContext> = config
//             .create_with_context(CustomContext)
//             .expect("Consumer creation failed");

//         let topics: Vec<&str> = self.topics.iter().map(|s| &**s).collect();

//         consumer
//             .subscribe(&topics)
//             .expect("Can't subscribe to specified topics");

//         thread::spawn(move || {
//             let message_stream = consumer.start();

//             for message in message_stream.wait() {
//                 match message {
//                     Err(_) => panic!("Error while reading from stream."),
//                     Ok(Err(e)) => panic!("Kafka error: {}", e),
//                     Ok(Ok(m)) => {
//                         match m.payload_view::<[u8]>() {
//                             None => (),
//                             Some(Ok(payload)) => {
//                                 let mut message = Message::default();
//                                 message.parts.push(MessagePart {
//                                     data: payload.into(),
//                                     ..Default::default()
//                                 });

//                                 let (ack, receiver) = mpsc::channel();
//                                 sender.send(Some(Transaction { message, ack })).unwrap();
//                                 receiver.recv().unwrap();
//                                 consumer.commit_message(&m, CommitMode::Sync).unwrap();
//                             }
//                             Some(Err(e)) => {
//                                 panic!("Error while deserializing message payload: {:?}", e);
//                             }
//                         };
//                     }
//                 };
//             }
//         });

//         receiver
//     }
// }

// #[cfg(feature = "http_server")]
// #[derive(Default, Deserialize, Serialize)]
// struct HttpServer {
//     address: String,
//     path: String,
// }

// #[cfg(feature = "http_server")]
// #[typetag::serde(name = "http_server")]
// impl Source for HttpServer {
//     fn start(&self) -> Receiver<Option<Transaction>> {
//         use tiny_http::{Method, Response, Server};

//         let (sender, receiver) = mpsc::channel();

//         let server = Server::http(&self.address).unwrap();

//         let path = self.path.clone();

//         thread::spawn(move || {
//             for mut request in server.incoming_requests() {
//                 if request.method() != &Method::Post {
//                     let response = Response::empty(405);
//                     request.respond(response).unwrap();
//                     continue;
//                 }
//                 if request.url() != &path {
//                     let response = Response::empty(404);
//                     request.respond(response).unwrap();
//                     continue;
//                 }
//                 let mut message = Message::default();
//                 let mut buffer = Vec::new();
//                 request.as_reader().read_to_end(&mut buffer).unwrap();
//                 message.parts.push(MessagePart {
//                     data: buffer,
//                     ..Default::default()
//                 });

//                 let (ack, receiver) = mpsc::channel();
//                 sender.send(Some(Transaction { message, ack })).unwrap();
//                 receiver.recv().unwrap();

//                 let response = Response::empty(201);
//                 request.respond(response).unwrap();
//             }
//         });

//         receiver
//     }
// }

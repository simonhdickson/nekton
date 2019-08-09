use std::{
    collections::HashMap,
    str,
    sync::mpsc,
    thread,
};

use failure::Error;
use futures::sync::mpsc::{channel, Receiver, Sender};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Message, MessageBatch, Sink};

#[derive(Default, Deserialize, Serialize)]
struct StdOut;

#[typetag::serde(name = "stdout")]
impl Sink for StdOut {
    fn write(&self, batch: Vec<MessageBatch>) -> Result<(), Error> {
        for m in batch {
            for p in m.messages {
                println!("{}", str::from_utf8(&p.data).unwrap())
            }
        }
        Ok(())
    }
}

// #[cfg(feature = "kafka")]
// #[derive(Default, Deserialize, Serialize)]
// struct KafkaOut {
//     topic: String,
//     config: HashMap<String, String>,
// }

// #[cfg(feature = "kafka")]
// #[typetag::serde(name = "kafka")]
// impl Sink for KafkaOut {
//     fn start(&self) -> Sender<Message> {
//         use futures::Future;
//         use rdkafka::config::ClientConfig;
//         use rdkafka::producer::{FutureProducer, FutureRecord};

//         let (sender, receiver) = mpsc::channel();

//         let mut config = &mut ClientConfig::new();

//         for (k, v) in &self.config {
//             config = config.set(k, v);
//         }

//         let producer: FutureProducer = config.create().expect("Producer creation error");

//         let topic = self.topic.clone();

//         thread::spawn(move || loop {
//             let message: Message = match receiver.recv() {
//                 Ok(m) => m,
//                 Err(_) => {
//                     info!("sink exiting");
//                     return;
//                 }
//             };

//             for m in message.parts {
//                 producer
//                     .send(
//                         FutureRecord::to(&topic)
//                             .payload(&m.data)
//                             .key(m.metadata.get("partition_key").unwrap_or(&"0".to_owned())),
//                         -1,
//                     )
//                     .map(move |delivery_status| {
//                         debug!("Delivery status for message {:?} received", delivery_status);
//                         delivery_status
//                     })
//                     .wait()
//                     .unwrap()
//                     .unwrap();
//             }
//         });
//         sender
//     }
// }

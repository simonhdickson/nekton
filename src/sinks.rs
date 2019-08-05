use std::{
    collections::HashMap,
    str,
    sync::mpsc::{self, Sender},
    thread
};

use log::{debug, info};
use serde::{Serialize, Deserialize};
use typetag::serde;

use crate::Message;

#[typetag::serde(tag = "type")]
pub trait Sink {
    fn start(&self) -> Sender<Message>;
}

#[derive(Default, Deserialize, Serialize)]
struct StdOut;

#[typetag::serde(name = "stdout")]
impl Sink for StdOut {
    fn start(&self) -> Sender<Message> {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let message: Message = match receiver.recv() {
                    Ok(m) => m,
                    Err(_) => {
                        info!("sink exiting");
                        return
                    },
                };
                
                for p in message.parts {
                    println!("{}", str::from_utf8(&p.data).unwrap())
                }
            }  
        });
        sender
    }
}

#[cfg(feature = "kafka")]
#[derive(Default, Deserialize, Serialize)]
struct KafkaOut {
    topic: String,
    config: HashMap<String, String>
}

#[cfg(feature = "kafka")]
#[typetag::serde(name = "kafka")]
impl Sink for KafkaOut {
    fn start(&self) -> Sender<Message> {
        use futures::Future;
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{FutureProducer, FutureRecord};

        let (sender, receiver) = mpsc::channel();

        let mut config = &mut ClientConfig::new();

        for (k, v) in &self.config {
            config = config.set(k, v);
        }

        let producer: FutureProducer = 
            config
                .create()
                .expect("Producer creation error");

        let topic = self.topic.clone();

        thread::spawn(move || {
            loop {
                let message: Message = match receiver.recv() {
                    Ok(m) => m,
                    Err(_) => {
                        info!("sink exiting");
                        return
                    },
                };
                
                for m in message.parts {
                    producer.send(
                        FutureRecord::to(&topic)
                            .payload(&m.data)
                            .key(m.metadata.get("partition_key").unwrap_or(&"0".to_owned())),
                        -1
                    )
                    .map(move |delivery_status| {
                        debug!("Delivery status for message {:?} received", delivery_status);
                        delivery_status
                    })
                    .wait().unwrap().unwrap();
                }
            }  
        });
        sender
    }
}
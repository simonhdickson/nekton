use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::str;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;

use failure::Error;
use futures::*;
use log::*;
use serde::{Serialize, Deserialize};
use serde_json;
use typetag::*;

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub parts: Vec<MessagePart>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct MessagePart {
    data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

pub struct Transaction {
    message: Message,
    ack: Sender<()>
}

pub fn parse<'a, T>(p: &'a MessagePart) -> Result<T, Error>
where
    T: Deserialize<'a>,
{
    let s = str::from_utf8(&p.data).unwrap();
    let x = serde_json::from_str(s)?;
    Ok(x)
}

#[typetag::serde(tag = "type")]
trait Source {
    fn start(&self) -> Receiver<Option<Transaction>>;
}

#[typetag::serde(tag = "type")]
trait Processor {
    fn process<'a>(&mut self, message: Message) -> Result<Vec<Message>, Error>;
}

#[typetag::serde(tag = "type")]
trait Sink {
    fn start(&self) -> Sender<Message>;
}

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde]
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
                        receiver.recv().unwrap();
                        sender.send(Some(Transaction { message, ack })).unwrap();
                    }
                    Err(error) => panic!(error),
                }

            }
        });
        receiver
    }
}

#[derive(Default, Deserialize, Serialize)]
struct KafkaIn {
    topics: Vec<String>,
    config: HashMap<String, String>
}

#[typetag::serde]
impl Source for KafkaIn {
    fn start(&self) -> Receiver<Option<Transaction>> {
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
                                consumer.commit_message(&m, CommitMode::Async).unwrap();
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

#[derive(Default, Deserialize, Serialize)]
struct StdOut;

#[typetag::serde]
impl Sink for StdOut {
    fn start(&self) -> Sender<Message> {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let message: Message = receiver.recv().unwrap();
                
                for p in message.parts {
                    println!("{}", str::from_utf8(&p.data).unwrap())
                }
            }  
        });
        sender
    }
}

#[derive(Default, Deserialize, Serialize)]
struct KafkaOut {
    topic: String,
    config: HashMap<String, String>
}

#[typetag::serde]
impl Sink for KafkaOut {
    fn start(&self) -> Sender<Message> {
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
                let message: Message = receiver.recv().unwrap();
                
                for m in message.parts {
                    producer.send(
                        FutureRecord::to(&topic)
                            .payload(&m.data)
                            .key("0"),
                        0
                    )
                    .map(move |delivery_status| {
                        info!("Delivery status for message {:?} received", delivery_status);
                        delivery_status
                    })
                    .wait().unwrap().unwrap();
                }
            }  
        });
        sender
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Noop;

#[typetag::serde]
impl Processor for Noop {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        Ok(vec![msg])
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Replace {
    from: String,
    to: String
}

#[typetag::serde]
impl Processor for Replace {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data).unwrap().to_owned();
            let data = source.replace(&self.from, &self.to);
            new_msg.parts.push(MessagePart {data:data.into(),..Default::default()});
        }
        Ok(vec![new_msg])
    }
}

use regex::Regex;

#[derive(Default, Deserialize, Serialize)]
struct RegexReplace {
    re: String,
    rep: String,
    #[serde(skip)]
    regex: Option<Regex>
}

#[typetag::serde]
impl Processor for RegexReplace {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        let r = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap());
            &self.regex.as_ref().unwrap()
        };

        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data)?.to_owned();
            let data = r.replace_all(&source, &*self.rep);
            new_msg.parts.push(MessagePart {  data: data.into_owned().into(),..Default::default() });
        }
        Ok(vec![new_msg])
    }
}

#[derive(Deserialize, Serialize)]
struct Pipeline {
    processors: Vec<Box<dyn Processor>>
}

#[derive(Deserialize, Serialize)]
struct Spec {
    input: Box<dyn Source>,
    pipeline: Pipeline,
    output: Box<dyn Sink>
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let file = fs::File::open(&args[1])?;

    let mut spec: Spec = serde_yaml::from_reader(file).unwrap();

    let receiver = spec.input.start();

    let sender = spec.output.start();

    loop {
        let transaction = receiver.recv()?;

        let transaction = match transaction {
            Some(transaction) => transaction,
            None => break,
        };
        
        let mut messages = vec![transaction.message.clone()];
        for processor in spec.pipeline.processors.iter_mut() {
            let mut new_messages = Vec::new();
            for message in messages {
                new_messages.append(&mut processor.process(message)?);
            }
            messages = new_messages;
        }

        for message in messages {
            sender.send(message).unwrap();
        }

        transaction.ack.send(())?;
    }

    Ok(())
}

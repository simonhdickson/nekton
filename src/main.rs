use std::{
    collections::HashMap,
    env,
    fs,
    str,
    sync::mpsc::Sender
};

mod processors;
mod sinks;
mod sources;

use failure::Error;
use processors::Processor;
use serde::{Serialize, Deserialize};
use sinks::Sink;
use sources::Source;

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub parts: Vec<MessagePart>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct MessagePart {
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

pub struct Transaction {
    message: Message,
    ack: Sender<()>
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

    let mut spec: Spec = serde_yaml::from_reader(file)?;

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
            sender.send(message)?;
        }

        transaction.ack.send(())?;
    }

    Ok(())
}

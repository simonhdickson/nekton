mod processors;
mod sinks;
mod sources;

//#[cfg(feature = "proximo")]
//mod proximo;

use std::{
    collections::HashMap,
    env, fs, str
};

use failure::{Error, SyncFailure};
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{future::{self, Either}, Future, Stream};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[typetag::serde(tag = "type")]
pub trait Source: Send {
    fn start(&self) -> Box<Stream<Item = MessageBatch, Error = Error> + Send>;
}

#[typetag::serde(tag = "type")]
pub trait Processor: Send {
    fn process<'a>(&mut self, message: MessageBatch) -> Result<Vec<MessageBatch>, Error>;
}

#[typetag::serde(tag = "type")]
pub trait Sink: Send {
    fn write(&self, messages: Vec<MessageBatch>) -> Result<(), Error>;
}

#[derive(Deserialize, Serialize)]
struct Pipeline {
    processors: Vec<Box<dyn Processor>>,
}

#[derive(Deserialize, Serialize)]
struct Spec {
    input: Box<dyn Source>,
    pipeline: Pipeline,
    output: Box<dyn Sink>,
}

fn start_stream_processor(mut spec: Spec) {
    let source = spec.input.start();

    let task = source
        .for_each(move |batch| {
            let mut messages = vec![batch];
            for processor in spec.pipeline.processors.iter_mut() {
                let mut new_messages = Vec::new();
                for message in messages {
                    new_messages.append(&mut processor.process(message)?);
                }
                messages = new_messages;
            }
            
            spec.output.write(messages)
        })
        .map_err(|e|panic!(e));

    tokio::run(task);
}

fn main() -> Result<(), Error> {
    #[cfg(feature = "env_log")]
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let file = fs::File::open(&args[1])?;

    let spec: Spec = serde_yaml::from_reader(file)?;

    start_stream_processor(spec);

    Ok(())
}

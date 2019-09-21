mod processors;
mod sinks;
mod sources;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "regexp")]
mod regex;

use std::{collections::HashMap, env, fs, str};

use failure::Error;
use futures::future::ok;
use futures::{Future, Stream};
use serde::{Deserialize, Serialize};

pub type BoxFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

pub type BoxStream<T, E> = Box<dyn Stream<Item = T, Error = E> + Send>;

pub type BoxFn<T, E> = Box<dyn FnMut(T) -> BoxFuture<(), E> + Send>;

#[derive(Debug)]
pub struct Transaction {
    pub batch: MessageBatch,
}

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
    fn start(&self, sender: BoxFn<Transaction, Error>) -> Result<(), Error>;
}

#[typetag::serde(tag = "type")]
pub trait Processor: Send {
    fn init(&mut self) {}
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error>;
}

#[typetag::serde(tag = "type")]
pub trait Sink: Send {
    fn init(&mut self) {}
    fn write(&mut self, batches: BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error>;
}

#[derive(Deserialize, Serialize)]
pub struct Pipeline {
    processors: Vec<Box<dyn Processor>>,
}

#[derive(Deserialize, Serialize)]
pub struct Spec {
    input: Box<dyn Source>,
    pipeline: Pipeline,
    output: Box<dyn Sink>,
}

pub fn start_stream_processor(mut spec: Spec) {
    for processor in spec.pipeline.processors.iter_mut() {
        processor.init();
    }

    let mut pipeline = spec.pipeline;
    let mut output = spec.output;

    spec.input
        .start(Box::new(move |tx| {
            let mut batches: BoxStream<MessageBatch, Error> = Box::new(ok(tx.batch).into_stream());
            for processor in pipeline.processors.iter_mut() {
                batches = processor.process(batches);
            }
            output.write(batches).wait().unwrap();
            Box::new(ok(()))
        }))
        .unwrap();
}

pub fn run() -> Result<(), Error> {
    #[cfg(feature = "env_log")]
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    let file = fs::File::open(&args[1])?;

    let spec: Spec = serde_yaml::from_reader(file)?;

    start_stream_processor(spec);

    Ok(())
}

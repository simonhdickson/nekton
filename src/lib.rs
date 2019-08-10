mod processors;
mod sinks;
mod sources;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "proximo")]
mod proximo;

use std::{collections::HashMap, env, fs, str};

use failure::Error;
use futures::{Future, Stream};
use futures::future::ok;
use serde::{Deserialize, Serialize};

type BoxFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

type BoxStream<T, E> = Box<dyn Stream<Item = T, Error = E> + Send>;

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
    fn start(&self) -> BoxStream<MessageBatch, Error>;
}

#[typetag::serde(tag = "type")]
pub trait Processor: Send {
    fn init(&self) {}
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error>;
}

#[typetag::serde(tag = "type")]
pub trait Sink: Send {
    fn init(&self) {}
    fn write(&self, batches: BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error>;
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
    let source = spec.input.start();

    let task = source
        .for_each(move |batch| {
            let mut batches: BoxStream<MessageBatch, Error> = Box::new(ok(batch).into_stream());
            for processor in spec.pipeline.processors.iter_mut() {
                batches = processor.process(batches);
            }
            spec.output.write(batches)
        })
        .map_err(|e| panic!(e));

    tokio::run(task);
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

mod processors;
mod sinks;
mod sources;

#[cfg(feature = "kafka")]
mod kafka;

#[cfg(feature = "regexp")]
mod regex;

use std::{collections::HashMap, fs, path::PathBuf, str};

use failure::Error;
use futures::future::ok;
use futures::{Future, Stream};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

pub type BoxFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

pub type BoxStream<T, E> = Box<dyn Stream<Item = T, Error = E> + Send>;

pub type BoxFn<T, E> = Box<dyn Fn(T) -> BoxFuture<(), E> + Send>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Transaction {
    pub batch: MessageBatch,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Message {
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[typetag::serde(tag = "type")]
pub trait Source: Send {
    fn start(&self, sender: BoxFn<Transaction, Error>) -> Result<(), Error>;
}

pub type ProcessHandler =
    Box<dyn Fn(BoxStream<MessageBatch, Error>) -> BoxStream<MessageBatch, Error> + Send>;

#[typetag::serde(tag = "type")]
pub trait Processor: Send {
    fn create<'a>(&self) -> ProcessHandler;
}

pub type WriteHandler = Box<dyn Fn(BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error> + Send>;

#[typetag::serde(tag = "type")]
pub trait Sink: Send {
    fn create<'a>(&self) -> WriteHandler;
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

pub fn start_stream_processor(spec: Spec) {
    let processors = spec
        .pipeline
        .processors
        .into_iter()
        .map(|p| p.create())
        .collect::<Vec<_>>();

    let output = spec.output.create();

    spec.input
        .start(Box::new(move |tx| {
            let mut batches: BoxStream<MessageBatch, Error> = Box::new(ok(tx.batch).into_stream());
            for process in processors.iter() {
                batches = process(batches);
            }
            output(batches).wait().unwrap();
            Box::new(ok(()))
        }))
        .unwrap();
}

#[derive(Debug, StructOpt)]
#[structopt(name = "nekton", about = "Stream procesing library.")]
struct Opt {
    #[structopt(
        parse(from_os_str),
        short = "f",
        long = "config_file",
        env = "CONFIG_FILE",
        default_value = "config.yml"
    )]
    config_file: PathBuf,
}

pub fn run() -> Result<(), Error> {
    #[cfg(feature = "env_log")]
    env_logger::init();

    let opt = Opt::from_args();

    let file = fs::File::open(opt.config_file)?;

    let spec: Spec = serde_yaml::from_reader(file)?;

    start_stream_processor(spec);

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use futures::future::{ok, Future};

    #[macro_export]
    macro_rules! run_processor {
        ( $process:expr, $input:expr ) => {{
            use crate::tests::block_on;

            use failure::format_err;
            use futures::stream;

            block_on($process.create()(Box::new(
                stream::iter_ok::<_, ()>($input).map_err(|_| format_err!("wtf")),
            )))
        }};
    }

    pub fn block_on(batches: BoxStream<MessageBatch, Error>) -> Vec<MessageBatch> {
        let mut result = Vec::new();
        batches
            .for_each(|batch| {
                result.push(batch);
                ok(())
            })
            .wait()
            .unwrap();
        result
    }

    #[macro_export]
    macro_rules! no_metdata_batches {
        ( $( $messages:expr ),* ) => {{
            use crate::MessageBatch;

            vec![
                $(
                    MessageBatch {
                        messages: $messages,
                        ..MessageBatch::default()
                    },
                )*
            ]
        }};
    }

    #[macro_export]
    macro_rules! no_metdata_messages {
        ( $( $message:expr ),* ) => {{
            vec![
                $(
                    Message {
                        data: $message.to_vec(),
                        ..Message::default()
                    },
                )*
            ]
        }};
    }
}

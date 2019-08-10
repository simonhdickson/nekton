use std::{
    str,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use failure::{format_err, Error};
use futures::{Future, Stream};
use futures::future::ok;
use futures::sync::mpsc::channel;
use hyper::client::connect::{Destination, HttpConnector};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::timer::Interval;
use tower_grpc::Request;
use tower_hyper::{client, util};
use tower_util::MakeService;
use typetag::serde;

pub mod proximo {
    include!(concat!(env!("OUT_DIR"), "/proximo.rs"));
}

use crate::{BoxFuture, BoxStream, Message, MessageBatch, Sink, Source};

#[derive(Default, Deserialize, Serialize)]
struct ProximoSource {
    endpoint: String,
    topic: String,
    consumer_group: String,
}

#[typetag::serde(name = "proximo")]
impl Source for ProximoSource {
    fn start(&self) -> BoxStream<MessageBatch, Error> {
        let (mut tx, rx) = channel(1);

        Box::new(rx.map_err(|_| format_err!("failed to read")))
    }
}

#[derive(Default, Deserialize, Serialize)]
struct ProximoSink {
    endpoint: String,
    topic: String,
}

#[typetag::serde(name = "proximo")]
impl Sink for ProximoSink {
    fn write(&self, batches: BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error> {
        Box::new(ok(()))
    }
}

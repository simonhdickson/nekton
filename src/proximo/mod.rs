use std::{
    str,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use futures::{Future, Stream};
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

use crate::{Message, MessagePart, Sink, Source, Transaction};

#[derive(Default, Deserialize, Serialize)]
struct ProximoSource {
    endpoint: String,
    topic: String,
    consumer_group: String,
}

#[typetag::serde(name = "proximo")]
impl Source for ProximoSource {
    fn start(&self) -> Receiver<Option<Transaction>> {
        let (sender, receiver) = mpsc::channel();

        receiver
    }
}

#[derive(Default, Deserialize, Serialize)]
struct ProximoSink {
    endpoint: String,
    topic: String,
}

#[typetag::serde(name = "proximo")]
impl Sink for ProximoSink {
    fn start(&self) -> Sender<Message> {
        let (sender, receiver) = mpsc::channel();

        sender
    }
}

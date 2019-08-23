use std::str;
use std::str::FromStr;
use tower_grpc::BoxBody;
use tower_hyper::Connection;

use failure::{format_err, Error};
use futures::sync::mpsc::channel;
use futures::{Future, Stream};
use hyper::client::connect::{Destination, HttpConnector};
use serde::{Deserialize, Serialize};
use tower_grpc::Request;
use tower_hyper::{client, util};
use tower_util::MakeService;
use typetag::serde;

use proximo_proto::{ConsumerRequest, Offset, StartConsumeRequest};

pub mod proximo_proto {
    include!(concat!(env!("OUT_DIR"), "/proximo.rs"));
}

use crate::{BoxFn, BoxFuture, Message, MessageBatch, Source, Transaction};

impl FromStr for Offset {
    type Err = ();

    fn from_str(s: &str) -> Result<Offset, ()> {
        match s {
            "default" => Ok(Offset::Default),
            "newest" => Ok(Offset::Newest),
            "oldest" => Ok(Offset::Oldest),
            _ => Err(()),
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
struct ProximoSource {
    endpoint: String,
    topic: String,
    consumer: String,
    intial_offset: String,
}

#[typetag::serde(name = "proximo")]
impl Source for ProximoSource {
    fn start(&self, mut f: BoxFn<Transaction, Error>) -> BoxFuture<(), Error> {
        let uri: http::Uri = self.endpoint.parse().unwrap();

        let dst = Destination::try_from_uri(uri.clone()).unwrap();
        let connector = util::Connector::new(HttpConnector::new(4));
        let settings = client::Builder::new().http2_only(true).clone();
        let mut make_client = client::Connect::with_builder(connector, settings);

        let topic = self.topic.to_owned();
        let consumer = self.consumer.to_owned();
        let intial_offset = self.intial_offset.parse::<Offset>().unwrap().into();

        let task = make_client
            .make_service(dst)
            .map_err(|e| {
                panic!("HTTP/2 connection failed; err={:?}", e);
            })
            .and_then(move |conn: Connection<BoxBody>| {
                use proximo_proto::client::MessageSource;

                let conn = tower_request_modifier::Builder::new()
                    .set_origin(uri)
                    .build(conn)
                    .unwrap();

                MessageSource::new(conn)
                    .ready()
                    .map_err(|e| eprintln!("client closed: {:?}", e))
            })
            .and_then(move |mut client| {
                let (mut inbound, outbound) = channel(1);
                let outbound = outbound.map_err(|e| panic!("timer error: {:?}", e));

                let response_stream = client.consume(Request::new(outbound));

                inbound
                    .try_send(ConsumerRequest {
                        start_request: Some(StartConsumeRequest {
                            consumer: consumer,
                            topic: topic,
                            initial_offset: intial_offset,
                        }),
                        ..Default::default()
                    })
                    .unwrap();

                Ok((response_stream, inbound))
            })
            .and_then(|(tx, rx)| {
                tx.map_err(|e| {
                    eprintln!("RouteChat request failed; err={:?}", e);
                })
                .and_then(|response| {
                    let inbound = response.into_inner();

                    inbound
                        .for_each(move |message| {
                            let mut batch = MessageBatch::default();
                            batch.messages.push(Message {
                                data: message.data,
                                ..Default::default()
                            });
                            f(Transaction { batch })
                                .and_then(|_| Ok(()))
                                .map_err(|_| tower_grpc::Status::new(tower_grpc::Code::Ok, ""))
                        })
                        .map_err(|e| eprintln!("gRPC inbound stream error: {:?}", e))
                })
            });

        Box::new(task.map_err(|_| format_err!("")))
    }
}

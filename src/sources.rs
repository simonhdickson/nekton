use std::{
    io::{self, BufRead},
    str, thread,
};

use failure::{format_err, Error};
use futures::sync::mpsc::channel;
use futures::{Future, Sink, Stream};
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Message, MessageBatch, Source};

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde(name = "stdin")]
impl Source for StdIn {
    fn start(&self) -> Box<dyn Stream<Item = MessageBatch, Error = Error> + Send> {
        let (mut tx, rx) = channel(1);
        thread::spawn(move || {
            let input = io::stdin();
            for line in input.lock().lines() {
                let mut batch = MessageBatch::default();
                batch.messages.push(Message {
                    data: line.unwrap().into_bytes(),
                    ..Default::default()
                });
                match tx.send(batch).wait() {
                    Ok(s) => tx = s,
                    Err(_) => break,
                }
            }
        });
        Box::new(rx.map_err(|_| format_err!("failed to read")))
    }
}

#[cfg(feature = "http_server")]
#[derive(Default, Deserialize, Serialize)]
struct HttpServer {
    address: String,
    path: String,
}

#[cfg(feature = "http_server")]
#[typetag::serde(name = "http_server")]
impl Source for HttpServer {
    fn start(&self) -> Box<dyn Stream<Item = MessageBatch, Error = Error> + Send> {
        use tiny_http::{Method, Response, Server};

        let (mut tx, rx) = channel(1);

        let server = Server::http(&self.address).unwrap();

        let path = self.path.clone();

        thread::spawn(move || {
            for mut request in server.incoming_requests() {
                if request.method() != &Method::Post {
                    let response = Response::empty(405);
                    request.respond(response).unwrap();
                    continue;
                }

                if request.url() != &path {
                    let response = Response::empty(404);
                    request.respond(response).unwrap();
                    continue;
                }

                let mut batch = MessageBatch::default();
                let mut buffer = Vec::new();
                request.as_reader().read_to_end(&mut buffer).unwrap();
                batch.messages.push(Message {
                    data: buffer,
                    ..Default::default()
                });

                match tx.send(batch).wait() {
                    Ok(s) => tx = s,
                    Err(_) => break,
                }

                let response = Response::empty(201);
                request.respond(response).unwrap();
            }
        });

        Box::new(rx.map_err(|_| format_err!("failed to read")))
    }
}

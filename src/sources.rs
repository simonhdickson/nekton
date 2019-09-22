use std::{
    io::{self, BufRead},
    str,
};

use failure::Error;
use futures::Future;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxFn, Message, MessageBatch, Source, Transaction};

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde(name = "stdin")]
impl Source for StdIn {
    fn start(&self, f: BoxFn<Transaction, Error>) -> Result<(), Error> {
        let input = io::stdin();
        for line in input.lock().lines() {
            let mut batch = MessageBatch::default();
            batch.messages.push(Message {
                data: line.unwrap().into_bytes(),
                ..Default::default()
            });
            f(Transaction { batch }).wait().unwrap();
        }
        Ok(())
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
    fn start(&self, f: BoxFn<Transaction, Error>) -> Result<(), Error> {
        use tiny_http::{Method, Response, Server};

        let server = Server::http(&self.address).unwrap();

        let path = self.path.clone();

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

            f(Transaction { batch }).wait().unwrap();

            let response = Response::empty(201);
            request.respond(response).unwrap();
        }

        Ok(())
    }
}

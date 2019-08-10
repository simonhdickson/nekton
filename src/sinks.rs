use std::str;

use failure::Error;
use futures::future::ok;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxFuture, BoxStream, MessageBatch, Sink};

#[derive(Default, Deserialize, Serialize)]
struct StdOut;

#[typetag::serde(name = "stdout")]
impl Sink for StdOut {
    fn write(&self, batches: BoxStream<MessageBatch, Error>) -> BoxFuture<(), Error> {
        let result = batches.for_each(|b| {
            for p in b.messages {
                println!("{}", str::from_utf8(&p.data).unwrap());
            }
            ok(())
        });

        Box::new(result)
    }
}

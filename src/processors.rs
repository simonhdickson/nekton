use std::str;

use failure::Error;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxStream, MessageBatch, Processor};

#[derive(Default, Deserialize, Serialize)]
struct Noop;

#[typetag::serde(name = "noop")]
impl Processor for Noop {
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        batches
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Replace {
    from: String,
    to: String,
}

#[typetag::serde(name = "replace")]
impl Processor for Replace {
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let (from, to) = (self.from.to_owned(), self.to.to_owned());

        let result = batches.map(move |mut b| {
            b.messages = b.messages.into_iter().map(|mut message| {
                let source = str::from_utf8(&message.data).unwrap().to_owned();
                message.data = source.replace(&from, &to).into();
                message
            }).collect();
            b
        });

        Box::new(result)
    }
}

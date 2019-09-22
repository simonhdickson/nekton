use std::str;

use futures::future::ok;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Sink, WriteHandler};

#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
struct StdOut;

#[typetag::serde(name = "stdout")]
impl Sink for StdOut {
    fn create(&self) -> WriteHandler {
        Box::new(|batches| {
            let result = batches.for_each(|b| {
                for p in b.messages {
                    println!("{}", str::from_utf8(&p.data).unwrap());
                }
                ok(())
            });

            Box::new(result)
        })
    }
}

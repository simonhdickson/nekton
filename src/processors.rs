use std::io::Write;
use std::process::{Command, Stdio};
use std::str;

use failure::Error;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxStream, Message, MessageBatch, Processor};

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
            b.messages = b
                .messages
                .into_iter()
                .map(|mut message| {
                    let source = str::from_utf8(&message.data).unwrap().to_owned();
                    message.data = source.replace(&from, &to).into();
                    message
                })
                .collect();
            b
        });

        Box::new(result)
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Process {
    name: String,
    args: Vec<String>,
}

#[typetag::serde(name = "process")]
impl Processor for Process {
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let name = self.name.to_owned();
        let args = self.args.to_owned();

        let result = batches.map(move |mut b| {
            let mut child_process = Command::new(&name)
                .args(&args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("failed to execute child");
            {
                let stdin = child_process.stdin.as_mut().expect("failed to get stdin");
                let mut data = b
                    .messages
                    .into_iter()
                    .map(|m| m.data)
                    .collect::<Vec<_>>()
                    .join(&('\n' as u8));

                data.push('\n' as u8);
                stdin.write_all(&data).expect("failed to write to stdin");
            }
            let output = child_process
                .wait_with_output()
                .expect("failed to wait on child");
            let data = output.stdout;

            b.messages = data
                .split(|i| i == &('\n' as u8))
                .filter(|s| !s.is_empty())
                .map(|d| Message {
                    data: d.to_vec(),
                    ..Default::default()
                })
                .collect();
            b
        });

        Box::new(result)
    }
}

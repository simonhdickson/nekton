use std::str;

use failure::Error;
use futures::stream::Stream;
use regex::Regex;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxStream, Message, MessageBatch, Processor};

#[derive(Debug, Default, Deserialize, Serialize)]
struct RegexReplace {
    re: String,
    rep: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[typetag::serde(name = "regex_replace")]
impl Processor for RegexReplace {
    fn init(&mut self) {
        self.regex.replace(Regex::new(&self.re).unwrap());
    }

    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let re = self.regex.clone().unwrap();
        let rep = self.rep.to_owned();

        let result = batches.map(move |mut b| {
            b.messages = b
                .messages
                .into_iter()
                .map(|mut message| {
                    let source = String::from_utf8(message.data).unwrap();
                    message.data = re.replace_all(&source, &*rep).into_owned().into_bytes();
                    message
                })
                .collect();
            b
        });

        Box::new(result)
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RegexSplit {
    re: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[typetag::serde(name = "regex_split")]
impl Processor for RegexSplit {
    fn init(&mut self) {
        self.regex.replace(Regex::new(&self.re).unwrap());
    }

    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let re = self.regex.clone().unwrap();

        let result = batches.map(move |mut b| {
            b.messages = b
                .messages
                .into_iter()
                .map(|message| {
                    let source = String::from_utf8(message.data).unwrap();
                    let new_messages: Vec<_> = re
                        .split(&source)
                        .map(|m| m.to_owned())
                        .map(|data| Message {
                            data: data.into_bytes(),
                            ..Default::default()
                        })
                        .collect();
                    new_messages
                })
                .collect::<Vec<Vec<_>>>()
                .concat();
            b
        });

        Box::new(result)
    }
}

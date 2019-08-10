use std::str;

use failure::Error;
use futures::stream::Stream;
use regex::Regex;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{BoxStream, Message, MessageBatch, Processor};

#[derive(Default, Deserialize, Serialize)]
struct RegexReplace {
    re: String,
    rep: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[typetag::serde(name = "regex_replace")]
impl Processor for RegexReplace {
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let re = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap()).clone()
        };
        let rep = self.rep.to_owned();

        let result = batches.map(move |mut b| {
            b.messages = b.messages.into_iter().map(|mut message| {
                let source = String::from_utf8(message.data).unwrap();
                message.data = re.replace_all(&source, &*rep).into_owned().into_bytes();
                message
            }).collect();
            b
        });

        Box::new(result)
    }
}

#[derive(Default, Deserialize, Serialize)]
struct RegexSplit {
    re: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[typetag::serde(name = "regex_split")]
impl Processor for RegexSplit {
    fn process<'a>(
        &mut self,
        batches: BoxStream<MessageBatch, Error>,
    ) -> BoxStream<MessageBatch, Error> {
        let re = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap()).clone()
        };

        let result = batches.map(move |mut b| {
            b.messages = b
                .messages
                .into_iter()
                .map(|message| {
                    let source = String::from_utf8(message.data).unwrap();
                    let new_messages: Vec<_> = re
                        .split(&source)
                        .map(|m|m.to_owned())
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

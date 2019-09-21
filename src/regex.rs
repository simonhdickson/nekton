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

#[cfg(test)]
mod replace_tests {
    use super::*;

    use crate::{no_metdata_batches, no_metdata_messages};

    macro_rules! regex_replace {
        ( $re:expr, $rep:expr, $input:expr ) => {{
            $crate::run_processor!(
                RegexReplace {
                    re: $re.into(),
                    rep: $rep.into(),
                    ..RegexReplace::default()
                },
                $input
            )
        }};
    }

    #[test]
    fn process_regex_replace_message_test() {
        assert_eq!(
            regex_replace!(
                r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})",
                "$m/$d/$y",
                no_metdata_batches![no_metdata_messages![
                    b"2012-03-14, 2013-01-15 and 2014-07-05"
                ]]
            ),
            no_metdata_batches![no_metdata_messages![
                b"03/14/2012, 01/15/2013 and 07/05/2014"
            ]]
        );
    }

    #[test]
    fn process_regex_replace_message_batch_test() {
        assert_eq!(
            regex_replace!(
                r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})",
                "$m/$d/$y",
                no_metdata_batches![
                    no_metdata_messages![b"2012-03-14, 2013-01-15 and 2014-07-05"],
                    no_metdata_messages![b"2014-07-05, 2012-03-14 and 2013-01-15"]
                ]
            ),
            no_metdata_batches![
                no_metdata_messages![b"03/14/2012, 01/15/2013 and 07/05/2014"],
                no_metdata_messages![b"07/05/2014, 03/14/2012 and 01/15/2013"]
            ]
        );
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

#[cfg(test)]
mod split_tests {
    use super::*;

    use crate::{no_metdata_batches, no_metdata_messages};

    macro_rules! regex_split {
        ( $re:expr, $input:expr ) => {{
            $crate::run_processor!(
                RegexSplit {
                    re: $re.into(),
                    ..RegexSplit::default()
                },
                $input
            )
        }};
    }

    #[test]
    fn process_regex_split_message_test() {
        assert_eq!(
            regex_split!(
                r"[ \t]+",
                no_metdata_batches![no_metdata_messages![b"cheese\tcheese"]]
            ),
            no_metdata_batches![no_metdata_messages![b"cheese", b"cheese"]]
        );
    }

    #[test]
    fn process_regex_split_message_batch_test() {
        assert_eq!(
            regex_split!(
                r"[ \t]+",
                no_metdata_batches![
                    no_metdata_messages![b"cheese\tcheese"],
                    no_metdata_messages![b"bacon\tbacon"]
                ]
            ),
            no_metdata_batches![
                no_metdata_messages![b"cheese", b"cheese"],
                no_metdata_messages![b"bacon", b"bacon"]
            ]
        );
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RegexSelect {
    re: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[typetag::serde(name = "regex_select")]
impl Processor for RegexSelect {
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
                        .find_iter(&source)
                        .map(|m| m.to_owned())
                        .map(|data| Message {
                            data: data.as_str().to_owned().into_bytes(),
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

#[cfg(test)]
mod select_tests {
    use super::*;

    use crate::{no_metdata_batches, no_metdata_messages};

    macro_rules! regex_select {
        ( $re:expr, $input:expr ) => {{
            $crate::run_processor!(
                RegexSelect {
                    re: $re.into(),
                    ..RegexSelect::default()
                },
                $input
            )
        }};
    }

    #[test]
    fn process_regex_select_message_test() {
        assert_eq!(
            regex_select!(
                r"\#[a-zA-Z][0-9a-zA-Z_]*",
                no_metdata_batches![no_metdata_messages![b"hello #cheese #world"]]
            ),
            no_metdata_batches![no_metdata_messages![b"#cheese", b"#world"]]
        );
    }

    #[test]
    fn process_regex_select_message_batch_test() {
        assert_eq!(
            regex_select!(
                r"\#[a-zA-Z][0-9a-zA-Z_]*",
                no_metdata_batches![
                    no_metdata_messages![b"hello #cheese"],
                    no_metdata_messages![b"cheese #world"]
                ]
            ),
            no_metdata_batches![
                no_metdata_messages![b"#cheese"],
                no_metdata_messages![b"#world"]
            ]
        );
    }
}

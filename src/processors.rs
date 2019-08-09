use std::str;

use failure::Error;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Message, MessageBatch, Processor};

#[derive(Default, Deserialize, Serialize)]
struct Noop;

#[typetag::serde(name = "noop")]
impl Processor for Noop {
    fn process<'a>(&mut self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        Ok(vec![batch])
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Replace {
    from: String,
    to: String,
}

#[typetag::serde(name = "replace")]
impl Processor for Replace {
    fn process<'a>(&mut self, mut batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        batch.messages = batch.messages
            .into_iter()
            .map(|mut message|{
                let source = str::from_utf8(&message.data).unwrap().to_owned();
                message.data = source.replace(&self.from, &self.to).into();
                message
            })
            .collect();
        Ok(vec![batch])
    }
}

#[cfg(feature = "regexp")]
#[derive(Default, Deserialize, Serialize)]
struct RegexReplace {
    re: String,
    rep: String,
    #[serde(skip)]
    regex: Option<regex::Regex>,
}

#[cfg(feature = "regexp")]
#[typetag::serde(name = "regex_replace")]
impl Processor for RegexReplace {
    fn process<'a>(&mut self, mut batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        use regex::Regex;

        let r = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap());
            &self.regex.as_ref().unwrap()
        };

        batch.messages = batch.messages
            .into_iter()
            .map(|mut message|{
                let source = str::from_utf8(&message.data).unwrap().to_owned();
                message.data = r.replace_all(&source, &*self.rep).into_owned().into();
                message
            })
            .collect();

        Ok(vec![batch])
    }
}

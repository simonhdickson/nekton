use std::str;

use failure::Error;
use serde::{Deserialize, Serialize};
use typetag::serde;

use crate::{Message, MessagePart, Processor};

#[derive(Default, Deserialize, Serialize)]
struct Noop;

#[typetag::serde(name = "noop")]
impl Processor for Noop {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        Ok(vec![msg])
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Replace {
    from: String,
    to: String,
}

#[typetag::serde(name = "replace")]
impl Processor for Replace {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data).unwrap().to_owned();
            let data = source.replace(&self.from, &self.to);
            new_msg.parts.push(MessagePart {
                data: data.into(),
                ..Default::default()
            });
        }
        Ok(vec![new_msg])
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
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        use regex::Regex;

        let r = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap());
            &self.regex.as_ref().unwrap()
        };

        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data)?.to_owned();
            let data = r.replace_all(&source, &*self.rep);
            new_msg.parts.push(MessagePart {
                data: data.into_owned().into(),
                ..Default::default()
            });
        }
        Ok(vec![new_msg])
    }
}

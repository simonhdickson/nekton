use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::str;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;

use failure::Error;
use serde::{Serialize, Deserialize};
use serde_json;
use typetag::*;

#[derive(Clone, Debug, Default)]
pub struct Message {
    pub parts: Vec<MessagePart>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct MessagePart {
    data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

pub struct Transaction {
    message: Message
}

pub fn parse<'a, T>(p: &'a MessagePart) -> Result<T, Error>
where
    T: Deserialize<'a>,
{
    let s = str::from_utf8(&p.data).unwrap();
    let x = serde_json::from_str(s)?;
    Ok(x)
}

#[typetag::serde(tag = "type")]
trait Producer {
    fn start(&self) -> Receiver<Option<Transaction>>;
}

#[typetag::serde(tag = "type")]
trait Processor {
    fn process<'a>(&mut self, message: Message) -> Result<Vec<Message>, Error>;
}

#[typetag::serde(tag = "type")]
trait Consumer {
    fn start(&self) -> Sender<Option<Transaction>>;
}

#[derive(Default, Deserialize, Serialize)]
struct StdIn;

#[typetag::serde]
impl Producer for StdIn {
    fn start(&self) -> Receiver<Option<Transaction>> {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let mut buffer = String::new();
                match io::stdin().read_line(&mut buffer) {
                    Ok(0) => {
                        sender.send(None).unwrap();
                        break
                    },
                    Ok(_) => {
                        let mut message = Message::default();
                        message.parts.push(MessagePart {  data: buffer[..buffer.len()-1].into(),..Default::default() });
                        sender.send(Some(Transaction { message })).unwrap()
                    }
                    Err(error) => panic!(error),
                }

            }
        });
        receiver
    }
}

#[derive(Default, Deserialize, Serialize)]
struct StdOut;

#[typetag::serde]
impl Consumer for StdOut {
    fn start(&self) -> Sender<Option<Transaction>> {
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let transaction: Option<Transaction> = receiver.recv().unwrap();
                
                for m in transaction.unwrap().message.parts {
                    println!("{}", str::from_utf8(&m.data).unwrap())
                }
            }  
        });
        sender
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Noop;

#[typetag::serde]
impl Processor for Noop {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        Ok(vec![msg])
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Replace {
    from: String,
    to: String
}

#[typetag::serde]
impl Processor for Replace {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data).unwrap().to_owned();
            let data = source.replace(&self.from, &self.to);
            new_msg.parts.push(MessagePart {data:data.into(),..Default::default()});
        }
        Ok(vec![new_msg])
    }
}

use regex::Regex;

#[derive(Default, Deserialize, Serialize)]
struct RegexReplace {
    re: String,
    rep: String,
    #[serde(skip)]
    regex: Option<Regex>
}

#[typetag::serde]
impl Processor for RegexReplace {
    fn process<'a>(&mut self, msg: Message) -> Result<Vec<Message>, Error> {
        let r = {
            let re = &self.re;
            self.regex.get_or_insert_with(|| Regex::new(re).unwrap());
            &self.regex.as_ref().unwrap()
        };

        let mut new_msg = Message::default();
        for p in msg.parts {
            let source = str::from_utf8(&p.data).unwrap().to_owned();
            let data = r.replace_all(&source, &*self.rep);
            new_msg.parts.push(MessagePart {  data: data.into_owned().into(),..Default::default() });
        }
        Ok(vec![new_msg])
    }
}

#[derive(Deserialize, Serialize)]
struct Pipeline {
    processors: Vec<Box<dyn Processor>>
}

#[derive(Deserialize, Serialize)]
struct Spec {
    input: Box<dyn Producer>,
    pipeline: Pipeline,
    output: Box<dyn Consumer>
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();

    let file = fs::File::open(&args[1])?;

    let mut spec: Spec = serde_yaml::from_reader(file).unwrap();

    let receiver = spec.input.start();

    let sender = spec.output.start();

    loop {
        let transaction = receiver.recv()?;

        match transaction {
            None => break,
            Some(_) => (),
        }
        
        let mut messages = vec![transaction.unwrap().message];
        for processor in spec.pipeline.processors.iter_mut() {
            let mut new_messages = Vec::new();
            for message in messages {
                new_messages.append(&mut processor.process(message)?);
            }
            messages = new_messages;
        }

        for message in messages {
            sender.send(Some(Transaction { message }))?;
        }
    }

    Ok(())
}

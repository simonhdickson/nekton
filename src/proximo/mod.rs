mod generated;

use std::{
    str,
    sync::{Arc, mpsc::{self, Receiver, Sender}},
    time::Duration,
    thread
};

use futures;
use futures::{Async, AsyncSink, Sink as _, Stream};
use grpcio::{ChannelBuilder, Environment, StreamingCallSink, WriteFlags};
use log::info;
use serde::{Serialize, Deserialize};
use typetag::serde;
use uuid::Uuid;

use generated::proximo::{ConsumerRequest, Confirmation, Message as ProximoMessage, PublisherRequest, StartConsumeRequest, StartPublishRequest};
use generated::proximo_grpc::{MessageSinkClient, MessageSourceClient};

use crate::{Message, MessagePart, Sink, Source, Transaction};

#[derive(Default, Deserialize, Serialize)]
struct ProximoSource {
    endpoint: String,
    topic: String,
    consumer_group: String
}

#[typetag::serde(name = "proximo")]
impl Source for ProximoSource {
    fn start(&self) -> Receiver<Option<Transaction>> {            
        let (sender, receiver) = mpsc::channel();

        let env = Arc::new(Environment::new(2));
        let duration = Duration::new(30, 0);
        let channel = ChannelBuilder::new(env)
            .keepalive_timeout(duration)
            .connect(&self.endpoint);
        
        let client = MessageSourceClient::new(channel);
        let (mut outgoing, mut incoming) = client.consume().unwrap();

        let mut request = ConsumerRequest::new();
        let mut consume_request = StartConsumeRequest::new();
        consume_request.set_consumer(self.consumer_group.to_owned());
        consume_request.set_topic(self.topic.to_owned());
        request.set_startRequest(consume_request);
        send_message(&mut outgoing, request);
        
        thread::spawn(move || {
            let x: Result<(), ()> = futures::executor::spawn(futures::lazy(|| {
                loop {
                    match incoming.poll() {
                        Ok(Async::Ready(Some(response))) => {
                            let mut message = Message::default();
                            message.parts.push(MessagePart { data: response.data,..Default::default() });
                            let (ack, receiver) = mpsc::channel();
                            sender.send(Some(Transaction { message, ack })).unwrap();
                            receiver.recv().unwrap();

                            let mut request = ConsumerRequest::new();
                            let mut consume_request = Confirmation::new();
                            consume_request.set_msgID(response.id);
                            request.set_confirmation(consume_request);

                            send_message(&mut outgoing, request);
                            continue;
                            break Ok(());
                        },
                        Ok(Async::Ready(None)) | Ok(Async::NotReady) => {
                            thread::sleep(Duration::from_millis(1));
                        },
                        Err(e) => panic!("disconnected: {}", e.to_string()),
                    }
                }
            })).wait_future();
            x.unwrap();
        });
        receiver
    }
}

#[derive(Default, Deserialize, Serialize)]
struct ProximoSink {
    endpoint: String,
    topic: String,
}

#[typetag::serde(name = "proximo")]
impl Sink for ProximoSink {
    fn start(&self) -> Sender<Message> {            
        let (sender, receiver) = mpsc::channel();

        let env = Arc::new(Environment::new(2));
        let duration = Duration::new(30, 0);
        let channel = ChannelBuilder::new(env)
            .keepalive_timeout(duration)
            .connect(&self.endpoint);

        let client = MessageSinkClient::new(channel);
        let (mut outgoing, mut incoming) = client.publish().unwrap();
        let mut request = PublisherRequest::new();
        let mut publish_request = StartPublishRequest::new();
        publish_request.set_topic(self.topic.to_owned());
        request.set_startRequest(publish_request);
        send_message(&mut outgoing, request);

        thread::spawn(move || {
            loop {
                let message: Message = match receiver.recv() {
                    Ok(m) => m,
                    Err(_) => {
                        info!("sink exiting");
                        return
                    },
                };
                for p in message.parts {
                    let mut request = PublisherRequest::new();
                    let mut msg = ProximoMessage::new();
                    let message_id = p.metadata.get("message_id").map(|s|s.to_owned()).unwrap_or_else(||Uuid::new_v4().to_string());
                    msg.set_id(message_id.to_owned());
                    request.set_msg(msg);
                    send_message(&mut outgoing, request);

                    let x: Result<(), ()> = futures::executor::spawn(futures::lazy(|| {
                        loop {
                            match incoming.poll() {
                                Ok(Async::Ready(Some(response))) => {
                                    if &response.msgID == &message_id {
                                        break Ok(())
                                    } else {
                                        panic!("failed to get successful ack of message: {}", &message_id);
                                    }
                                },
                                Ok(Async::Ready(None)) | Ok(Async::NotReady) => {
                                    thread::sleep(Duration::from_millis(1));
                                },
                                Err(e) => panic!("disconnected: {}", e.to_string()),
                            }
                        }
                    })).wait_future();
                    x.unwrap();
                }
            }
        });

        sender
    }
}

pub fn send_message<T>(sink: &mut StreamingCallSink<T>, request: T) {
    let x: Result<(), ()> = futures::executor::spawn(futures::lazy(|| {
        match sink.start_send((request, WriteFlags::default())) {
            Ok(AsyncSink::Ready) => {
                loop {
                    match sink.poll_complete() {
                        Ok(Async::Ready(())) => break Ok(()),
                        Ok(Async::NotReady) => (),
                        Err(e) => panic!(e),
                    };
                }
            },
            Ok(AsyncSink::NotReady(_)) => panic!("remote stopped"),
            Err(e) => panic!(e),
        }
    })).wait_future();
    x.unwrap();
}

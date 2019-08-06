// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_MESSAGE_SOURCE_CONSUME: ::grpcio::Method<super::proximo::ConsumerRequest, super::proximo::Message> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/proximo.MessageSource/Consume",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct MessageSourceClient {
    client: ::grpcio::Client,
}

impl MessageSourceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        MessageSourceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn consume_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::proximo::ConsumerRequest>, ::grpcio::ClientDuplexReceiver<super::proximo::Message>)> {
        self.client.duplex_streaming(&METHOD_MESSAGE_SOURCE_CONSUME, opt)
    }

    pub fn consume(&self) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::proximo::ConsumerRequest>, ::grpcio::ClientDuplexReceiver<super::proximo::Message>)> {
        self.consume_opt(::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait MessageSource {
    fn consume(&mut self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::proximo::ConsumerRequest>, sink: ::grpcio::DuplexSink<super::proximo::Message>);
}

pub fn create_message_source<S: MessageSource + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_duplex_streaming_handler(&METHOD_MESSAGE_SOURCE_CONSUME, move |ctx, req, resp| {
        instance.consume(ctx, req, resp)
    });
    builder.build()
}

const METHOD_MESSAGE_SINK_PUBLISH: ::grpcio::Method<super::proximo::PublisherRequest, super::proximo::Confirmation> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/proximo.MessageSink/Publish",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct MessageSinkClient {
    client: ::grpcio::Client,
}

impl MessageSinkClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        MessageSinkClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn publish_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::proximo::PublisherRequest>, ::grpcio::ClientDuplexReceiver<super::proximo::Confirmation>)> {
        self.client.duplex_streaming(&METHOD_MESSAGE_SINK_PUBLISH, opt)
    }

    pub fn publish(&self) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::proximo::PublisherRequest>, ::grpcio::ClientDuplexReceiver<super::proximo::Confirmation>)> {
        self.publish_opt(::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait MessageSink {
    fn publish(&mut self, ctx: ::grpcio::RpcContext, stream: ::grpcio::RequestStream<super::proximo::PublisherRequest>, sink: ::grpcio::DuplexSink<super::proximo::Confirmation>);
}

pub fn create_message_sink<S: MessageSink + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_duplex_streaming_handler(&METHOD_MESSAGE_SINK_PUBLISH, move |ctx, req, resp| {
        instance.publish(ctx, req, resp)
    });
    builder.build()
}

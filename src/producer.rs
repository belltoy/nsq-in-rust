use std::net::SocketAddr;

use futures::{Future, Stream, Sink};

use command;

pub struct Producer {
}

impl Producer {
    pub fn connect(addr: SocketAddr) {
    }

    pub fn publish() {
    }

    pub fn multi_publish() {
    }

    pub fn deferred_publish() {
    }
}

impl Sink for Producer {
    type SinkItem = Message;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
    }
}

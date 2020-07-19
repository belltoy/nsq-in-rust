use std::pin::Pin;
use std::task::{Context, Poll};
use futures::prelude::*;
use futures::ready;
use tokio_util::codec::Framed;

use crate::codec::NsqCodec;
use crate::error::Error;
use crate::{
    codec::{
        NsqFramed,
        RawResponse,
    },
    conn::Response,
};
use crate::command::Command;

use super::connection::AsyncRW;
type InnerFramed = Framed<Box<dyn AsyncRW + Send + Unpin + 'static>, NsqCodec>;

pub struct Heartbeat {
    inner: InnerFramed,
    response_remaining: usize,
    status: Status,
}

enum Status {
    Responding,
    Reading,
}

impl Heartbeat {
    pub(crate) fn new(inner: InnerFramed) -> Self {
        Self { inner, response_remaining: 0, status: Status::Reading }
    }

    fn start_pong(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        ready!(Pin::new(&mut self.inner).poll_ready(cx)?);
        while self.response_remaining > 0 {
            Pin::new(&mut self.inner).start_send(Command::Nop)?;
            self.response_remaining -= 1;
        }
        self.status = Status::Responding;
        Poll::Ready(Ok(()))
    }

    fn poll_pong(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        ready!(Pin::new(&mut self.inner).poll_flush(cx)?);
        self.status = Status::Reading;
        Poll::Ready(Ok(()))
    }
}

impl Stream for Heartbeat {
    type Item = Result<Response, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.response_remaining > 0 {
            ready!(self.as_mut().start_pong(cx)?);
        }

        match self.status {
            Status::Responding => {
                ready!(self.as_mut().poll_pong(cx)?);
            }
            Status::Reading => {}
        }

        loop {
            match ready!(Pin::new(&mut self.as_mut().inner).poll_next(cx)) {
                Some(Ok(msg)) => {
                    match msg {
                        NsqFramed::Response(RawResponse::Ok) => {
                            return Poll::Ready(Some(Ok(Response::Ok)));
                        }
                        // Handling heartbeat
                        NsqFramed::Response(RawResponse::Heartbeat) => {
                            self.response_remaining += 1;
                            ready!(self.as_mut().start_pong(cx))?;
                            // poll pong
                            ready!(self.as_mut().poll_pong(cx))?;
                            continue;
                        }
                        NsqFramed::Response(RawResponse::CloseWait) => {
                            return Poll::Ready(None);
                        }
                        NsqFramed::Response(RawResponse::Json(_)) => {
                            // Not possible
                            // TODO
                            // return Poll::Ready(None);
                            unreachable!();
                        }
                        NsqFramed::Message(msg) => {
                            return Poll::Ready(Some(Ok(Response::Msg(msg))));
                        }
                        NsqFramed::Error(nsq_error) => {
                            if nsq_error.is_fatal() {
                                return Poll::Ready(Some(Err(nsq_error.into())));
                            } else {
                                return Poll::Ready(Some(Ok(Response::Err(nsq_error))));
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Sink<Command> for Heartbeat {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Command) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

use crate::error::{Error, NsqError};
use crate::codec::{NsqMsg, NsqFramed};
use crate::command::Command;

use futures::prelude::*;

mod snappy;
mod deflate;
mod heartbeat;
mod reconnect;
pub mod connection;
mod codec;

pub(crate) trait Transport: Stream<Item = Result<NsqFramed, Error>> + Sink<Command, Error = Error> + Unpin {}
pub(crate) trait MessageStream: Stream<Item = Result<Response, Error>> + Sink<Command, Error = Error> + Unpin {}
pub(crate) use heartbeat::Heartbeat;
pub use connection::Connection;

#[derive(Debug)]
pub enum Response {
    Ok,
    Err(NsqError),
    Msg(NsqMsg),
}

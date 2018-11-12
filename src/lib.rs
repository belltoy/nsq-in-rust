extern crate bytes;
extern crate byteorder;
extern crate hostname;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;
extern crate hyper;
extern crate futures;

mod codec;
mod command;
pub mod connection;
mod error;
mod config;
// mod producer;
mod consumer;

pub const USER_AGENT: &'static str = concat!("nsq-rust/", env!("CARGO_PKG_VERSION"));

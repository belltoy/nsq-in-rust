//! It provides high-level Consumer and Producer types as well as low-level functions to communicate over the NSQ protocol.
//!
//! ## Consumer
//!
//! TODO
//!
//! ## Producer
//!
//! Producing messages can be done by creating an instance of a Producer.
//!
//! See [example](examples/producer.rs)

mod codec;
mod error;
pub mod config;
pub mod producer;
mod consumer;

pub mod command;
pub mod conn;

pub const USER_AGENT: &'static str = concat!("nsq-rust/", env!("CARGO_PKG_VERSION"));
pub use conn::Connection;
pub use error::Error;
pub use config::Config;
pub use producer::Producer;

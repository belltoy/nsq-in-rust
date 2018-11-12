extern crate tokio;
extern crate nsq_in_rust;
extern crate futures;

use std::net::SocketAddr;
use nsq_in_rust::connection::Connection;
use futures::Future;

pub fn main() {
    let addr: SocketAddr = "127.0.0.1:4150".parse().unwrap();

    // TcpStream.connect(&addr, &handle)
    //     .and_then(|socket| {
    //     })
    let conn = Connection::connect(addr);
    let done = conn.map(|_| ()).map_err(|_| ());
    tokio::run(done);
}

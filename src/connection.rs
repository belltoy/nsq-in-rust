// connect
//   V2
// IDENTIFY
//   tls_v1
//   snappy
//   deflate
//
// PUB/MPUB/DPUB
// SUB
//
// IDENTIFY
// {
//     "client_id": "nsq-rust",
//     "hostname": "localhsot",
//     "feature_negotiation": true,
//     "heartbeat_interval": 5000,
//     "output_buffer_size": 16 * 1024,
//     "output_buffer_timeout": 250,
//     "tls_v1": false,
//     "snappy": false,
//     "deflate": true,
//     "deflate_level": 3,
//     "sample_rate": 0,
//     "user_agent": "nsq-rust/0.1.0",
//     "msg_timeout": 100000
// }

use std::net::SocketAddr;
use tokio::net::TcpStream;
use futures::{Future, Stream, Sink};
use tokio_codec::{Framed, Decoder};

use error::Error;
use codec::NsqCodec;
use command::Command;
use super::config::Config;

pub struct Connection {
    // config: Config,
    addr: SocketAddr,
    transport: Framed<TcpStream, NsqCodec>,
}

impl Connection {
    pub fn connect(addr: SocketAddr) -> Box<Future<Item = Self, Error = Error> + Send> {
        let done = TcpStream::connect(&addr)
            .map_err(From::from)
            .and_then(|tcp| {
                let framed = NsqCodec::new(true).framed(tcp);
                framed.send(Command::Version)
            })
            .and_then(|framed| {
                let obj = json!({
                    "client_id": "nsq-rust",
                    "hostname": ::hostname::get_hostname().unwrap_or_else(|| "unknown".to_owned()),
                    "tls_v1": false,
                    "snappy": false,
                    "deflate": false,
                    "feature_negotiation": true,
               //      "heartbeat_interval": 5000,
               //      "output_buffer_size": 16 * 1024,
               //      "output_buffer_timeout": 250,
                    "user_agent": ::USER_AGENT
                });

                let identify = Command::Identify(obj);
                println!("send");
                framed.send(identify)
            })
            .and_then(|framed| {
                println!("sent identify, receive response");
                framed.into_future().map_err(|(e, _)| e)
            })
            .and_then(move |(rst, framed)| {
                println!("response: {:?}", rst);
                // TODO
                // match rst {
                //     Some(NsqFramed::Response(Response::Ok)) => {
                //         // feature_negotiation false response Ok
                //     }
                //     Some(NsqFramed::Response(Response::Json(value))) => {
                //         // feature_negotiation true response Json object
                //         // let res = value.as_object().expect("expect JSON response");
                //         let res: Map<_, _> = value.into();
                //         if let Some(true) = res.get("tls_v1") {
                //             // Upgrade to TLS
                //         } else if let Some(true) = res.get("snappy") {
                //             // Upgrade to snappy
                //         } else if let Some(true) = res.get("deflate") {
                //             // Upgrade to deflate
                //             let deflate_level = res.get("deflate_level").unwrap_or(0);
                //             let max_deflate_level = res.get("max_deflate_level").unwrap_or(6);
                //         } else {
                //         }
                //     }
                //     Some(NsqFramed::Response(Response::Heartbeat)) => {
                //         //
                //     }
                //     Some(NsqFramed::Response(Response::CloseWait)) => {
                //         //
                //     }
                //     None => {
                //     }
                // }
                // Ok(framed)
                Ok(Connection { addr: addr, transport: framed })
            })
            .map_err(|e| {
                println!("error: {:?}", e);
                e
            });
        Box::new(done)
    }

    // fn identify(self, framed: Framed<TcpStream, NsqCodec>)
    //     -> Box<Future<Item = Option<>, Error = Error>> {
    //     let obj = json!({
    //         "client_id": "nsq-rust",
    //         "hostname": ::hostname::get_hostname().unwrap_or_else(|| "unknown".to_owned()),
    //         "tls_v1": false,
    //         "snappy": false,
    //         "deflate": false,
    //         "feature_negotiation": true,
    //    //      "heartbeat_interval": 5000,
    //    //      "output_buffer_size": 16 * 1024,
    //    //      "output_buffer_timeout": 250,
    //         "user_agent": ::USER_AGENT
    //     });
    //
    //     let identify = Command::Identify(obj);
    //     println!("send");
    //     framed.send(identify)
    //         .and_then(|framed| {
    //             println!("sent identify, receive response");
    //             framed.into_future().map_err(|(e, _)| e)
    //         })
    //     let done = TcpStream::connect(&self.addr)
    //         .map_err(From::from)
    //         .and_then(|tcp| {
    //             let framed = NsqCodec::new(true).framed(tcp);
    //             framed.send(Command::Version)
    //         })
    //         .and_then(|framed| {
    //         })
    //         .and_then(|framed| {
    //             println!("sent identify, receive response");
    //             framed.into_future().map_err(|(e, _)| e)
    //         })
    //         .and_then(move |(rst, framed)| {
    //             println!("response: {:?}", rst);
    //             // TODO
    //             // match rst {
    //             //     Some(NsqFramed::Response(Response::Ok)) => {
    //             //         // feature_negotiation false response Ok
    //             //     }
    //             //     Some(NsqFramed::Response(Response::Json(value))) => {
    //             //         // feature_negotiation true response Json object
    //             //         // let res = value.as_object().expect("expect JSON response");
    //             //         let res: Map<_, _> = value.into();
    //             //         if let Some(true) = res.get("tls_v1") {
    //             //             // Upgrade to TLS
    //             //         } else if let Some(true) = res.get("snappy") {
    //             //             // Upgrade to snappy
    //             //         } else if let Some(true) = res.get("deflate") {
    //             //             // Upgrade to deflate
    //             //             let deflate_level = res.get("deflate_level").unwrap_or(0);
    //             //             let max_deflate_level = res.get("max_deflate_level").unwrap_or(6);
    //             //         } else {
    //             //         }
    //             //     }
    //             //     Some(NsqFramed::Response(Response::Heartbeat)) => {
    //             //         //
    //             //     }
    //             //     Some(NsqFramed::Response(Response::CloseWait)) => {
    //             //         //
    //             //     }
    //             //     None => {
    //             //     }
    //             // }
    //             // Ok(framed)
    //             Ok(Connection { addr: addr, transport: framed })
    //         })
    //         .map_err(|e| {
    //             println!("error: {:?}", e);
    //             e
    //         });
    //     Box::new(done)
    // }
    //
    // fn upgrade_tls() {
    // }
    //
    // fn upgrade_deflate() {
    // }
    //
    // fn upgrade_snappy() {
    // }
    //
    // fn auth() {
    // }
}

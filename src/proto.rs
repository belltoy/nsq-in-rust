use std::io;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::pipeline::ClientProto;

pub struct NsqProto {
    client_id: String,
    hostname: String,
    feature_negotiation: bool,
    heartbeat_interval: i32,
    output_buffer_size: i32,
    tls_v1: bool,
    compression: Compression,
    sample_rate: u32,
    user_agent: String,
    msg_timeout: u32,
};

enum Compression {
    Disabled,
    Snappy(bool), /* snappy enabled */
    Deflate(bool, u32), /* deflate enabled, level */
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for NsqProto
{
    type Request = Command;
    type Response = NsqFramed;

    type Transport = Framed<T, NsqCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        let buf = "  V2".as_ref();
        io.write_buf(buf);

        let io = io.framed(NsqCodec::new());

        io
            .send(Command::Identify(json))
            .and_then(|io| {
                io.into_future()
            })
            .and_then(|(response, io)| {
                match response {
                    NsqFramed::Response(Response::Ok) => {
                    }

                    NsqFramed::Response(Response::Heartbeat) => {
                    }

                    NsqFramed::Response(Response::Payload(data)) => {
                        // decode json payload
                        let res = serde_json::from_slice(data)?;

                        let tls_v1 = res["tls_v1"];
                        let snappy = res["snappy"];
                        let deflate = res["deflate"];

                        let framed = if res["tls_v1"] {
                            // do TLS handshake
                            let io = io.into_parts().inner;
                            let connector = TlsConnector::builder().unwrap().build().unwrap();
                            connector
                                .connect_async("domain.com", io)
                                .and_then(|io| {
                                    io.framed(NsqCodec::new())
                                })
                                .and_then(|io| {
                                    receive_ok(io)
                                })
                        } else {
                            Ok(io)
                        };

                        framed.and_then(|stream| {
                            if snappy || deflate {
                                receive_ok(stream)
                            } else {
                                Ok(stream)
                            }
                        })
                    }

                    NsqFramed::Error(error) => {
                    }
                }
            })
    }
}

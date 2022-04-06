use tokio::net::TcpStream;
use crate::config::TlsConfig;
use crate::error::Error;
use crate::codec::NsqCodec;

#[cfg(feature = "tls-native")]
pub(crate) use tokio_native_tls::{TlsConnector, TlsStream};
#[cfg(feature = "tls-tokio")]
pub(crate) use tokio_rustls::{
    rustls::RootCertStore,
    rustls::client::{
        ClientConfig,
        ServerName,
    },
    client::TlsStream,
    TlsConnector,
};

#[cfg(feature = "tls-tokio")]
async fn upgrade_tls(domain: &str, inner: TcpStream, tls_config: &TlsConfig, nsq_codec: &mut NsqCodec)
    -> Result<TlsStream<TcpStream>, Error>
{
    todo!()
    // // TODO from config
    // let root_certs = RootCertStore { roots: vec![] };
    // let client_config = ClientConfig::builder()
    //     .with_safe_defaults()
    //     .with_root_certificates(root_certs)
    //     .with_no_client_auth();
    // let client_config = Arc::new(client_config);
    // let connector = TlsConnector::from(client_config);
    // // TODO FIXME data from peer may have already read in the buffer
    // let mut tls_socket = connector.connect(ServerName::try_from(tls_config.domain.as_str())?, inner).await?;
    // if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut tls_socket, nsq_codec).await? {
    //     // Ok(Box::new(tls_socket))
    //     Ok(tls_socket)
    // } else {
    //     Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    // }
}

#[cfg(feature = "tls-native")]
async fn upgrade_tls<T>(domain: &str, inner: T, tls_config: &TlsConfig, nsq_codec: &mut NsqCodec)
    // -> Result<TlsStream, Error>
    -> Result<Box<dyn AsyncRead + AsyncWrite + Unpin>, Error>
    where T: AsyncRead + AsyncWrite + Unpin,
{
    let connector = native_tls::TlsConnector::new().unwrap();
    let connector = TlsConnector::from(connector);
    // TODO FIXME data from peer may have already read in the buffer
    let tls_socket = connector.connect(domain, inner).await?;
    if let NsqFramed::Response(RawResponse::ok) = read_response(&mut tls_socket, nsq_codec).await?{
        Ok(Box::new(tls_socket))
    } else {
        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }
}

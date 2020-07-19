use std::io;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    Utf8Error(std::str::Utf8Error),
    JsonError(serde_json::Error),
    NsqError(NsqError),
    #[cfg(feature = "tls-native")]
    TlsError(native_tls::Error),
    #[cfg(feature = "tls-tokio")]
    InvalidDnsNameError(tokio_rustls::rustls::client::InvalidDnsNameError),
    SnapError(snap::Error),
    DeflateCompressError(flate2::CompressError),
    DeflateDecompressError(flate2::DecompressError),
    HttpError(reqwest::Error),
    Auth(String),
}

#[derive(Debug)]
pub struct NsqError {
    code: String,
    description: String,
}

impl NsqError {
    pub fn new<S1, S2>(code: S1, description: S2) -> NsqError
        where S1: Into<String>,
              S2: Into<String>,
    {
        NsqError {
            code: code.into(),
            description: description.into(),
        }
    }

    pub fn is_fatal(&self) -> bool {
        match self.code.as_str() {
            "E_FIN_FAILED" | "E_REQ_FAILED" | "E_TOUCH_FAILED" => false,
            _ => true,
        }
    }
}

impl std::error::Error for NsqError {}

impl std::fmt::Display for NsqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NsqError: {}, description: {}", self.code, self.description)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use Error::*;
        match self {
            IoError(e) => Some(e),
            Utf8Error(e) => Some(e),
            JsonError(e) => Some(e),
            NsqError(e) => Some(e),
            #[cfg(feature = "tls-native")]
            TlsError(e) => Some(e),
            #[cfg(feature = "tls-tokio")]
            InvalidDnsNameError(e) => Some(e),
            SnapError(e) => Some(e),
            DeflateCompressError(e) => Some(e),
            DeflateDecompressError(e) => Some(e),
            HttpError(e) => Some(e),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error::*;
        match self {
            IoError(e) => e.fmt(f),
            Utf8Error(e) => e.fmt(f),
            JsonError(e) => e.fmt(f),
            NsqError(e) => e.fmt(f),
            #[cfg(feature = "tls-native")]
            TlsError(e) => e.fmt(f),
            #[cfg(feature = "tls-tokio")]
            InvalidDnsNameError(e) => e.fmt(f),
            SnapError(e) => e.fmt(f),
            DeflateCompressError(e) => e.fmt(f),
            DeflateDecompressError(e) => e.fmt(f),
            Auth(e) => write!(f, "Auth Error: {}", e),
            HttpError(e) => e.fmt(f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IoError(e)
    }
}

impl From<NsqError> for Error {
    fn from(e: NsqError) -> Error {
        Error::NsqError(e)
    }
}

impl From<::std::str::Utf8Error> for Error {
    fn from(e: ::std::str::Utf8Error) -> Error {
        Error::Utf8Error(e)
    }
}

impl From<::serde_json::Error> for Error {
    fn from(e: ::serde_json::Error) -> Error {
        Error::JsonError(e)
    }
}

#[cfg(feature = "tls-native")]
impl From<::native_tls::Error> for Error {
    fn from(e: native_tls::Error) -> Error {
        Error::TlsError(e)
    }
}

#[cfg(feature = "tls-tokio")]
impl From<tokio_rustls::rustls::client::InvalidDnsNameError> for Error {
    fn from(e: tokio_rustls::rustls::client::InvalidDnsNameError) -> Error {
        Error::InvalidDnsNameError(e)
    }
}

impl From<snap::Error> for Error {
    fn from(e: snap::Error) -> Error {
        Error::SnapError(e)
    }
}

impl From<flate2::CompressError> for Error {
    fn from(e: flate2::CompressError) -> Error {
        Error::DeflateCompressError(e)
    }
}

impl From<flate2::DecompressError> for Error {
    fn from(e: flate2::DecompressError) -> Error {
        Error::DeflateDecompressError(e)
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Error {
        Error::HttpError(e)
    }
}

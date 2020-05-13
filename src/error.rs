use std::io;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    Utf8Error(std::str::Utf8Error),
    JsonError(serde_json::Error),
    NsqError(NsqError),
    TlsError(native_tls::Error),
    // #[cfg(future = "snappy")]
    SnapError(snap::Error),
    DeflateCompressError(flate2::CompressError),
    DeflateDecompressError(flate2::DecompressError),
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

impl From<::native_tls::Error> for Error {
    fn from(e: native_tls::Error) -> Error {
        Error::TlsError(e)
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

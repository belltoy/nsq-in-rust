use std::io;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    Utf8Error(::std::str::Utf8Error),
    JsonError(::serde_json::Error),
    NsqError(NsqError),
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

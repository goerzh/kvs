use failure::Fail;
use serde_json;
use std::io;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Input was invalid UTF-8 at index {}", _0)]
    Utf8Error(usize),
    #[fail(display = "{}", _0)]
    Io(io::Error),
    #[fail(display = "{}", _0)]
    Serde(serde_json::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// UnexpectedCommandType indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    /// UnexpectedResponseType indicated a invalid response.
    #[fail(display = "Unexpected response type")]
    UnexpectedResponseType,
    #[fail(display = "{}", _0)]
    StringErr(String),
}

impl From<io::Error> for KvsError {
    fn from(f: io::Error) -> Self {
        KvsError::Io(f)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(f: serde_json::Error) -> Self {
        KvsError::Serde(f)
    }
}

impl From<String> for KvsError {
    fn from(f: String) -> Self {
        KvsError::StringErr(f)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;

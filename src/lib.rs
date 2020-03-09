#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;

pub mod client;
pub mod common;
pub mod config;
pub mod connection;
pub mod error;
pub mod kv;
pub mod server;

pub use config::{Config, Engine};
pub use connection::Connection;
pub use error::{KvsError, Result};
pub use kv::{Command, KvStore, KvsEngine};

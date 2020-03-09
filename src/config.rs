use slog::Logger;
use std::net::SocketAddr;
use std::path::PathBuf;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum Engine {
        kvs,
        sled
    }
}

pub struct Config {
    pub addr: SocketAddr,
    pub path: PathBuf,
    pub engine: Engine,
    pub log: Logger,
}

impl Config {
    pub fn new(addr: SocketAddr, path: PathBuf, engine: Engine, log: Logger) -> Self {
        Config {
            addr,
            path,
            engine,
            log,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.addr.clone()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

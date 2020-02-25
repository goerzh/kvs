use std::net::SocketAddr;
use std::path::PathBuf;

pub struct Config {
    addr: SocketAddr,
    path: PathBuf,
}

impl Config {
    pub fn new(addr: SocketAddr, path: PathBuf) -> Self {
        Config { addr, path }
    }

    pub fn address(&self) -> SocketAddr {
        self.addr.clone()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

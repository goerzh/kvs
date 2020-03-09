#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use kvs::server::Server;
use kvs::{Config, Engine, Result};
use slog::{Drain, Logger};
use std::env::current_dir;
use std::net::SocketAddr;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(StructOpt)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(long, help = "Sets the kv engine", value_name = "ENGINE-NAME")]
    engine: Option<Engine>,
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = "IP:PORT",
        default_value = DEFAULT_LISTENING_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

pub fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let root = slog::Logger::root(drain, o!());

    let opt = Opt::from_args();
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    let config = Config::new(opt.addr, current_dir()?, engine, root);
    run(config)?;
    Ok(())
}

fn run(cfg: Config) -> Result<()> {
    info!(cfg.log, "kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!(cfg.log, "Storage engine: {}", cfg.engine);
    info!(cfg.log, "Listening on {}", cfg.addr);
    let mut server = Server::new(cfg);
    server.run()?;
    Ok(())
}

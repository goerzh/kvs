use kvs::server::Server;
use kvs::{Config, Result};
use std::env::current_dir;
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(
        long,
        help = "Sets the kv engine",
        value_name = "ENGINE-NAME",
        default_value = "kvs"
    )]
    engine: String,
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

pub fn main() -> Result<()> {
    let opt = Opt::from_args();
    let config = Config::new(opt.addr, current_dir()?);
    let mut server = Server::new(config);
    server.run()?;
    Ok(())
}

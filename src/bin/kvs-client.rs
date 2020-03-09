use kvs::client::Client;
use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::net::SocketAddr;
use std::process;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs-client")]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
    #[structopt(
        long,
        help = "Sets the server address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
}

#[derive(StructOpt)]
enum Command {
    #[structopt(name = "get", about = "Gets the string value from a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
    },
    #[structopt(name = "set", about = "Sets a key/value string pair")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", help = "A string value of key")]
        value: String,
    },
    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
    },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    match opt.command {
        Command::Set { key, value } => {
            let mut client = Client::connect(opt.addr)?;
            client.set(key, value)?;
        }
        Command::Get { key } => {
            let mut client = Client::connect(opt.addr)?;
            match client.get(key)? {
                Some(value) => {
                    println!("{:?}", value);
                }
                None => {
                    println!("Key not found");
                }
            }
        }
        Command::Remove { key } => {
            let mut client = Client::connect(opt.addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}

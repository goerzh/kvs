use structopt::StructOpt;
use std::process;
use kvs::{Result, KvStore, KvsError};
use std::env::current_dir;
use std::process::exit;

#[derive(StructOpt)]
#[structopt(about = "kvs subcommands")]
enum Opt {
    /// Gets the string value from a given string key
    Get(Key),
    /// Sets a key/value string pair
    Set(Pair),
    /// Removes a given key
    Rm(Key),
}

#[derive(Debug)]
#[derive(StructOpt)]
struct Key {
    #[structopt(name = "KEY")]
    key: String,
}

#[derive(Debug)]
#[derive(StructOpt)]
struct Pair {
    #[structopt(name = "KEY")]
    key: String,
    #[structopt(name = "VALUE")]
    value: String,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Get(_k) => {
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(_k.key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        },
        Opt::Set(_p) => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(_p.key, _p.value);
        },
        Opt::Rm(_k) => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(_k.key) {
                Ok(_) => {},
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e)
            }
        }
    };

    Ok(())
}

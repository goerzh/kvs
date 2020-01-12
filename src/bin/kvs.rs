use structopt::StructOpt;
use std::process;

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

fn main() {
    let opt = Opt::from_args();
    match opt {
        Opt::Get(_k) => {
            eprintln!("unimplemented");
            process::exit(1);
        },
        Opt::Set(_p) => {
            eprintln!("unimplemented");
            process::exit(1);
        },
        Opt::Rm(_k) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}

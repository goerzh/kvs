use clap::{App, Arg, SubCommand};
use std::process;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("Memory key/value store")
        .subcommand(
            SubCommand::with_name("set")
                .about("set key/value")
                .arg(
                    Arg::with_name("KEY")
                        .value_name("KEY")
                        .index(1)
                        .required(true)
                        .help(" KEY value"),
                )
                .arg(
                    Arg::with_name("VALUE")
                        .value_name("VALUE")
                        .index(2)
                        .required(true)
                        .help("VALUE value"),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get value from key")
                .arg(Arg::with_name("KEY").value_name("KEY").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove KEY/VALUE")
                .arg(Arg::with_name("KEY").value_name("KEY").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("get", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("set", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("rm", Some(_m)) => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => {
            println!("unreachable");
            process::exit(1);
        }
    }
}

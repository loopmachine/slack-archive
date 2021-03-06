#[macro_use]
extern crate failure;

extern crate rusqlite;
extern crate slack_api as slack;

mod archive;
mod search;

use std::env;
use failure::Error;

fn main() {
    if let Err(err) = run() {
        use std::io::Write;
        let stderr = &mut ::std::io::stderr();

        for cause in err.causes() {
            writeln!(stderr, "{}", cause).expect("unable to write to stderr");
        }

        if env::var("RUST_BACKTRACE").unwrap_or_default() == "1" {
            // this prints the backtrace
            writeln!(stderr, "{:?}.", err).expect("unable to write to stderr");
        }
        ::std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        match args[1].as_ref() {
            "archive" => archive::archive(),
            "search" => search::search(),
            cmd @ _ => Err(format_err!("invalid command: {}", cmd)),
        }
    } else {
        // default cmd
        archive::archive()
    }
}

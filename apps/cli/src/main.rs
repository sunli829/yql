use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-cli")]
struct Options {
    /// YQL Server address
    host: String,
}

fn main() {
    let options: Options = Options::from_args();
}

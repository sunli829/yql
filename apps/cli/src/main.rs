use anyhow::Result;
use futures_util::TryFutureExt;
use structopt::StructOpt;
use yql_dataset::dataset::DataSet;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-cli")]
struct Options {
    #[structopt(default_value = "http://localhost:33001")]
    /// YQL Server url
    url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options: Options = Options::from_args();
    let cli = reqwest::Client::builder().gzip(true).brotli(true).build()?;

    let mut rl = rustyline::Editor::<()>::new();

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let res = cli
                    .post(&options.url)
                    .json(&serde_json::json!({
                        "sql": line,
                    }))
                    .send()
                    .and_then(|resp| async move { resp.error_for_status() })
                    .and_then(|resp| resp.json::<DataSet>())
                    .await;

                match res {
                    Ok(dataset) => {
                        println!("{}", dataset);
                        println!();
                    }
                    Err(err) => {
                        println!("Error: {}", err);
                    }
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}

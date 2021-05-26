use anyhow::Result;
use futures_util::TryFutureExt;
use std::path::PathBuf;
use structopt::StructOpt;
use yql_dataset::dataset::DataSet;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-cli")]
struct Options {
    #[structopt(default_value = "http://localhost:33001")]
    /// YQL Server url
    url: String,
}

fn history_path() -> Option<PathBuf> {
    dirs::document_dir().map(|path| path.join(".yql-cli-history"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let options: Options = Options::from_args();
    let cli = reqwest::Client::builder().gzip(true).brotli(true).build()?;

    let mut rl = rustyline::Editor::<()>::new();

    if let Some(path) = history_path() {
        let _ = rl.history_mut().load(&path);
    }

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.history_mut().add(&line);
                let res = cli
                    .post(&format!("{}/sql", options.url))
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

    if let Some(path) = history_path() {
        let _ = rl.history_mut().save(&path);
    }

    Ok(())
}

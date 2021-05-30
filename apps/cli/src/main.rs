use std::path::PathBuf;

use anyhow::Result;
use futures_util::StreamExt;
use structopt::StructOpt;
use tonic::Request;
use yql_dataset::dataset::DataSet;
use yql_protocol::{execute_response, ExecuteRequest};

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
    let mut client = yql_protocol::yql_client::YqlClient::connect(options.url).await?;

    let mut rl = rustyline::Editor::<()>::new();

    if let Some(path) = history_path() {
        let _ = rl.history_mut().load(&path);
    }

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                rl.history_mut().add(line);

                match client
                    .execute(Request::new(ExecuteRequest {
                        sql: line.to_string(),
                    }))
                    .await
                {
                    Ok(resp) => {
                        let stream = resp.into_inner().take_until(tokio::signal::ctrl_c());
                        tokio::pin!(stream);

                        let mut first = true;

                        while let Some(res) = stream.next().await {
                            let resp = match res {
                                Ok(resp) => resp,
                                Err(err) => {
                                    println!("Error: {}", err);
                                    break;
                                }
                            };

                            match resp.item {
                                Some(execute_response::Item::Dataset(dataset)) => {
                                    let dataset: DataSet =
                                        match bincode::deserialize(&dataset.dataset) {
                                            Ok(dataset) => dataset,
                                            Err(err) => {
                                                println!("Error: {}", err);
                                                break;
                                            }
                                        };
                                    if first {
                                        println!("{}", dataset.display());
                                        first = false;
                                    } else {
                                        println!("{}", dataset.display_no_header());
                                    }
                                }
                                Some(execute_response::Item::Metrics(
                                    execute_response::Metrics {
                                        start_time,
                                        end_time,
                                        num_input_rows,
                                        num_output_rows,
                                    },
                                )) => {
                                    println!(
                                        "Input {} rows, output {} rows, done in {:.3} seconds.",
                                        num_input_rows,
                                        num_output_rows,
                                        (end_time - start_time) as f64 / 1000.0
                                    )
                                }
                                Some(execute_response::Item::Error(execute_response::Error {
                                    error,
                                })) => {
                                    println!("Error: {}", error);
                                    break;
                                }
                                None => break,
                            }
                        }
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

use std::convert::TryInto;

use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use std::path::PathBuf;
use std::time::Instant;
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

fn create_dataset_stream(
    mut stream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
) -> BoxStream<'static, Result<DataSet>> {
    enum State {
        Header([u8; 5], usize),
        Body(bool, Vec<u8>, usize),
    }

    let mut state = State::Header(Default::default(), 0);

    Box::pin(async_stream::try_stream! {
        while let Some(res) = stream.next().await {
            let mut data = res?;

            while !data.is_empty() {
                match &mut state {
                    State::Header(header_data, size) => {
                        let rsz = (header_data.len() - *size).min(data.len());
                        header_data.copy_from_slice(&data.split_to(rsz));
                        *size += rsz;
                        if *size == 5 {
                            let is_dataset = header_data[0] == 1;
                            let body_size = u32::from_le_bytes(header_data[1..].try_into().unwrap());
                            state = State::Body(is_dataset, Vec::new(), body_size as usize);
                        }
                    }
                    State::Body(is_dataset, body_data, body_size) => {
                        let rsz = (*body_size - body_data.len()).min(data.len());
                        body_data.extend(data.split_to(rsz));
                        if body_data.len() == *body_size {
                            if *is_dataset {
                                let dataset: DataSet = bincode::deserialize(&body_data)?;
                                yield dataset;
                                state = State::Header(Default::default(), 0);
                            } else {
                                let err_msg = std::str::from_utf8(&body_data)?;
                                Err(anyhow::anyhow!("{}", err_msg))?;
                            }
                        }
                    }
                }
            }
        }
    })
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
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                rl.history_mut().add(line);
                let start_time = Instant::now();
                let res = cli
                    .post(&format!("{}/sql", options.url))
                    .body(line.to_string())
                    .send()
                    .await;
                let resp = match res {
                    Ok(resp) => resp,
                    Err(err) => {
                        println!("Error: {}", err);
                        continue;
                    }
                };

                if !resp.status().is_success() {
                    match resp.bytes().await {
                        Ok(data) => match std::str::from_utf8(&data) {
                            Ok(err_msg) => {
                                println!("Error: {}", err_msg);
                                continue;
                            }
                            _ => {
                                println!("Error: unknown");
                                continue;
                            }
                        },
                        Err(err) => {
                            println!("Error: {}", err);
                            continue;
                        }
                    }
                }

                let stream =
                    create_dataset_stream(resp.bytes_stream()).take_until(tokio::signal::ctrl_c());
                tokio::pin!(stream);

                let mut first = true;
                while let Some(res) = stream.next().await {
                    match res {
                        Ok(dataset) => {
                            if first {
                                print!("{}", dataset.display());
                                first = false;
                            } else {
                                print!("{}", dataset.display_no_header());
                            }
                        }
                        Err(err) => {
                            println!("Error: {}", err);
                            break;
                        }
                    }
                    println!();
                }
                println!();
                println!(
                    "Done in {:.3} seconds.",
                    (Instant::now() - start_time).as_secs_f32()
                );
            }
            Err(_) => break,
        }
    }

    if let Some(path) = history_path() {
        let _ = rl.history_mut().save(&path);
    }

    Ok(())
}

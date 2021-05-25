use std::path::PathBuf;

use anyhow::Result;
use serde::Deserialize;
use std::convert::Infallible;
use structopt::StructOpt;
use warp::http::StatusCode;
use warp::{Filter, Reply};
use yql_service::Service;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-server")]
struct Options {
    #[structopt(parse(from_os_str), long = "data-dir", default_value = "data")]
    data_dir: PathBuf,
}

#[derive(Deserialize)]
struct ExecuteSql {
    sql: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::from_args();
    let service = Service::open(&opt.data_dir)?;

    let post_sql = warp::post()
        .and(warp::path!("sql"))
        .and(warp::body::json::<ExecuteSql>())
        .and_then({
            let service = service.clone();
            move |req: ExecuteSql| {
                let service = service.clone();
                async move {
                    match service.execute(&req.sql).await {
                        Ok(dataset) => {
                            Ok::<_, Infallible>(warp::reply::json(&dataset).into_response())
                        }
                        Err(err) => Ok(warp::reply::with_status(
                            err.to_string(),
                            StatusCode::BAD_REQUEST,
                        )
                        .into_response()),
                    }
                }
            }
        });

    let routes = post_sql;

    warp::serve(routes).bind(([0, 0, 0, 0], 33001)).await;
    Ok(())
}

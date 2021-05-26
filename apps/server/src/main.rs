use std::convert::Infallible;
use std::path::PathBuf;

use anyhow::Result;
use serde::Deserialize;
use structopt::StructOpt;
use warp::http::StatusCode;
use warp::hyper::body::Bytes;
use warp::{Filter, Reply, Stream};
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

fn create_body_stream(
    stream: impl Stream<Item = Result<DataSet>> + Unpin + Send + 'static,
) -> impl Stream<Item = Result<Bytes>> + Send + 'static {
    async_stream::try_stream! {}
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
                    let res = service.execute(&req.sql).await;
                    match res {
                        Ok(stream) => Ok::<_, Infallible>(
                            warp::reply::Response::new(create_body_stream(stream)).into_response(),
                        ),
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

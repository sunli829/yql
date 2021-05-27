use std::convert::Infallible;
use std::path::PathBuf;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use hyper::Body;
use structopt::StructOpt;
use warp::http::StatusCode;
use warp::reply::Response;
use warp::{Filter, Reply, Stream};
use yql_core::dataset::DataSet;
use yql_service::Service;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-server")]
struct Options {
    #[structopt(parse(from_os_str), long = "data-dir", default_value = "data")]
    data_dir: PathBuf,
}

fn create_body_stream(
    mut stream: impl Stream<Item = Result<DataSet>> + Unpin + Send + 'static,
) -> Response {
    let bytes_stream: BoxStream<'static, Result<Bytes>> = Box::pin(async_stream::try_stream! {
        while let Some(res) = stream.next().await {
            match res {
                Ok(dataset) => {
                    let data = bincode::serialize(&dataset)?;
                    let mut bytes = BytesMut::new();
                    bytes.put_u8(1);
                    bytes.put_u32_le(data.len() as u32);
                    bytes.put_slice(&data);
                    yield bytes.freeze();
                }
                Err(err) => {
                    let mut bytes = BytesMut::new();
                    bytes.put_u8(0);
                    let err_str = err.to_string();
                    bytes.put_u32_le(err_str.len() as u32);
                    bytes.put_slice(err_str.as_bytes());
                    yield bytes.freeze();
                }
            }
        }
    });
    Response::new(Body::wrap_stream(bytes_stream))
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::from_args();
    let service = Service::open(&opt.data_dir)?;

    let post_sql = warp::post()
        .and(warp::path!("sql"))
        .and(warp::body::bytes())
        .and_then({
            let service = service.clone();
            move |req: Bytes| {
                let service = service.clone();
                async move {
                    match std::str::from_utf8(&req) {
                        Ok(sql) => match service.execute(sql).await {
                            Ok(stream) => Ok::<_, Infallible>(create_body_stream(stream)),
                            Err(err) => Ok(warp::reply::with_status(
                                err.to_string(),
                                StatusCode::BAD_REQUEST,
                            )
                            .into_response()),
                        },
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

mod rpc_yql_service;

use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;
use yql_protocol::yql_server::YqlServer;
use yql_service::Service;

use rpc_yql_service::RpcYqlService;

#[derive(Debug, StructOpt)]
#[structopt(name = "yql-server")]
struct Options {
    #[structopt(parse(from_os_str), long = "data-dir", default_value = "data")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Options = Options::from_args();
    let service = Service::open(&opt.data_dir)?;

    tonic::transport::Server::builder()
        .add_service(YqlServer::new(RpcYqlService::new(service)))
        .serve("0.0.0.0:33001".parse().unwrap())
        .await?;

    Ok(())
}

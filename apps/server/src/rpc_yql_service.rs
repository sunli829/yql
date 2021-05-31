use std::pin::Pin;

use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use yql_protocol::{execute_response, ExecuteRequest, ExecuteResponse};
use yql_service::{ExecuteResult, ExecuteStreamItem, Service};

pub struct RpcYqlService {
    service: Service,
}

impl RpcYqlService {
    pub fn new(service: Service) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl yql_protocol::yql_server::Yql for RpcYqlService {
    type ExecuteStream =
        Pin<Box<dyn Stream<Item = Result<ExecuteResponse, Status>> + Send + Sync + 'static>>;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let service = self.service.clone();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            match service.execute(&request.into_inner().sql).await {
                Ok(ExecuteResult::DataSet(dataset)) => {
                    let data = match bincode::serialize(&dataset) {
                        Ok(data) => data,
                        Err(err) => {
                            tx.send(Err(Status::internal(err.to_string()))).await.ok();
                            return;
                        }
                    };

                    tx.send(Ok(ExecuteResponse {
                        item: Some(execute_response::Item::Dataset(execute_response::DataSet {
                            dataset: data,
                        })),
                    }))
                    .await
                    .ok();
                }
                Ok(ExecuteResult::ExecStream(mut stream)) => {
                    let mut num_output_rows = 0;
                    while let Some(res) = stream.next().await {
                        let item = match res {
                            Ok(item) => item,
                            Err(err) => {
                                tx.send(Err(Status::internal(err.to_string()))).await.ok();
                                return;
                            }
                        };

                        match item {
                            ExecuteStreamItem::DataSet(dataset) => {
                                num_output_rows += dataset.len();
                                let data = match bincode::serialize(&dataset) {
                                    Ok(data) => data,
                                    Err(err) => {
                                        tx.send(Err(Status::internal(err.to_string()))).await.ok();
                                        return;
                                    }
                                };

                                tx.send(Ok(ExecuteResponse {
                                    item: Some(execute_response::Item::Dataset(
                                        execute_response::DataSet { dataset: data },
                                    )),
                                }))
                                .await
                                .ok();
                            }
                            ExecuteStreamItem::Metrics(metrics) => {
                                tx.send(Ok(ExecuteResponse {
                                    item: Some(execute_response::Item::Metrics(
                                        execute_response::Metrics {
                                            start_time: metrics.start_time.unwrap_or_default(),
                                            end_time: metrics.end_time.unwrap_or_default(),
                                            num_input_rows: metrics.num_input_rows as i64,
                                            num_output_rows: num_output_rows as i64,
                                        },
                                    )),
                                }))
                                .await
                                .ok();
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tx.send(Err(Status::internal(err.to_string()))).await.ok();
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use yql_dataset::DataSet;
use yql_planner::SinkProvider;

pub struct ConsoleSink;

impl SinkProvider for ConsoleSink {
    fn provider_name(&self) -> &'static str {
        "console"
    }

    fn create(
        &self,
        mut stream: BoxStream<'static, Result<DataSet>>,
    ) -> Result<BoxFuture<'static, Result<()>>> {
        Ok(Box::pin(async move {
            while let Some(res) = stream.next().await {
                let dataset = res?;
                println!("{}", dataset);
            }
            Ok(())
        }))
    }
}

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use yql_dataset::DataSet;

pub trait SinkProvider: Send + Sync + 'static {
    fn provider_name(&self) -> &'static str;

    fn create(
        &self,
        stream: BoxStream<'static, Result<DataSet>>,
    ) -> Result<BoxFuture<'static, Result<()>>>;
}

use anyhow::Result;

use crate::dataset::DataSet;

#[async_trait::async_trait]
pub trait Sink {
    async fn send(&mut self, dataset: DataSet) -> Result<()>;
}

pub type BoxSink = Box<dyn Sink + Send + 'static>;

pub trait SinkProvider: Send + Sync + 'static {
    fn provider_name(&self) -> &'static str;

    fn create(&self) -> Result<BoxSink>;
}

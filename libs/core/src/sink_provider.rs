use anyhow::Result;

use yql_dataset::dataset::DataSet;

#[async_trait::async_trait]
pub trait Sink {
    async fn send(&mut self, dataset: DataSet) -> Result<()>;
}

pub type BoxSink = Box<dyn Sink + Send + 'static>;

pub trait SinkProvider: Send + 'static {
    fn provider_name(&self) -> &'static str;

    fn create(&self) -> Result<BoxSink>;
}

impl SinkProvider for Box<dyn SinkProvider> {
    fn provider_name(&self) -> &'static str {
        self.as_ref().provider_name()
    }

    fn create(&self) -> Result<BoxSink> {
        self.as_ref().create()
    }
}

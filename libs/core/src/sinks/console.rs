use anyhow::Result;

use crate::dataset::DataSet;
use crate::{BoxSink, Sink, SinkProvider};

struct ConsoleSink;

#[async_trait::async_trait]
impl Sink for ConsoleSink {
    async fn send(&mut self, dataset: DataSet) -> Result<()> {
        println!("{}", dataset.display());
        Ok(())
    }
}

pub struct Console;

impl SinkProvider for Console {
    fn provider_name(&self) -> &'static str {
        "console"
    }

    fn create(&self) -> Result<BoxSink> {
        Ok(Box::new(ConsoleSink))
    }
}

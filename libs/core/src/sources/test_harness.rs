use anyhow::Result;
use futures_util::stream::BoxStream;

use crate::dataset::{DataSet, SchemaRef};
use crate::{GenericSourceDataSet, GenericSourceProvider};

pub struct Provider {
    schema: SchemaRef,
    datasets: Vec<DataSet>,
}

impl GenericSourceProvider for Provider {
    type State = usize;

    fn provider_name(&self) -> &'static str {
        "test"
    }

    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn create_stream(
        &self,
        state: Option<Self::State>,
    ) -> Result<BoxStream<'static, Result<GenericSourceDataSet<Self::State>>>> {
        let datasets = self
            .datasets
            .clone()
            .into_iter()
            .enumerate()
            .skip(state.unwrap_or_default());
        Ok(Box::pin(async_stream::try_stream! {
            for (offset, dataset) in datasets {
                yield GenericSourceDataSet {
                    state: offset,
                    dataset,
                };
            }
        }))
    }
}

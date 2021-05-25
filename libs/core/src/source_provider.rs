use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_stream::StreamExt;
use yql_dataset::dataset::{DataSet, SchemaRef};

pub struct GenericSourceDataSet<T> {
    pub state: T,
    pub dataset: DataSet,
}

pub type SourceDataSet = GenericSourceDataSet<Vec<u8>>;

pub trait GenericSourceProvider: Send + Sync + 'static {
    type State: Send + Sync + Serialize + DeserializeOwned + 'static;

    fn provider_name(&self) -> &'static str;

    fn schema(&self) -> Result<SchemaRef>;

    fn create_stream(
        &self,
        state: Option<Self::State>,
    ) -> Result<BoxStream<'static, Result<GenericSourceDataSet<Self::State>>>>;
}

pub type SourceProvider = Arc<dyn GenericSourceProvider<State = Vec<u8>>>;

pub struct SourceProviderWrapper<T>(pub T);

impl<T: GenericSourceProvider> GenericSourceProvider for SourceProviderWrapper<T> {
    type State = Vec<u8>;

    fn provider_name(&self) -> &'static str {
        self.0.provider_name()
    }

    fn schema(&self) -> Result<SchemaRef> {
        self.0.schema()
    }

    fn create_stream(
        &self,
        state: Option<Self::State>,
    ) -> Result<BoxStream<'static, Result<SourceDataSet>>> {
        let state = match state {
            Some(data) => Some(bincode::deserialize(&data).with_context(|| {
                format!(
                    "failed to deserialize state for source '{}'",
                    self.provider_name()
                )
            })?),
            None => None,
        };
        let inner_stream = self.0.create_stream(state)?;
        let provider_name = self.provider_name();

        Ok(Box::pin(async_stream::try_stream! {
            tokio::pin!(inner_stream);
            while let Some(GenericSourceDataSet { state, dataset }) = inner_stream.next().await.transpose()? {
                let state = bincode::serialize(&state).with_context(|| {
                    format!("failed to serialize state for source '{}'", provider_name)
                })?;
                yield SourceDataSet {
                    state,
                    dataset,
                };
            }
        }))
    }
}

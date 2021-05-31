use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use futures_util::stream::{Stream, StreamExt};

use crate::dataset::{DataSet, SchemaRef};
use crate::execution::execution_context::ExecutionContext;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::PhysicalPlan;
use crate::ExecutionMetrics;

pub struct CreateStreamContext {
    pub ctx: Arc<ExecutionContext>,
    pub prev_state: HashMap<usize, Vec<u8>>,
}

pub struct DataSetWithWatermark {
    pub watermark: Option<i64>,
    pub dataset: DataSet,
}

pub trait DataSetStream: Stream<Item = Result<DataSetWithWatermark>> {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()>;
}

pub type BoxDataSetStream = Pin<Box<dyn DataSetStream + Send + 'static>>;

pub struct DataStream {
    ctx: Arc<ExecutionContext>,
    input: BoxDataSetStream,
    schema: SchemaRef,
}

impl DataStream {
    pub(crate) fn new(plan: LogicalPlan, state: Option<Vec<u8>>) -> Result<Self> {
        let physical_plan = PhysicalPlan::try_new(plan)?;

        // load previous state
        let prev_state: HashMap<usize, Vec<u8>> = match state {
            Some(data) => {
                bincode::deserialize(&data).context("failed to deserialize stream state.")?
            }
            None => Default::default(),
        };

        let exec_ctx = Arc::new(ExecutionContext::new());
        let mut create_ctx = CreateStreamContext {
            ctx: exec_ctx.clone(),
            prev_state,
        };

        Ok(Self {
            ctx: exec_ctx,
            schema: physical_plan.root.schema(),
            input: crate::execution::streams::create_stream(&mut create_ctx, physical_plan.root)?,
        })
    }

    pub fn save_state(&self) -> Result<Vec<u8>> {
        let mut state = Default::default();
        self.input.save_state(&mut state)?;
        Ok(bincode::serialize(&state)?)
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.ctx.metrics()
    }
}

impl Stream for DataStream {
    type Item = Result<DataSet>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(dataset))) => Poll::Ready(Some(Ok(dataset.dataset))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::StreamExt;

    use crate::array::{ArrayRef, DataType, Int64Array, TimestampArray};
    use crate::dataset::{DataSet, Field, Schema, SchemaRef};
    use crate::dsl::*;
    use crate::sources::test_harness::Provider;
    use crate::{DataFrame, SourceProviderWrapper};

    fn create_dataset(schema: SchemaRef, i: i64, with_time: bool) -> DataSet {
        let mut columns = vec![
            Arc::new((i * 10..(i + 1) * 10).collect::<Int64Array>()) as ArrayRef,
            Arc::new(
                (i * 10..(i + 1) * 10)
                    .map(|x| x * 10)
                    .collect::<Int64Array>(),
            ),
            Arc::new(
                (i * 10..(i + 1) * 10)
                    .map(|x| x * 1000)
                    .collect::<TimestampArray>(),
            ),
        ];

        if with_time {
            columns.push(Arc::new(
                (i * 10..(i + 1) * 10)
                    .map(|x| x * 1000)
                    .collect::<TimestampArray>(),
            ));
        }

        DataSet::try_new(schema.clone(), columns).unwrap()
    }

    fn create_source_provider() -> Provider {
        let schema = Arc::new(
            Schema::try_new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
                Field::new("t", DataType::Timestamp(None)),
            ])
            .unwrap(),
        );

        let mut datasets = Vec::new();
        for i in 0..10 {
            datasets.push(create_dataset(schema.clone(), i, false));
        }

        Provider::new(schema, datasets)
    }

    #[tokio::test]
    async fn test_source_stream() {
        let provider = create_source_provider();
        let df = DataFrame::new(
            Arc::new(SourceProviderWrapper(provider)),
            None,
            Some(col("t")),
            None,
        );
        let mut stream = df.clone().into_stream(None).unwrap();

        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            create_dataset(stream.schema(), 0, true)
        );

        let state = stream.save_state().unwrap();

        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            create_dataset(stream.schema(), 1, true)
        );
    }
}

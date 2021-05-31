use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use futures_util::stream::{Stream, StreamExt};

use crate::dataset::DataSet;
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
}

impl DataStream {
    pub(crate) fn new(plan: LogicalPlan, state: Option<Vec<u8>>) -> Result<Self> {
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
            input: crate::execution::streams::create_stream(
                &mut create_ctx,
                PhysicalPlan::try_new(plan)?.root,
            )?,
        })
    }

    pub fn save_state(&self) -> Result<Vec<u8>> {
        let mut state = Default::default();
        self.input.save_state(&mut state)?;
        Ok(bincode::serialize(&state)?)
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

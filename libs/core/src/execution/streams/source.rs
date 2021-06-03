use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::array::TimestampArray;
use crate::dataset::{DataSet, SchemaRef};
use crate::execution::execution_context::ExecutionContext;
use crate::execution::stream::{BoxDataSetStream, CreateStreamContext, DataSetStream};
use crate::expr::physical_expr::PhysicalExpr;
use crate::expr::ExprState;
use crate::planner::physical_plan::PhysicalSourceNode;
use crate::source_provider::SourceDataSet;
use crate::GenericSourceDataSet;

pub fn create_source_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalSourceNode,
) -> Result<BoxDataSetStream> {
    let PhysicalSourceNode {
        id,
        schema,
        source_provider: provider,
        mut time_expr,
    } = node;

    let input = if let Some(data) = create_ctx.prev_state.remove(&id) {
        let saved_state: SavedState = bincode::deserialize(&data)?;
        let input = provider.create_stream(saved_state.source_state)?;
        if let (Some(expr), Some(data)) = (&mut time_expr, saved_state.time_expr) {
            expr.load_state(data)?;
        }
        input
    } else {
        provider.create_stream(None)?
    };

    Ok(Box::pin(SourceStream {
        id,
        ctx: create_ctx.ctx.clone(),
        schema,
        time_expr,
        input,
        current_state: None,
    }))
}

#[derive(Serialize, Deserialize)]
struct SavedState {
    source_state: Option<Vec<u8>>,
    time_expr: Option<ExprState>,
}

struct SourceStream {
    id: usize,
    ctx: Arc<ExecutionContext>,
    schema: SchemaRef,
    time_expr: Option<PhysicalExpr>,
    input: BoxStream<'static, Result<GenericSourceDataSet<Vec<u8>>>>,
    current_state: Option<Vec<u8>>,
}

impl SourceStream {
    fn process_dataset(&mut self, dataset: &DataSet) -> Result<DataSet> {
        let times_array = match &mut self.time_expr {
            Some(expr) => expr.eval(dataset)?,
            None => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                Arc::new(TimestampArray::new_scalar(dataset.len(), Some(now)))
            }
        };

        DataSet::try_new(
            self.schema.clone(),
            dataset
                .columns()
                .iter()
                .cloned()
                .chain(std::iter::once(times_array))
                .collect(),
        )
    }
}

impl DataSetStream for SourceStream {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()> {
        let time_expr_state = match &self.time_expr {
            Some(expr) => Some(expr.save_state()?),
            None => None,
        };
        let data = bincode::serialize(&SavedState {
            source_state: self.current_state.clone(),
            time_expr: time_expr_state,
        })?;
        state.insert(self.id, data);
        Ok(())
    }
}

impl Stream for SourceStream {
    type Item = Result<DataSet>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => {
                let SourceDataSet { state, dataset } = res?;
                self.ctx
                    .update_metrics(|metrics| metrics.num_input_rows += dataset.len());
                self.current_state = Some(state);
                let new_dataset = self.process_dataset(&dataset)?;
                Poll::Ready(Some(Ok(new_dataset)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

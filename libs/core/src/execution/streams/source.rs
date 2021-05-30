use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream;

use crate::array::{ArrayExt, BooleanBuilder, TimestampArray};
use crate::dataset::{DataSet, SchemaRef};
use crate::execution::stream::{
    BoxDataSetStream, CreateStreamContext, DataSetStream, DataSetWithWatermark,
};
use crate::expr::physical_expr::PhysicalExpr;
use crate::expr::ExprState;
use crate::planner::physical_plan::PhysicalSourceNode;
use crate::source_provider::SourceDataSet;
use crate::{ExecutionContext, GenericSourceDataSet};

pub fn create_source_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalSourceNode,
) -> Result<BoxDataSetStream> {
    let PhysicalSourceNode {
        id,
        schema,
        source_provider: provider,
        mut time_expr,
        mut watermark_expr,
    } = node;

    let (input, current_watermark) = if let Some(data) = create_ctx.prev_state.remove(&id) {
        let saved_state: SavedState = bincode::deserialize(&data)?;
        let input = provider.create_stream(saved_state.source_state)?;
        if let (Some(expr), Some(data)) = (&mut time_expr, saved_state.time_expr) {
            expr.load_state(data)?;
        }
        if let (Some(expr), Some(data)) = (&mut watermark_expr, saved_state.watermark_expr) {
            expr.load_state(data)?;
        }
        let current_watermark = saved_state.current_watermark;
        (input, current_watermark)
    } else {
        (provider.create_stream(None)?, None)
    };

    Ok(Box::pin(SourceStream {
        id,
        ctx: create_ctx.ctx.clone(),
        schema,
        time_expr,
        watermark_expr,
        input,
        current_state: None,
        current_watermark,
    }))
}

#[derive(Serialize, Deserialize)]
struct SavedState {
    current_watermark: Option<i64>,
    source_state: Option<Vec<u8>>,
    time_expr: Option<ExprState>,
    watermark_expr: Option<ExprState>,
}

struct SourceStream {
    id: usize,
    ctx: Arc<ExecutionContext>,
    schema: SchemaRef,
    time_expr: Option<PhysicalExpr>,
    watermark_expr: Option<PhysicalExpr>,
    input: BoxStream<'static, Result<GenericSourceDataSet<Vec<u8>>>>,
    current_state: Option<Vec<u8>>,
    current_watermark: Option<i64>,
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
        let watermarks_array = match &mut self.watermark_expr {
            Some(expr) => expr.eval(dataset)?,
            None => times_array.clone(),
        };

        let times = times_array.downcast_ref::<TimestampArray>();
        let watermarks = watermarks_array.downcast_ref::<TimestampArray>();
        let mut flags = BooleanBuilder::default();

        for (time, watermark) in times.iter_opt().zip(watermarks.iter_opt()) {
            if let Some(time) = time {
                let watermark = watermark.unwrap_or(time);

                // update watermark
                let current_watermark = match &mut self.current_watermark {
                    Some(current_watermark) => {
                        if watermark > *current_watermark {
                            *current_watermark = watermark;
                            watermark
                        } else {
                            *current_watermark
                        }
                    }
                    None => {
                        self.current_watermark = Some(watermark);
                        watermark
                    }
                };

                flags.append(time >= current_watermark);
            } else {
                flags.append(false);
            }
        }

        let new_dataset = DataSet::try_new(
            self.schema.clone(),
            dataset
                .columns()
                .iter()
                .cloned()
                .chain(std::iter::once(times_array))
                .collect(),
        )?;
        new_dataset.filter(&flags.finish())
    }
}

impl DataSetStream for SourceStream {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()> {
        let time_expr_state = match &self.time_expr {
            Some(expr) => Some(expr.save_state()?),
            None => None,
        };
        let watermark_expr_state = match &self.watermark_expr {
            Some(expr) => Some(expr.save_state()?),
            None => None,
        };

        let data = bincode::serialize(&SavedState {
            current_watermark: self.current_watermark,
            source_state: self.current_state.clone(),
            time_expr: time_expr_state,
            watermark_expr: watermark_expr_state,
        })?;
        state.insert(self.id, data);
        Ok(())
    }
}

impl Stream for SourceStream {
    type Item = Result<DataSetWithWatermark>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(res)) => {
                let SourceDataSet { state, dataset } = res?;
                self.ctx
                    .update_metrics(|metrics| metrics.num_input_rows += dataset.len());
                self.current_state = Some(state);
                let new_dataset = self.process_dataset(&dataset)?;
                Poll::Ready(Some(Ok(DataSetWithWatermark {
                    watermark: self.current_watermark,
                    dataset: new_dataset,
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

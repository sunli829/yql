use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use crate::array::{ArrayExt, BooleanBuilder, TimestampArray};
use crate::dataset::{DataSet, SchemaRef};
use crate::execution::checkpoint::CheckPointBarrier;
use crate::execution::stream::{CreateStreamContext, Event, EventStream};
use crate::expr::physical_expr::PhysicalExpr;
use crate::expr::ExprState;
use crate::planner::physical_plan::PhysicalSourceNode;
use crate::source_provider::SourceDataSet;

enum Message {
    CheckPointBarrier(Result<Arc<CheckPointBarrier>, BroadcastStreamRecvError>),
    DataSet(Result<SourceDataSet>),
}

#[derive(Serialize, Deserialize)]
struct SavedState {
    current_watermark: Option<i64>,
    source_state: Vec<u8>,
    time_expr: Option<ExprState>,
    watermark_expr: Option<ExprState>,
}

struct CombinedStream {
    rx_barrier: BroadcastStream<Arc<CheckPointBarrier>>,
    input: BoxStream<'static, Result<SourceDataSet>>,
}

impl Stream for CombinedStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx_barrier).poll_next(cx) {
            Poll::Ready(Some(item)) => return Poll::Ready(Some(Message::CheckPointBarrier(item))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }

        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(Message::DataSet(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn create_source_stream(
    ctx: &mut CreateStreamContext,
    node: PhysicalSourceNode,
) -> Result<EventStream> {
    let PhysicalSourceNode {
        id,
        schema,
        source_provider: provider,
        mut time_expr,
        mut watermark_expr,
    } = node;

    let (input, mut current_watermark) = if let Some(data) = ctx.prev_state.remove(&node.id) {
        let saved_state: SavedState = bincode::deserialize(&data)?;
        let input = provider.create_stream(Some(saved_state.source_state))?;
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

    let rx_barrier = ctx.tx_barrier.subscribe();
    let mut input = CombinedStream {
        rx_barrier: BroadcastStream::new(rx_barrier),
        input,
    };
    let exec_ctx = ctx.ctx.clone();

    Ok(Box::pin(async_stream::try_stream! {
        let mut current_state = None;
        while let Some(message) = input.next().await {
            match message {
                Message::CheckPointBarrier(res) => {
                    if let (Ok(barrier), Some(current_state)) = (res, current_state.clone()) {
                        let _ = barrier.source_barrier().wait().await;
                        let time_expr_state = match &time_expr {
                            Some(expr) => Some(expr.save_state()?),
                            None => None,
                        };
                        let watermark_expr_state = match &watermark_expr {
                            Some(expr) => Some(expr.save_state()?),
                            None => None,
                        };
                        let saved_data = bincode::serialize(&SavedState {
                            current_watermark,
                            source_state: current_state,
                            time_expr: time_expr_state,
                            watermark_expr: watermark_expr_state,
                        })?;
                        barrier.set_state(id, Some(saved_data));
                        yield Event::CreateCheckPoint(barrier);
                    }
                }
                Message::DataSet(item) => {
                    let SourceDataSet { state, dataset } = item?;
                    exec_ctx.update_metrics(|metrics| metrics.num_input_rows += dataset.len());
                    current_state = Some(state);
                    let new_dataset = process_dataset(
                        schema.clone(),
                        &dataset,
                        time_expr.as_mut(),
                        watermark_expr.as_mut(),
                        &mut current_watermark,
                    )?;
                    yield Event::DataSet {
                        current_watermark,
                        dataset: new_dataset,
                    };
                },
            }
        }
    }))
}

fn process_dataset(
    schema: SchemaRef,
    dataset: &DataSet,
    time_expr: Option<&mut PhysicalExpr>,
    watermark_expr: Option<&mut PhysicalExpr>,
    current_watermark: &mut Option<i64>,
) -> Result<DataSet> {
    let times_array = match time_expr {
        Some(expr) => expr.eval(dataset)?,
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            Arc::new(TimestampArray::new_scalar(dataset.len(), Some(now)))
        }
    };
    let watermarks_array = match watermark_expr {
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
            let current_watermark = match current_watermark {
                Some(current_watermark) => {
                    if watermark > *current_watermark {
                        *current_watermark = watermark;
                        watermark
                    } else {
                        *current_watermark
                    }
                }
                None => {
                    *current_watermark = Some(watermark);
                    watermark
                }
            };

            flags.append(time >= current_watermark);
        } else {
            flags.append(false);
        }
    }

    let new_dataset = DataSet::try_new(
        schema,
        dataset
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(times_array))
            .collect(),
    )?;
    new_dataset.filter(&flags.finish())
}

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use futures_util::stream::{BoxStream, StreamExt};
use futures_util::Stream;
use tokio::sync::broadcast;
use tokio::time::Interval;

use crate::dataset::DataSet;
use crate::execution::checkpoint::CheckPointBarrier;
use crate::execution::execution_context::ExecutionContext;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::PhysicalPlan;

pub enum Event {
    DataSet {
        current_watermark: Option<i64>,
        dataset: DataSet,
    },
    CreateCheckPoint(Arc<CheckPointBarrier>),
}

impl Debug for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::DataSet {
                current_watermark,
                dataset,
            } => f
                .debug_struct("DataSet")
                .field("current_watermark", current_watermark)
                .field("dataset", dataset)
                .finish(),
            Event::CreateCheckPoint(_) => f.debug_struct("CreateCheckPoint").finish(),
        }
    }
}

pub type EventStream = BoxStream<'static, Result<Event>>;

pub struct CreateStreamContext {
    pub ctx: Arc<ExecutionContext>,
    pub tx_barrier: broadcast::Sender<Arc<CheckPointBarrier>>,
    pub prev_state: HashMap<usize, Vec<u8>>,
}

enum Message {
    CreateCheckPoint,
    Event(Result<Event>),
}

struct CombinedStream {
    interval: Pin<Box<Interval>>,
    input: EventStream,
}

impl Stream for CombinedStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.interval.poll_tick(cx) {
            Poll::Ready(_) => return Poll::Ready(Some(Message::CreateCheckPoint)),
            Poll::Pending => {}
        }

        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Message::Event(event))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn create_data_stream(
    ctx: ExecutionContext,
    plan: LogicalPlan,
    signal: Option<impl Future<Output = ()> + Send + 'static>,
) -> BoxStream<'static, Result<DataSet>> {
    Box::pin(async_stream::try_stream! {
         let prev_state: HashMap<usize, Vec<u8>> = match &ctx.storage {
            Some(storage) => {
                match storage.load_state().await? {
                    Some(data) => bincode::deserialize(&data).context("failed to deserialize stream state.")?,
                    None => Default::default(),
                }
            }
            None => Default::default(),
        };

        let ctx = Arc::new(ctx);
        let plan = PhysicalPlan::try_new(plan)?;
        let node_count = plan.node_count;
        let source_count = plan.source_count;
        let (tx_barrier, _) = broadcast::channel(8);
        let mut create_ctx = CreateStreamContext {
            ctx: ctx.clone(),
            tx_barrier: tx_barrier.clone(),
            prev_state,
        };
        let event_stream = crate::execution::streams::create_stream(&mut create_ctx, plan.root)?;
        let checkpoint_interval = tokio::time::interval(ctx.checkpoint_interval);

        if let Some(signal) = signal {
            tokio::spawn({
                let tx_barrier = tx_barrier.clone();
                async move {
                    signal.await;
                    let barrier = Arc::new(CheckPointBarrier::new(node_count, source_count, true));
                    let _ = tx_barrier.send(barrier);
                }
            });
        }

        let mut input = CombinedStream {
            interval: Box::pin(checkpoint_interval),
            input: event_stream,
        };

        while let Some(message) = input.next().await {
            match message {
                Message::CreateCheckPoint => {
                    let barrier = Arc::new(CheckPointBarrier::new(
                        node_count,
                        source_count,
                        false,
                    ));
                    let _ = tx_barrier.send(barrier.clone());
                    let ctx = ctx.clone();
                    tokio::spawn(save_state(ctx, barrier));
                }
                Message::Event(res) => {
                    let event = res?;
                    if let Event::DataSet { dataset, .. } = event {
                        if !dataset.is_empty() {
                            yield dataset;
                        }
                    }
                }
            }
        }
    })
}

async fn save_state(ctx: Arc<ExecutionContext>, barrier: Arc<CheckPointBarrier>) {
    tracing::info!(name = %ctx.name, "create checkpoint");
    barrier.wait().await;

    let data = match bincode::serialize(&barrier.take_state()) {
        Ok(data) => data,
        Err(err) => {
            tracing::error!(
                name = %ctx.name,
                error = %err,
                "failed to serialize stream state"
            );
            return;
        }
    };

    if let Some(storage) = &ctx.storage {
        match storage.save_state(data).await {
            Ok(()) => tracing::info!(name = %ctx.name, "checkpoint created"),
            Err(err) => {
                tracing::info!(name = %ctx.name, error = %err, "failed to save checkpoint")
            }
        }
    }
}

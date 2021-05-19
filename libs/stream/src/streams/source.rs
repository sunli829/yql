use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use yql_planner::physical_plan::PhysicalSourceNode;
use yql_planner::SourceDataSet;

use crate::checkpoint::CheckPointBarrier;
use crate::stream::{CreateStreamContext, Event, EventStream};

enum Control {
    CheckPointBarrier(Result<Arc<CheckPointBarrier>, BroadcastStreamRecvError>),
    DataSet(Result<SourceDataSet>),
}

pub fn create_source_stream(
    ctx: &mut CreateStreamContext,
    node: PhysicalSourceNode,
) -> Result<EventStream> {
    let input = node
        .provider
        .create_stream(ctx.prev_state.remove(&node.id))?;
    let rx_barrier = ctx.tx_barrier.subscribe();
    let id = node.id;

    let mut input = futures_util::stream::select(
        tokio_stream::wrappers::BroadcastStream::new(rx_barrier).map(Control::CheckPointBarrier),
        input.map(Control::DataSet),
    );

    Ok(Box::pin(async_stream::try_stream! {
        let mut current_state = None;
        while let Some(control) = input.next().await {
            match control {
                Control::CheckPointBarrier(res) => {
                    if let Ok(barrier) = res {
                        let _ = barrier.source_barrier().wait().await;
                        barrier.set_state(id, current_state.clone());
                        yield Event::CreateCheckPoint(barrier);
                    }
                }
                Control::DataSet(item) => {
                    let SourceDataSet { state, dataset } = item?;
                    current_state = Some(state);
                    yield Event::DataSet(dataset);
                },
            }
        }
    }))
}

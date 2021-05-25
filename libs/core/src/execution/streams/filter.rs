use anyhow::Result;
use tokio_stream::StreamExt;
use yql_dataset::array::{ArrayExt, BooleanArray};

use crate::execution::stream::{CreateStreamContext, Event, EventStream};
use crate::execution::streams::create_stream;
use crate::planner::physical_plan::PhysicalFilterNode;

pub fn create_filter_stream(
    ctx: &mut CreateStreamContext,
    node: PhysicalFilterNode,
) -> Result<EventStream> {
    let mut input = create_stream(ctx, *node.input)?;
    let id = node.id;
    let mut expr = node.expr;

    if let Some(data) = ctx.prev_state.remove(&id) {
        expr.load_state(data)?;
    }

    Ok(Box::pin(async_stream::try_stream! {
        while let Some(event) = input.next().await.transpose()? {
            match event {
                Event::DataSet{ current_watermark, dataset } => {
                    let array = expr.eval(&dataset)?;
                    let result_dataset = dataset.filter(array.downcast_ref::<BooleanArray>())?;
                    if !result_dataset.is_empty() {
                        yield Event::DataSet { current_watermark, dataset: result_dataset };
                    }
                }
                Event::CreateCheckPoint(barrier) => {
                    if !barrier.is_saved(id) {
                        barrier.set_state(id, Some(expr.save_state()?));
                    }
                    yield Event::CreateCheckPoint(barrier.clone());
                    if barrier.is_exit() {
                        break;
                    }
                }
            }
        }
    }))
}

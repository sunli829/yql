use anyhow::Result;
use tokio_stream::StreamExt;
use yql_array::{ArrayExt, BooleanArray};
use yql_planner::physical_plan::PhysicalFilterNode;

use crate::stream::{CreateStreamContext, Event, EventStream};
use crate::streams::create_stream;

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
                Event::DataSet(dataset) => {
                    let array = expr.eval(&dataset)?;
                    let result_dataset = dataset.filter(array.downcast_ref::<BooleanArray>())?;
                    if !result_dataset.is_empty() {
                        yield Event::DataSet(result_dataset);
                    }
                }
                Event::CreateCheckPoint(barrier) => {
                    if !barrier.is_saved(id) {
                        barrier.set_state(id, Some(expr.save_state()?));
                    }
                    if barrier.is_exit() {
                        break;
                    }
                }
            }
        }
    }))
}

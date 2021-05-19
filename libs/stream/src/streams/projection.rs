use anyhow::Result;
use itertools::Itertools;
use tokio_stream::StreamExt;
use yql_dataset::DataSet;
use yql_planner::physical_plan::PhysicalProjectionNode;

use crate::stream::{CreateStreamContext, Event, EventStream};
use crate::streams::create_stream;

pub fn create_projection_stream(
    ctx: &mut CreateStreamContext,
    node: PhysicalProjectionNode,
) -> Result<EventStream> {
    let mut input = create_stream(ctx, *node.input)?;
    let id = node.id;
    let schema = node.schema;
    let mut exprs = node.exprs;

    if let Some(data) = ctx.prev_state.remove(&id) {
        let state: Vec<Vec<u8>> = bincode::deserialize(&data)?;
        for (expr, state_data) in exprs.iter_mut().zip(state) {
            expr.load_state(state_data)?;
        }
    }

    Ok(Box::pin(async_stream::try_stream! {
        while let Some(event) = input.next().await.transpose()? {
            match event {
                Event::DataSet(dataset) => {
                    let mut columns = Vec::with_capacity(exprs.len());
                    for expr in &mut exprs {
                        columns.push(expr.eval(&dataset)?);
                    }
                    let result_dataset = DataSet::try_new(schema.clone(), columns)?;
                    yield Event::DataSet(result_dataset);
                }
                Event::CreateCheckPoint(barrier) => {
                    if !barrier.is_saved(id) {
                        let state = exprs.iter().map(|expr| expr.save_state()).try_collect::<_, Vec<_>, _>()?;
                        let state_data = bincode::serialize(&state)?;
                        barrier.set_state(id, Some(state_data));
                    }
                    if barrier.is_exit() {
                        break;
                    }
                }
            }
        }
    }))
}

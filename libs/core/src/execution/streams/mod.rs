// mod aggregate;
// mod filter;
// mod projection;
mod source;

use anyhow::Result;

use crate::execution::stream::{BoxDataSetStream, CreateStreamContext};
use crate::planner::physical_plan::PhysicalNode;

pub fn create_stream(
    create_ctx: &mut CreateStreamContext,
    node: PhysicalNode,
) -> Result<BoxDataSetStream> {
    match node {
        PhysicalNode::Source(source) => source::create_source_stream(create_ctx, source),
        PhysicalNode::Projection(projection) => {
            todo!()
            //projection::create_projection_stream(ctx, projection)
        }
        PhysicalNode::Filter(filter) => {
            todo!()
            //filter::create_filter_stream(ctx, filter)
        }
        PhysicalNode::Aggregate(aggregate) => {
            todo!()
            // aggregate::create_aggregate_stream(ctx, aggregate)
        }
    }
}

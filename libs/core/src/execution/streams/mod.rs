mod aggregate;
mod filter;
mod projection;
mod source;

use anyhow::Result;

use crate::execution::stream::{CreateStreamContext, EventStream};
use crate::planner::physical_plan::PhysicalNode;

pub fn create_stream(ctx: &mut CreateStreamContext, node: PhysicalNode) -> Result<EventStream> {
    match node {
        PhysicalNode::Source(source) => source::create_source_stream(ctx, source),
        PhysicalNode::Projection(projection) => {
            projection::create_projection_stream(ctx, projection)
        }
        PhysicalNode::Filter(filter) => filter::create_filter_stream(ctx, filter),
        PhysicalNode::Aggregate(aggregate) => aggregate::create_aggregate_stream(ctx, aggregate),
    }
}

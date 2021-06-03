mod aggregate;
mod filter;
mod projection;
mod repartition;
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
        PhysicalNode::Repartition(repartition) => {
            repartition::create_repartition_stream(create_ctx, repartition)
        }
        PhysicalNode::Projection(projection) => {
            projection::create_projection_stream(create_ctx, projection)
        }
        PhysicalNode::Filter(filter) => filter::create_filter_stream(create_ctx, filter),
        PhysicalNode::Aggregate(aggregate) => {
            aggregate::create_aggregate_stream(create_ctx, aggregate)
        }
    }
}

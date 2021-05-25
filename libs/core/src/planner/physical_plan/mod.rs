mod aggregate;
mod filter;
mod projection;
mod source;
mod to_physical;

use yql_dataset::dataset::SchemaRef;

pub use aggregate::PhysicalAggregateNode;
pub use filter::PhysicalFilterNode;
pub use projection::PhysicalProjectionNode;
pub use source::PhysicalSourceNode;

pub const FIELD_TIME: &str = "@time";

pub enum PhysicalNode {
    Source(PhysicalSourceNode),
    Projection(PhysicalProjectionNode),
    Filter(PhysicalFilterNode),
    Aggregate(PhysicalAggregateNode),
}

impl PhysicalNode {
    pub fn schema(&self) -> SchemaRef {
        match self {
            PhysicalNode::Source(source) => source.schema.clone(),
            PhysicalNode::Projection(projection) => projection.schema.clone(),
            PhysicalNode::Filter(filter) => filter.schema.clone(),
            PhysicalNode::Aggregate(aggregate) => aggregate.schema.clone(),
        }
    }
}

pub struct PhysicalPlan {
    pub root: PhysicalNode,
    pub source_count: usize,
    pub node_count: usize,
}

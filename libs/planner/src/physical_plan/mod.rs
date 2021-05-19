mod aggregate;
mod filter;
mod projection;
mod source;
mod to_physical;

use yql_dataset::SchemaRef;

pub use aggregate::PhysicalAggregateNode;
pub use filter::PhysicalFilterNode;
pub use projection::PhysicalProjectionNode;
pub use source::PhysicalSourceNode;

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
    pub(crate) root: PhysicalNode,
    pub(crate) source_count: usize,
    pub(crate) node_count: usize,
}

impl PhysicalPlan {
    #[inline]
    fn root(&self) -> &PhysicalNode {
        &self.root
    }

    #[inline]
    fn source_count(&self) -> usize {
        self.source_count
    }

    #[inline]
    fn node_count(&self) -> usize {
        self.node_count
    }
}

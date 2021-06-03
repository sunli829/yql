mod aggregate;
mod filter;
mod projection;
mod repartition;
mod source;

pub use aggregate::LogicalAggregatePlan;
pub use filter::LogicalFilterPlan;
pub use projection::LogicalProjectionPlan;
pub use repartition::{Partitioning, RepartitionPlan};
pub use source::LogicalSourcePlan;

#[derive(Clone)]
pub enum LogicalPlan {
    Source(LogicalSourcePlan),
    Repartition(RepartitionPlan),
    Projection(LogicalProjectionPlan),
    Filter(LogicalFilterPlan),
    Aggregate(LogicalAggregatePlan),
}

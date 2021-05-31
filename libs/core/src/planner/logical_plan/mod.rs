mod aggregate;
mod filter;
mod projection;
mod source;

pub use aggregate::LogicalAggregatePlan;
pub use filter::LogicalFilterPlan;
pub use projection::LogicalProjectionPlan;
pub use source::LogicalSourcePlan;

#[derive(Clone)]
pub enum LogicalPlan {
    Source(LogicalSourcePlan),
    Projection(LogicalProjectionPlan),
    Filter(LogicalFilterPlan),
    Aggregate(LogicalAggregatePlan),
}

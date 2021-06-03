use crate::expr::Expr;
use crate::planner::logical_plan::LogicalPlan;

#[derive(Clone)]
pub enum Partitioning {
    RoundRobin(usize),
    Hash(Vec<Expr>, usize),
    Group(Vec<Expr>),
}

#[derive(Clone)]
pub struct RepartitionPlan {
    pub input: Box<LogicalPlan>,
    pub partitioning: Partitioning,
}

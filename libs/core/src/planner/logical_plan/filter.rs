use crate::expr::Expr;
use crate::planner::logical_plan::LogicalPlan;

#[derive(Clone)]
pub struct LogicalFilterPlan {
    pub input: Box<LogicalPlan>,
    pub expr: Expr,
}

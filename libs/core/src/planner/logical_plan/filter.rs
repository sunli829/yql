use crate::expr::Expr;
use crate::planner::logical_plan::LogicalPlan;

pub struct LogicalFilterPlan {
    pub input: Box<LogicalPlan>,
    pub expr: Expr,
}

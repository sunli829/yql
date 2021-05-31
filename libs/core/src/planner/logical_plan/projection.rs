use crate::expr::Expr;
use crate::planner::logical_plan::LogicalPlan;

#[derive(Clone)]
pub struct LogicalProjectionPlan {
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<Expr>,
}

use crate::expr::Expr;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::window::Window;

#[derive(Clone)]
pub struct LogicalAggregatePlan {
    pub input: Box<LogicalPlan>,
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<Expr>,
    pub window: Window,
}

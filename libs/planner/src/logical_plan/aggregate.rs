use yql_expr::Expr;

use crate::logical_plan::LogicalPlan;
use crate::window::Window;

pub struct LogicalAggregatePlan {
    pub input: Box<LogicalPlan>,
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<Expr>,
    pub window: Window,
    pub time_expr: Option<Expr>,
    pub watermark_expr: Option<Expr>,
}

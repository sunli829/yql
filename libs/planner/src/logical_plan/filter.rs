use yql_expr::Expr;

use crate::logical_plan::LogicalPlan;

pub struct LogicalFilterPlan {
    pub input: Box<LogicalPlan>,
    pub expr: Expr,
}

use yql_expr::Expr;

use crate::logical_plan::LogicalPlan;

pub struct LogicalProjectionPlan {
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<Expr>,
}

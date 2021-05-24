use crate::expr::Expr;
use crate::source_provider::SourceProvider;

pub struct LogicalSourcePlan {
    pub name: String,
    pub qualifier: Option<String>,
    pub provider: SourceProvider,
    pub time_expr: Option<Expr>,
    pub watermark_expr: Option<Expr>,
}

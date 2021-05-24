use crate::expr::Expr;
use crate::source_provider::SourceProvider;

pub struct LogicalSourcePlan {
    pub qualifier: Option<String>,
    pub source_provider: SourceProvider,
    pub time_expr: Option<Expr>,
    pub watermark_expr: Option<Expr>,
}

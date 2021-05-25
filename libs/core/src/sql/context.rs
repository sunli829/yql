use anyhow::Result;

use crate::expr::Expr;
use crate::SourceProvider;

pub struct SqlSourceProvider {
    pub source_provider: SourceProvider,
    pub time_expr: Option<Expr>,
    pub watermark_expr: Option<Expr>,
}

pub trait SqlContext {
    fn create_source_provider(&self, name: &str) -> Result<Option<SqlSourceProvider>>;
}

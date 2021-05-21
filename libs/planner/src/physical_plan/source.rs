use yql_dataset::SchemaRef;
use yql_expr::PhysicalExpr;

use crate::source_provider::SourceProvider;

pub struct PhysicalSourceNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub provider: SourceProvider,
    pub time_expr: Option<PhysicalExpr>,
    pub watermark_expr: Option<PhysicalExpr>,
}

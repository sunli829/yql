use crate::dataset::SchemaRef;
use crate::expr::physical_expr::PhysicalExpr;
use crate::source_provider::SourceProvider;

pub struct PhysicalSourceNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub provider: SourceProvider,
    pub time_expr: Option<PhysicalExpr>,
    pub watermark_expr: Option<PhysicalExpr>,
}

use crate::dataset::SchemaRef;
use crate::expr::physical_expr::PhysicalExpr;
use crate::source_provider::SourceProvider;

#[derive(Clone)]
pub struct PhysicalSourceNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub source_provider: SourceProvider,
    pub time_expr: Option<PhysicalExpr>,
}

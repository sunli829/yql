use yql_dataset::SchemaRef;
use yql_expr::PhysicalExpr;

use crate::physical_plan::PhysicalNode;

pub struct PhysicalProjectionNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub exprs: Vec<PhysicalExpr>,
    pub input: Box<PhysicalNode>,
}

use yql_dataset::SchemaRef;
use yql_expr::PhysicalExpr;

use crate::physical_plan::PhysicalNode;

pub struct PhysicalFilterNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub expr: PhysicalExpr,
    pub input: Box<PhysicalNode>,
}

use yql_dataset::dataset::SchemaRef;

use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalNode;

pub struct PhysicalProjectionNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub exprs: Vec<PhysicalExpr>,
    pub input: Box<PhysicalNode>,
}

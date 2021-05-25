use yql_dataset::dataset::SchemaRef;

use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalNode;

pub struct PhysicalFilterNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub expr: PhysicalExpr,
    pub input: Box<PhysicalNode>,
}

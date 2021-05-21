use yql_dataset::SchemaRef;
use yql_expr::PhysicalExpr;

use crate::physical_plan::PhysicalNode;
use crate::window::Window;

pub struct PhysicalAggregateNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub group_exprs: Vec<PhysicalExpr>,
    pub aggr_exprs: Vec<PhysicalExpr>,
    pub window: Window,
    pub time_idx: usize,
    pub input: Box<PhysicalNode>,
}

use crate::dataset::SchemaRef;
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalNode;
use crate::planner::window::Window;

#[derive(Clone)]
pub struct PhysicalAggregateNode {
    pub id: usize,
    pub schema: SchemaRef,
    pub group_exprs: Vec<PhysicalExpr>,
    pub aggr_exprs: Vec<PhysicalExpr>,
    pub window: Window,
    pub time_idx: usize,
    pub input: Box<PhysicalNode>,
}

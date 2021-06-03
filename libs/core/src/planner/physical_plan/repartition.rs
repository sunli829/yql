use crate::dataset::SchemaRef;
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::physical_plan::PhysicalNode;

#[derive(Clone)]
pub enum PhysicalPartitioning {
    RoundRobin(usize),
    Hash(Vec<PhysicalExpr>, usize),
    Group(Vec<PhysicalExpr>),
}

#[derive(Clone)]
pub struct PhysicalRepartitionNode {
    pub input: Box<PhysicalNode>,
    pub schema: SchemaRef,
    pub partitioning: PhysicalPartitioning,
}

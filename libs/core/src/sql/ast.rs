use serde::{Deserialize, Serialize};

use crate::expr::Expr;
use crate::Window;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum SourceFrom {
    Named(String),
    SubQuery(Box<Select>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub from: SourceFrom,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GroupBy {
    pub exprs: Vec<Expr>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub projection: Vec<Expr>,
    pub source: Source,
    pub where_clause: Option<Expr>,
    pub having_clause: Option<Expr>,
    pub group_clause: Option<GroupBy>,
    pub window: Option<Window>,
}

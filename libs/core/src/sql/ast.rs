use crate::expr::Expr;
use crate::Window;

#[derive(Debug, PartialEq)]
pub enum SourceFrom {
    Named(String),
    SubQuery(Box<Select>),
}

#[derive(Debug, PartialEq)]
pub struct Source {
    pub from: SourceFrom,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct GroupBy {
    pub exprs: Vec<Expr>,
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub projection: Vec<Expr>,
    pub source: Source,
    pub where_clause: Option<Expr>,
    pub having_clause: Option<Expr>,
    pub group_clause: Option<GroupBy>,
    pub window: Option<Window>,
}

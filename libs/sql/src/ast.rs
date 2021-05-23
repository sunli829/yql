use yql_dataset::Field;
use yql_expr::Expr;
use yql_planner::Window;

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

#[derive(Debug, PartialEq)]
pub struct StmtCreateStream {
    pub name: String,
    pub select: Select,
    pub to: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtCreateSource {
    pub name: String,
    pub uri: String,
    pub fields: Vec<Field>,
    pub time: Option<Expr>,
    pub watermark: Option<Expr>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OutputFormat {
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Json
    }
}

#[derive(Debug, PartialEq)]
pub struct StmtCreateSink {
    pub name: String,
    pub uri: String,
    pub format: OutputFormat,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteSource {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteStream {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct StmtDeleteSink {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub enum Stmt {
    CreateSource(StmtCreateSource),
    CreateStream(StmtCreateStream),
    CreateSink(StmtCreateSink),
    DeleteSource(StmtDeleteSource),
    DeleteStream(StmtDeleteStream),
    DeleteSink(StmtDeleteSink),
}

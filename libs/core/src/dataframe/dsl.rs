use crate::expr::{Expr, Literal};

pub fn value(value: impl Into<Literal>) -> Expr {
    Expr::Literal(value.into())
}

pub fn col(name: impl Into<String>) -> Expr {
    Expr::Column {
        qualifier: None,
        name: name.into(),
    }
}

pub fn qualified_col(table: impl Into<String>, name: impl Into<String>) -> Expr {
    Expr::Column {
        qualifier: Some(table.into()),
        name: name.into(),
    }
}

pub fn wildcard() -> Expr {
    Expr::Wildcard { qualifier: None }
}

pub fn wildcard_with_table(table: impl Into<String>) -> Expr {
    Expr::Wildcard {
        qualifier: Some(table.into()),
    }
}

pub fn call(name: impl Into<String>, args: Vec<Expr>) -> Expr {
    Expr::Call {
        namespace: None,
        name: name.into(),
        args,
    }
}

pub fn call_with_namespace(
    namespace: impl Into<String>,
    name: impl Into<String>,
    args: Vec<Expr>,
) -> Expr {
    Expr::Call {
        namespace: Some(namespace.into()),
        name: name.into(),
        args,
    }
}

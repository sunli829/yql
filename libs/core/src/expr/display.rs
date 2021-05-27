use std::fmt::{self, Display, Formatter, Write};

use crate::expr::Expr;

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Literal(value) => write!(f, "{}", value),
            Expr::Column { qualifier, name } => match qualifier {
                Some(qualifier) => write!(f, "{}.{}", qualifier, name),
                None => write!(f, "{}", name),
            },
            Expr::Binary { op, lhs, rhs } => write!(f, "({} {} {})", lhs, op, rhs),
            Expr::Unary { op, expr } => write!(f, "{} {}", op, expr),
            Expr::Call {
                namespace,
                name,
                args: arguments,
            } => {
                match namespace {
                    Some(namespace) => write!(f, "{}.{}", namespace, name)?,
                    None => write!(f, "{}", name)?,
                }
                f.write_char('(')?;
                for (idx, argument) in arguments.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{}", argument)?;
                }
                f.write_char(')')
            }
            Expr::Wildcard { .. } => unreachable!(),
            Expr::Alias(expr, name) => {
                write!(f, "{} as {}", expr, name)
            }
        }
    }
}

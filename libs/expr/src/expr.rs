use std::ops::{Add, Div, Mul, Neg, Not, Sub};

use crate::{BinaryOperator, Literal, UnaryOperator};

#[derive(Debug, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Column {
        qualifier: Option<String>,
        name: String,
    },
    Wildcard {
        qualifier: Option<String>,
    },
    Binary {
        op: BinaryOperator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Alias(Box<Expr>, String),
}

impl Expr {
    pub fn alias(self, alias: impl Into<String>) -> Expr {
        Expr::Alias(Box::new(self), alias.into())
    }

    pub fn eq(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::Eq,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn and(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::And,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn or(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::Or,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn not_eq(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::NotEq,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn lt(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::Lt,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn lt_eq(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::LtEq,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn gt(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::Gt,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn gt_eq(self, rhs: Expr) -> Expr {
        Expr::Binary {
            op: BinaryOperator::GtEq,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }

    pub fn create_name(&self) -> String {
        match self {
            Expr::Column { name, .. } => name.clone(),
            Expr::Alias(_, name) => name.clone(),
            _ => self.to_string(),
        }
    }
}

impl Add for Expr {
    type Output = Expr;

    fn add(self, rhs: Self) -> Self::Output {
        Expr::Binary {
            op: BinaryOperator::Plus,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }
}

impl Sub for Expr {
    type Output = Expr;

    fn sub(self, rhs: Self) -> Self::Output {
        Expr::Binary {
            op: BinaryOperator::Minus,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }
}

impl Mul for Expr {
    type Output = Expr;

    fn mul(self, rhs: Self) -> Self::Output {
        Expr::Binary {
            op: BinaryOperator::Multiply,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }
}

impl Div for Expr {
    type Output = Expr;

    fn div(self, rhs: Self) -> Self::Output {
        Expr::Binary {
            op: BinaryOperator::Divide,
            lhs: Box::new(self),
            rhs: Box::new(rhs),
        }
    }
}

impl Neg for Expr {
    type Output = Expr;

    fn neg(self) -> Self::Output {
        Expr::Unary {
            op: UnaryOperator::Neg,
            expr: Box::new(self),
        }
    }
}

impl Not for Expr {
    type Output = Expr;

    fn not(self) -> Self::Output {
        Expr::Unary {
            op: UnaryOperator::Not,
            expr: Box::new(self),
        }
    }
}

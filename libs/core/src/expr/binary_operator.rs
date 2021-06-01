use std::sync::Arc;

use anyhow::{Error, Result};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::array::{
    Array, ArrayExt, ArrayRef, BooleanArray, BooleanBuilder, BooleanType, DataType, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, PrimitiveArray, PrimitiveBuilder,
    PrimitiveType, StringArray,
};

macro_rules! binary_arithmetic_array {
    ($opcode:expr, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::Int8, DataType::Int8) => math_op::<Int8Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int16) => math_op::<Int8Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int32) => math_op::<Int8Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int64) => math_op::<Int8Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int8, DataType::Float32) => math_op::<Int8Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int8, DataType::Float64) => math_op::<Int8Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int16, DataType::Int8) => math_op::<Int16Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int16) => math_op::<Int16Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int32) => math_op::<Int16Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int64) => math_op::<Int16Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int16, DataType::Float32) => math_op::<Int16Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int16, DataType::Float64) => math_op::<Int16Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int32, DataType::Int8) => math_op::<Int32Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int16) => math_op::<Int32Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int32) => math_op::<Int32Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int64) => math_op::<Int32Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int32, DataType::Float32) => math_op::<Int32Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int32, DataType::Float64) => math_op::<Int32Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int64, DataType::Int8) => math_op::<Int64Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int16) => math_op::<Int64Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int32) => math_op::<Int64Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int64) => math_op::<Int64Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| a $op b),
            (DataType::Int64, DataType::Float32) => math_op::<Int64Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int64, DataType::Float64) => math_op::<Int64Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Float32, DataType::Int8) => math_op::<Float32Type, Int8Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int16) => math_op::<Float32Type, Int16Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int32) => math_op::<Float32Type, Int32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int64) => math_op::<Float32Type, Int64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Float32) => math_op::<Float32Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Float64) => math_op::<Float32Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Float64, DataType::Int8) => math_op::<Float64Type, Int8Type, Float64Type, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int16) => math_op::<Float64Type, Int16Type, Float64Type, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int32) => math_op::<Float64Type, Int32Type, Float64Type, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int64) => math_op::<Float64Type, Int64Type, Float64Type, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Float32) => math_op::<Float64Type, Float32Type, Float64Type, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Float64) => math_op::<Float64Type, Float64Type, Float64Type, _>($lhs, $rhs, |a, b| a $op b),

            _ => Err(binary_error($opcode, $lhs.data_type(), $rhs.data_type())),
        }
    };
}

macro_rules! binary_rem_array {
    ($opcode:expr, $lhs:expr, $rhs:expr) => {
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::Int8, DataType::Int8) => {
                math_op::<Int8Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int8, DataType::Int16) => {
                math_op::<Int8Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int8, DataType::Int32) => {
                math_op::<Int8Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int8, DataType::Int64) => {
                math_op::<Int8Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) % b)
            }

            (DataType::Int16, DataType::Int8) => {
                math_op::<Int16Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int16, DataType::Int16) => {
                math_op::<Int16Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int16, DataType::Int32) => {
                math_op::<Int16Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int16, DataType::Int64) => {
                math_op::<Int16Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) % b)
            }

            (DataType::Int32, DataType::Int8) => {
                math_op::<Int32Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int32, DataType::Int16) => {
                math_op::<Int32Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int32, DataType::Int32) => {
                math_op::<Int32Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| {
                    (a as i64) % (b as i64)
                })
            }
            (DataType::Int32, DataType::Int64) => {
                math_op::<Int32Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| (a as i64) % b)
            }

            (DataType::Int64, DataType::Int8) => {
                math_op::<Int64Type, Int8Type, Int64Type, _>($lhs, $rhs, |a, b| a % (b as i64))
            }
            (DataType::Int64, DataType::Int16) => {
                math_op::<Int64Type, Int16Type, Int64Type, _>($lhs, $rhs, |a, b| a % (b as i64))
            }
            (DataType::Int64, DataType::Int32) => {
                math_op::<Int64Type, Int32Type, Int64Type, _>($lhs, $rhs, |a, b| a % (b as i64))
            }
            (DataType::Int64, DataType::Int64) => {
                math_op::<Int64Type, Int64Type, Int64Type, _>($lhs, $rhs, |a, b| a % b)
            }

            _ => Err(binary_error($opcode, $lhs.data_type(), $rhs.data_type())),
        }
    };
}

macro_rules! binary_equal_array {
    ($opcode:expr, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => math_op::<BooleanType, BooleanType, BooleanType, _>($lhs, $rhs, |a, b| a $op b),
            (DataType::String, DataType::String) => {
                let a = $lhs.downcast_ref::<StringArray>();
                let b = $rhs.downcast_ref::<StringArray>();
                if let (Some(a_scalar), Some(b_scalar)) = (a.to_scalar(), b.to_scalar()) {
                    return match (a_scalar, b_scalar) {
                        (Some(a_scalar), Some(b_scalar)) => Ok(Arc::new(BooleanArray::new_scalar(a.len(), Some(a_scalar $op b_scalar)))),
                        _ => Ok(Arc::new(BooleanArray::new_scalar(a.len(), None))),
                    }
                }
                let mut builder = BooleanBuilder::with_capacity(a.len());
                for (a, b) in a.iter_opt().zip(b.iter_opt()) {
                    match (a, b) {
                        (Some(a), Some(b)) => builder.append(a $op b),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            },

            (DataType::Int8, DataType::Int8) => math_op::<Int8Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int16) => math_op::<Int8Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int32) => math_op::<Int8Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int64) => math_op::<Int8Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),

            (DataType::Int16, DataType::Int8) => math_op::<Int16Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int16) => math_op::<Int16Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int32) => math_op::<Int16Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int64) => math_op::<Int16Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),

            (DataType::Int32, DataType::Int8) => math_op::<Int32Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int16) => math_op::<Int32Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int32) => math_op::<Int32Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int64) => math_op::<Int32Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),

            (DataType::Int64, DataType::Int8) => math_op::<Int64Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int16) => math_op::<Int64Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int32) => math_op::<Int64Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int64) => math_op::<Int64Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| a $op b),

            _ => Err(binary_error($opcode, $lhs.data_type(), $rhs.data_type())),
        }
    };
}

macro_rules! binary_order_array {
    ($opcode:expr, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::String, DataType::String) => {
                let a = $lhs.downcast_ref::<StringArray>();
                let b = $rhs.downcast_ref::<StringArray>();
                let mut builder = BooleanBuilder::with_capacity(a.len());
                for (a, b) in a.iter_opt().zip(b.iter_opt()) {
                    match (a, b) {
                        (Some(a), Some(b)) => builder.append(a $op b),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            },

            (DataType::Int8, DataType::Int8) => math_op::<Int8Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int16) => math_op::<Int8Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int32) => math_op::<Int8Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int8, DataType::Int64) => math_op::<Int8Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int8, DataType::Float32) => math_op::<Int8Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int8, DataType::Float64) => math_op::<Int8Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int16, DataType::Int8) => math_op::<Int16Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int16) => math_op::<Int16Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int32) => math_op::<Int16Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int16, DataType::Int64) => math_op::<Int16Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int16, DataType::Float32) => math_op::<Int16Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int16, DataType::Float64) => math_op::<Int16Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int32, DataType::Int8) => math_op::<Int32Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int16) => math_op::<Int32Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int32) => math_op::<Int32Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op (b as i64)),
            (DataType::Int32, DataType::Int64) => math_op::<Int32Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as i64) $op b),
            (DataType::Int32, DataType::Float32) => math_op::<Int32Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int32, DataType::Float64) => math_op::<Int32Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Int64, DataType::Int8) => math_op::<Int64Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int16) => math_op::<Int64Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int32) => math_op::<Int64Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as i64)),
            (DataType::Int64, DataType::Int64) => math_op::<Int64Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| a $op b),
            (DataType::Int64, DataType::Float32) => math_op::<Int64Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Int64, DataType::Float64) => math_op::<Int64Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Float32, DataType::Int8) => math_op::<Float32Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int16) => math_op::<Float32Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int32) => math_op::<Float32Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Int64) => math_op::<Float32Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Float32) => math_op::<Float32Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op (b as f64)),
            (DataType::Float32, DataType::Float64) => math_op::<Float32Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| (a as f64) $op b),

            (DataType::Float64, DataType::Int8) => math_op::<Float64Type, Int8Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int16) => math_op::<Float64Type, Int16Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int32) => math_op::<Float64Type, Int32Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Int64) => math_op::<Float64Type, Int64Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Float32) => math_op::<Float64Type, Float32Type, BooleanType, _>($lhs, $rhs, |a, b| a $op (b as f64)),
            (DataType::Float64, DataType::Float64) => math_op::<Float64Type, Float64Type, BooleanType, _>($lhs, $rhs, |a, b| a $op b),

            _ => Err(binary_error($opcode, $lhs.data_type(), $rhs.data_type())),
        }
    };
}

macro_rules! binary_logic_array {
    ($opcode:expr, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs.data_type(), $rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                let a = $lhs.downcast_ref::<BooleanArray>();
                let b = $rhs.downcast_ref::<BooleanArray>();
                if let (Some(a_scalar), Some(b_scalar)) = (a.to_scalar(), b.to_scalar()) {
                    return match (a_scalar, b_scalar) {
                        (Some(a_scalar), Some(b_scalar)) => Ok(Arc::new(BooleanArray::new_scalar(a.len(), Some(a_scalar $op b_scalar)))),
                        _ => Ok(Arc::new(BooleanArray::new_scalar(a.len(), None))),
                    }
                }
                let mut builder = BooleanBuilder::with_capacity(a.len());
                for (a, b) in a.iter_opt().zip(b.iter_opt()) {
                    match (a, b) {
                        (Some(a), Some(b)) => builder.append(a $op b),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            },

            _ => Err(binary_error($opcode, $lhs.data_type(), $rhs.data_type())),
        }
    };
}

#[derive(Debug, Copy, Clone, PartialEq, Display, Serialize, Deserialize)]
pub enum BinaryOperator {
    #[display(fmt = "and")]
    And,

    #[display(fmt = "or")]
    Or,

    #[display(fmt = "=")]
    Eq,

    #[display(fmt = "!=")]
    NotEq,

    #[display(fmt = "<")]
    Lt,

    #[display(fmt = "<=")]
    LtEq,

    #[display(fmt = ">")]
    Gt,

    #[display(fmt = ">=")]
    GtEq,

    #[display(fmt = "+")]
    Plus,

    #[display(fmt = "-")]
    Minus,

    #[display(fmt = "*")]
    Multiply,

    #[display(fmt = "/")]
    Divide,

    #[display(fmt = "%")]
    Rem,
}

impl BinaryOperator {
    pub(crate) fn data_type(&self, left: DataType, right: DataType) -> Result<DataType> {
        use BinaryOperator::*;
        use DataType::*;

        match self {
            And | Or => {
                if let (Boolean, Boolean) = (left, right) {
                    Ok(Boolean)
                } else {
                    Err(binary_error(*self, left, right))
                }
            }
            Eq | NotEq => {
                if (left.is_string() && right.is_string())
                    || (left.is_integer() && right.is_integer())
                {
                    Ok(Boolean)
                } else {
                    Err(binary_error(*self, left, right))
                }
            }
            Lt | LtEq | Gt | GtEq => {
                if (left.is_numeric() && right.is_numeric())
                    || (left.is_string() && right.is_string())
                {
                    Ok(Boolean)
                } else {
                    Err(binary_error(*self, left, right))
                }
            }
            Plus | Minus | Multiply | Divide => {
                if (left.is_float() && right.is_numeric())
                    || (left.is_numeric() && right.is_float())
                {
                    Ok(DataType::Float64)
                } else if left.is_integer() && right.is_integer() {
                    Ok(DataType::Int64)
                } else {
                    Err(binary_error(*self, left, right))
                }
            }
            Rem => {
                if left.is_integer() && right.is_integer() {
                    Ok(DataType::Int64)
                } else {
                    Err(binary_error(*self, left, right))
                }
            }
        }
    }

    pub(crate) fn eval_array(&self, lhs: &dyn Array, rhs: &dyn Array) -> Result<ArrayRef> {
        anyhow::ensure!(
            lhs.len() == rhs.len(),
            "cannot perform math operation on arrays of different length"
        );

        match self {
            BinaryOperator::And => binary_logic_array!(*self, lhs, rhs, &&),
            BinaryOperator::Or => binary_logic_array!(*self, lhs, rhs, ||),
            BinaryOperator::Eq => binary_equal_array!(*self, lhs, rhs, ==),
            BinaryOperator::NotEq => binary_equal_array!(*self, lhs, rhs, !=),
            BinaryOperator::Lt => binary_order_array!(*self, lhs, rhs, <),
            BinaryOperator::LtEq => binary_order_array!(*self, lhs, rhs, <=),
            BinaryOperator::Gt => binary_order_array!(*self, lhs, rhs, >),
            BinaryOperator::GtEq => binary_order_array!(*self, lhs, rhs, >=),
            BinaryOperator::Plus => binary_arithmetic_array!(*self, lhs, rhs, +),
            BinaryOperator::Minus => binary_arithmetic_array!(*self, lhs, rhs, -),
            BinaryOperator::Multiply => binary_arithmetic_array!(*self, lhs, rhs, *),
            BinaryOperator::Divide => binary_arithmetic_array!(*self, lhs, rhs, /),
            BinaryOperator::Rem => binary_rem_array!(*self, lhs, rhs),
        }
    }
}

fn binary_error(op: BinaryOperator, left: DataType, right: DataType) -> Error {
    anyhow::anyhow!(
        "cannot perform '{}' operator on '{}' and '{}' types",
        op,
        left,
        right,
    )
}

#[inline]
fn math_op<A, B, R, F>(a: &dyn Array, b: &dyn Array, f: F) -> Result<ArrayRef>
where
    A: PrimitiveType,
    B: PrimitiveType,
    R: PrimitiveType,
    F: Fn(A::Native, B::Native) -> R::Native,
{
    let a = a.downcast_ref::<PrimitiveArray<A>>();
    let b = b.downcast_ref::<PrimitiveArray<B>>();
    if let (Some(a_scalar), Some(b_scalar)) = (a.to_scalar(), b.to_scalar()) {
        return match (a_scalar, b_scalar) {
            (Some(a_scalar), Some(b_scalar)) => Ok(Arc::new(PrimitiveArray::<R>::new_scalar(
                a.len(),
                Some(f(a_scalar, b_scalar)),
            ))),
            _ => Ok(Arc::new(PrimitiveArray::<R>::new_scalar(a.len(), None))),
        };
    }
    let mut builder = PrimitiveBuilder::<R>::with_capacity(a.len());
    for (a, b) in a.iter_opt().zip(b.iter_opt()) {
        match (a, b) {
            (Some(a), Some(b)) => builder.append(f(a, b)),
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

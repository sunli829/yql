use std::sync::Arc;

use anyhow::{Error, Result};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::array::{
    Array, ArrayExt, ArrayRef, BooleanType, DataType, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, PrimitiveArray, PrimitiveBuilder, PrimitiveType,
};

#[derive(Debug, Copy, Clone, PartialEq, Display, Deserialize, Serialize)]
pub enum UnaryOperator {
    #[display(fmt = "-")]
    Neg,

    #[display(fmt = "not")]
    Not,
}

macro_rules! integer_neg {
    ($array:expr, $ty:ty) => {{
        unary_op::<$ty, _>($array, |x| {
            x.checked_neg()
                .ok_or_else(|| anyhow::anyhow!("arithmetic overflowed"))
        })
    }};
}

impl UnaryOperator {
    pub(crate) fn data_type(&self, data_type: DataType) -> Result<DataType> {
        use DataType::*;
        use UnaryOperator::*;

        match self {
            Neg => {
                if data_type.is_numeric() {
                    Ok(data_type)
                } else {
                    Err(unary_error(*self, data_type))
                }
            }
            Not => {
                if data_type.is_boolean() {
                    Ok(Boolean)
                } else {
                    Err(unary_error(*self, data_type))
                }
            }
        }
    }

    pub(crate) fn eval_array(&self, array: &dyn Array) -> Result<ArrayRef> {
        match self {
            UnaryOperator::Neg => match array.data_type() {
                DataType::Int8 => integer_neg!(array, Int8Type),
                DataType::Int16 => integer_neg!(array, Int16Type),
                DataType::Int32 => integer_neg!(array, Int32Type),
                DataType::Int64 => integer_neg!(array, Int64Type),
                DataType::Float32 => unary_op::<Float32Type, _>(array, |x| Ok(-x)),
                DataType::Float64 => unary_op::<Float64Type, _>(array, |x| Ok(-x)),
                data_type => Err(unary_error(*self, data_type)),
            },

            UnaryOperator::Not => match array.data_type() {
                DataType::Boolean => unary_op::<BooleanType, _>(array, |x| Ok(!x)),
                data_type => Err(unary_error(*self, data_type)),
            },
        }
    }
}

fn unary_error(op: UnaryOperator, data_type: DataType) -> Error {
    anyhow::anyhow!("cannot perform '{}' operator on '{}' type", op, data_type,)
}

#[inline]
fn unary_op<T, F>(array: &dyn Array, f: F) -> Result<ArrayRef>
where
    T: PrimitiveType,
    F: Fn(T::Native) -> Result<T::Native>,
{
    let array = array.downcast_ref::<PrimitiveArray<T>>();
    if let Some(scalar) = array.to_scalar() {
        return Ok(Arc::new(PrimitiveArray::<T>::new_scalar(
            array.len(),
            match scalar {
                Some(value) => Some(f(value)?),
                None => None,
            },
        )));
    }
    let mut builder = PrimitiveBuilder::<T>::with_capacity(array.len());
    for value in array.iter_opt() {
        match value {
            Some(value) => builder.append(f(value)?),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

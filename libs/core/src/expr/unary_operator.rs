use std::sync::Arc;

use anyhow::{Error, Result};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use yql_dataset::array::{
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
                DataType::Int8 => unary_op::<Int8Type, _>(array, |x| -x),
                DataType::Int16 => unary_op::<Int16Type, _>(array, |x| -x),
                DataType::Int32 => unary_op::<Int32Type, _>(array, |x| -x),
                DataType::Int64 => unary_op::<Int64Type, _>(array, |x| -x),
                DataType::Float32 => unary_op::<Float32Type, _>(array, |x| -x),
                DataType::Float64 => unary_op::<Float64Type, _>(array, |x| -x),
                data_type => Err(unary_error(*self, data_type)),
            },
            UnaryOperator::Not => match array.data_type() {
                DataType::Boolean => unary_op::<BooleanType, _>(array, |x| !x),
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
    F: Fn(T::Native) -> T::Native,
{
    let array = array.downcast_ref::<PrimitiveArray<T>>();
    if let Some(scalar) = array.to_scalar() {
        return Ok(Arc::new(PrimitiveArray::<T>::new_scalar(
            array.len(),
            scalar.map(|value| f(value)),
        )));
    }
    let mut builder = PrimitiveBuilder::<T>::with_capacity(array.len());
    for value in array.iter_opt() {
        match value {
            Some(value) => builder.append(f(value)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

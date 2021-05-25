use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use yql_dataset::array::{
    ArrayRef, BooleanArray, DataType, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray,
};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Literal {
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Boolean(n) => write!(f, "{}", n),
            Literal::Int(n) => write!(f, "{}", n),
            Literal::Float(n) => write!(f, "{}", n),
            Literal::String(n) => write!(f, "\"{}\"", n),
        }
    }
}

impl Literal {
    pub fn data_type(&self) -> DataType {
        match self {
            Literal::Boolean(_) => DataType::Boolean,
            Literal::Int(n) => {
                if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 {
                    DataType::Int8
                } else if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 {
                    DataType::Int16
                } else if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    DataType::Int32
                } else {
                    DataType::Int64
                }
            }
            Literal::Float(n) => {
                if *n >= f32::MIN as f64 && *n <= f32::MAX as f64 {
                    DataType::Float32
                } else {
                    DataType::Float64
                }
            }
            Literal::String(_) => DataType::String,
        }
    }

    pub fn to_array(&self, len: usize) -> ArrayRef {
        match self {
            Literal::Boolean(n) => Arc::new(BooleanArray::new_scalar(len, Some(*n))),
            Literal::Int(n) => {
                if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 {
                    Arc::new(Int8Array::new_scalar(len, Some(*n as i8)))
                } else if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 {
                    Arc::new(Int16Array::new_scalar(len, Some(*n as i16)))
                } else if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    Arc::new(Int32Array::new_scalar(len, Some(*n as i32)))
                } else {
                    Arc::new(Int64Array::new_scalar(len, Some(*n)))
                }
            }
            Literal::Float(n) => {
                if *n >= f32::MIN as f64 && *n <= f32::MAX as f64 {
                    Arc::new(Float32Array::new_scalar(len, Some(*n as f32)))
                } else {
                    Arc::new(Float64Array::new_scalar(len, Some(*n)))
                }
            }
            Literal::String(s) => Arc::new(StringArray::new_scalar(len, Some(s.as_str()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type() {
        assert_eq!(Literal::Boolean(true).data_type(), DataType::Boolean);

        assert_eq!(Literal::Int(0).data_type(), DataType::Int8);
        assert_eq!(Literal::Int(i8::MIN as i64).data_type(), DataType::Int8);
        assert_eq!(Literal::Int(i8::MAX as i64).data_type(), DataType::Int8);

        assert_eq!(Literal::Int(i16::MIN as i64).data_type(), DataType::Int16);
        assert_eq!(Literal::Int(i16::MAX as i64).data_type(), DataType::Int16);

        assert_eq!(Literal::Int(i32::MIN as i64).data_type(), DataType::Int32);
        assert_eq!(Literal::Int(i32::MAX as i64).data_type(), DataType::Int32);

        assert_eq!(Literal::Int(i64::MIN as i64).data_type(), DataType::Int64);
        assert_eq!(Literal::Int(i64::MAX as i64).data_type(), DataType::Int64);

        assert_eq!(
            Literal::Float(f32::MIN as f64).data_type(),
            DataType::Float32
        );
        assert_eq!(
            Literal::Float(f32::MAX as f64).data_type(),
            DataType::Float32
        );

        assert_eq!(Literal::Float(f64::MIN).data_type(), DataType::Float64);
        assert_eq!(Literal::Float(f64::MAX).data_type(), DataType::Float64);

        assert_eq!(
            Literal::String("abc".to_string()).data_type(),
            DataType::String
        );
    }
}

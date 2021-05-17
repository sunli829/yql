use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use chrono::TimeZone;
use serde::{Deserialize, Serialize};

use crate::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Timestamp(i64),
    String(Arc<str>),
}

impl Default for Scalar {
    #[inline]
    fn default() -> Self {
        Scalar::Null
    }
}

macro_rules! impl_from_numerics {
    ($(($ty:ty, $item:ident)),*) => {
        $(
        impl From<$ty> for Scalar {
            fn from(value: $ty) -> Self {
                Scalar::$item(value)
            }
        }
        )*
    };
}

impl_from_numerics!(
    (i8, Int8),
    (i16, Int16),
    (i32, Int32),
    (i64, Int64),
    (f32, Float32),
    (f64, Float64)
);

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Scalar::String(value.into())
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Scalar::String(value.into())
    }
}

impl From<bool> for Scalar {
    fn from(value: bool) -> Self {
        Scalar::Boolean(value)
    }
}

impl From<()> for Scalar {
    fn from(_: ()) -> Self {
        Scalar::Null
    }
}

impl Scalar {
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Null => DataType::Null,
            Scalar::Int8(_) => DataType::Int8,
            Scalar::Int16(_) => DataType::Int16,
            Scalar::Int32(_) => DataType::Int32,
            Scalar::Int64(_) => DataType::Int64,
            Scalar::Float32(_) => DataType::Float32,
            Scalar::Float64(_) => DataType::Float64,
            Scalar::Boolean(_) => DataType::Boolean,
            Scalar::Timestamp(_) => DataType::Timestamp(None),
            Scalar::String(_) => DataType::String,
        }
    }

    #[inline]
    pub fn is_true(&self) -> bool {
        matches!(self, Scalar::Boolean(true))
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Null => f.write_str("null"),
            Scalar::Int8(n) => write!(f, "{}", n),
            Scalar::Int16(n) => write!(f, "{}", n),
            Scalar::Int32(n) => write!(f, "{}", n),
            Scalar::Int64(n) => write!(f, "{}", n),
            Scalar::Float32(n) => write!(f, "{}", n),
            Scalar::Float64(n) => write!(f, "{}", n),
            Scalar::Boolean(n) => write!(f, "{}", n),
            Scalar::Timestamp(n) => write!(f, "{}", chrono::Local.timestamp_millis(*n)),
            Scalar::String(n) => f.write_str(n),
        }
    }
}

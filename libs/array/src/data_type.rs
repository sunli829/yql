use chrono_tz::Tz;
use derive_more::Display;
use serde::{Deserialize, Serialize};

/// The sets of data types.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Display, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Null type
    #[display(fmt = "null")]
    Null,

    /// A signed 8-bit integer.
    #[display(fmt = "int8")]
    Int8,

    /// A signed 16-bit integer.
    #[display(fmt = "int16")]
    Int16,

    /// A signed 32-bit integer.
    #[display(fmt = "int32")]
    Int32,

    /// A signed 64-bit integer.
    #[display(fmt = "int64")]
    Int64,

    /// A 32-bit floating point number.
    #[display(fmt = "float32")]
    Float32,

    /// A 64-bit floating point number.
    #[display(fmt = "float64")]
    Float64,

    /// A boolean type representing the values `true` and `false`.
    #[display(fmt = "boolean")]
    Boolean,

    /// A timestamp type, it can attach a timezone.
    #[display(fmt = "timestamp")]
    Timestamp(Option<Tz>),

    /// A variable-length string in Unicode with UTF-8 encoding.
    #[display(fmt = "string")]
    String,
}

impl DataType {
    /// Returns `true` if this type is a numeric type (integer or float).
    #[inline]
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    }

    /// Returns `true` if this type is a integer type.
    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    }

    /// Returns `true` if this type is a float type.
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    /// Returns `true` if this type is a boolean type.
    #[inline]
    pub fn is_boolean(&self) -> bool {
        matches!(self, DataType::Boolean)
    }

    /// Returns `true` if this type is a string type.
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::String)
    }

    /// Returns `true` if this type is a timestamp type.
    #[inline]
    pub fn is_timestamp(&self) -> bool {
        matches!(self, DataType::Timestamp(_))
    }

    /// Returns `true` if this type can be cast to `to` type.
    #[inline]
    pub fn can_cast_to(&self, to: Self) -> bool {
        use DataType::*;

        if self == &to {
            return true;
        }

        match to {
            Null => matches!(self, Null),
            Int8 => matches!(self, Int8),
            Int16 => matches!(self, Int8 | Int16),
            Int32 => matches!(self, Int8 | Int16 | Int32),
            Int64 => matches!(self, Int8 | Int16 | Int32 | Int64),
            Float32 => matches!(self, Int8 | Int16 | Int32 | Int64 | Float32),
            Float64 => matches!(self, Int8 | Int16 | Int32 | Int64 | Float32 | Float64),
            Boolean => matches!(self, Boolean),
            Timestamp(_) => matches!(self, Timestamp(_)),
            String => true,
        }
    }
}

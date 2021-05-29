use chrono_tz::Tz;
use derive_more::Display;
use serde::{Deserialize, Serialize};

/// The sets of data types.
#[derive(Debug, Copy, Clone, Display, Hash, Serialize, Deserialize)]
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

impl Eq for DataType {}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        use DataType::*;

        match (self, other) {
            (Null, Null) => true,
            (Int8, Int8) => true,
            (Int16, Int16) => true,
            (Int32, Int32) => true,
            (Int64, Int64) => true,
            (Float32, Float32) => true,
            (Float64, Float64) => true,
            (Boolean, Boolean) => true,
            (Timestamp(_), Timestamp(_)) => true,
            (String, String) => true,
            _ => false,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::DataType::*;

    macro_rules! test_cast {
        ($t:expr => $($mt1:tt ), + | $($mt2:tt ), +) => {
            test_cast!(@check1 $t => $($mt1), + , $($mt2 ), +);
            test_cast!(@check2 $t => $($mt1), + | $($mt2 ), + );
        };

        (@check1 $t:expr => $($mt:pat ), +) => {#[allow(unused_parens)] match $t {$($mt => {})+}};

        (@check2 $t:expr => $data:tt , $($tail:tt)*) => {
            test_cast!(@check3 $t => $data);
            test_cast!(@check2 $t => $($tail)*);
        };
        (@check2 $t:expr => $data:tt | $($tail:tt)*) => {
            test_cast!(@check3 $t => $data);
            test_cast!(@check4 $t => $($tail)*);
        };

        (@check3 $t:expr =>)=>{};
        (@check3 $t:expr => ($data:tt(_))) => {assert!($t.can_cast_to($data(None)));};
        (@check3 $t:expr => $data:tt) => {assert!($t.can_cast_to($data));};

        (@check4 $t:expr => $data:tt , $($tail:tt)*) => {
            test_cast!(@check4 $t => $data);
            test_cast!(@check4 $t => $($tail)*);
        };

        (@check4 $t:expr =>)=>{};
        (@check4 $t:expr => ($data:tt(_))) => {assert!(!$t.can_cast_to($data(None)));};
        (@check4 $t:expr => $data:tt) => {assert!(!$t.can_cast_to($data));};
}

    #[test]
    fn test_null_can_cast() {
        test_cast!(Null => Null, String | Int8, Int16, Int32, Int64, Float32, Float64, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_int8_can_cast() {
        test_cast!(Int8 => Int8, Int16, Int32, Int64, Float32, Float64, String | Null, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_int16_can_cast() {
        test_cast!(Int16 => Int16, Int32, Int64, Float32, Float64, String | Null, Int8, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_int32_can_cast() {
        test_cast!(Int32 => Int32, Int64, Float32, Float64, String | Null, Int8, Int16, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_int64_can_cast() {
        test_cast!(Int64 => Int64, Float32, Float64, String | Null, Int8, Int16, Int32, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_float32_can_cast() {
        test_cast!(Float32 => Float32, Float64, String | Null, Int8, Int16, Int32, Int64, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_float64_can_cast() {
        test_cast!(Float64 => Float64, String | Null, Int8, Int16, Int32, Int64, Float32, Boolean, (Timestamp(_)));
    }

    #[test]
    fn test_boolean_can_cast() {
        test_cast!(Boolean => Boolean, String | Null, Int8, Int16, Int32, Int64, Float32, Float64, (Timestamp(_)));
    }

    #[test]
    fn test_timestamp_can_cast() {
        test_cast!(Timestamp(None) => (Timestamp(_)), String | Null, Int8, Int16, Int32, Int64, Float32, Float64, Boolean);
    }

    #[test]
    fn test_string_can_cast() {
        test_cast!(String =>  String | Null, Int8, Int16, Int32, Int64, Float32, Float64, Boolean, (Timestamp(_)));
    }
}

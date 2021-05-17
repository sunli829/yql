use std::any::Any;

use crate::{
    Array, BooleanType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, PrimitiveArray, Scalar, StringArray, TimestampType,
};

macro_rules! get_scalar_value {
    ($array:expr, $index:expr, $ty:ty, $scalar_ty:ident) => {
        $array
            .downcast_ref::<PrimitiveArray<$ty>>()
            .value_opt($index)
            .map(Scalar::$scalar_ty)
            .unwrap_or_default()
    };
}

pub trait ArrayExt: Array {
    fn downcast_ref<T: Any>(&self) -> &T {
        self.as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("downcast_ref error: {}", std::any::type_name::<T>()))
    }

    /// Returns a scalar of the value at position `index`.
    ///
    /// # Panics
    ///
    /// Panics if index > len.
    #[inline]
    fn scalar_value(&self, index: usize) -> Scalar {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        match self.data_type() {
            DataType::Null => Scalar::Null,
            DataType::Int8 => get_scalar_value!(self, index, Int8Type, Int8),
            DataType::Int16 => get_scalar_value!(self, index, Int16Type, Int16),
            DataType::Int32 => get_scalar_value!(self, index, Int32Type, Int32),
            DataType::Int64 => get_scalar_value!(self, index, Int64Type, Int64),
            DataType::Float32 => get_scalar_value!(self, index, Float32Type, Float32),
            DataType::Float64 => get_scalar_value!(self, index, Float64Type, Float64),
            DataType::Boolean => get_scalar_value!(self, index, BooleanType, Boolean),
            DataType::Timestamp(_) => get_scalar_value!(self, index, TimestampType, Timestamp),
            DataType::String => self
                .downcast_ref::<StringArray>()
                .value_opt(index)
                .map(|s| Scalar::String(s.into()))
                .unwrap_or_default(),
        }
    }
}

impl<T: Array + ?Sized> ArrayExt for T {}

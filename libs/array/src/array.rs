use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    BooleanType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    NullArray, PrimitiveArray, StringArray, TimestampType,
};

/// Trait for dealing with different types of array at runtime when the type of the array is not known in advance.
pub trait Array: Debug + Send + Sync {
    /// Returns the array as Any so that it can be downcasted to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the data type of this array.
    fn data_type(&self) -> DataType;

    /// Returns true if the length is empty.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length (i.e., number of elements) of this array.
    fn len(&self) -> usize;

    /// Returns a zero-copy slice of this array with the indicated `offset` and `length`.
    ///
    /// # Panics
    ///
    /// Panics if offset > len or offset + length > len
    fn slice(&self, offset: usize, length: usize) -> ArrayRef;

    /// Shortens the array, keeping the first len elements and dropping the rest.
    ///
    /// # Panics
    ///
    /// Panics if length > len.
    #[inline]
    fn truncate(&self, length: usize) -> ArrayRef {
        self.slice(0, length)
    }

    /// Returns whether the element at position `index` is not null.
    ///
    /// # Panics
    ///
    /// Panics if index > len.
    fn is_valid(&self, index: usize) -> bool;

    /// Returns whether the element at position `index` is null.
    ///
    /// # Panics
    ///
    /// Panics if index > len.
    #[inline]
    fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }

    /// Returns the total number of null values in this array.
    fn null_count(&self) -> usize;
}

/// A reference-counted reference to a generic `Array`.
pub type ArrayRef = Arc<dyn Array>;

macro_rules! eq_primitive_array {
    ($ty:ty, $left:expr, $right:expr) => {
        $left.as_any().downcast_ref::<PrimitiveArray<$ty>>()
            == $right.as_any().downcast_ref::<PrimitiveArray<$ty>>()
    };
}

impl PartialEq for dyn Array {
    fn eq(&self, other: &Self) -> bool {
        if self.data_type() != self.data_type() {
            return false;
        }
        match self.data_type() {
            DataType::Null => {
                self.as_any().downcast_ref::<NullArray>()
                    == other.as_any().downcast_ref::<NullArray>()
            }
            DataType::Int8 => eq_primitive_array!(Int8Type, self, other),
            DataType::Int16 => eq_primitive_array!(Int16Type, self, other),
            DataType::Int32 => eq_primitive_array!(Int32Type, self, other),
            DataType::Int64 => eq_primitive_array!(Int64Type, self, other),
            DataType::Float32 => eq_primitive_array!(Float32Type, self, other),
            DataType::Float64 => eq_primitive_array!(Float64Type, self, other),
            DataType::Boolean => eq_primitive_array!(BooleanType, self, other),
            DataType::Timestamp(_) => eq_primitive_array!(TimestampType, self, other),
            DataType::String => {
                self.as_any().downcast_ref::<StringArray>()
                    == other.as_any().downcast_ref::<StringArray>()
            }
        }
    }
}

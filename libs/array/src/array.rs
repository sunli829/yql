use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::DataType;

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

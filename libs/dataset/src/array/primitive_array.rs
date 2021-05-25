use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::array::bitmap::{Bitmap, BitmapBuilder};
use crate::array::{Array, ArrayBuilder, ArrayRef, DataType};

pub trait NativeType:
    Debug + Copy + Send + Sync + Default + PartialEq + Serialize + DeserializeOwned + 'static
{
}

pub trait PrimitiveType: Copy + Send + Sync + 'static {
    const DATA_TYPE: DataType;

    type Native: NativeType;

    fn byte_width() -> usize {
        std::mem::size_of::<Self::Native>()
    }
}

macro_rules! impl_native_types {
    ($($ty:ty),*) => {
        $(
        impl NativeType for $ty {}
        )*
    };
}

macro_rules! impl_primitive_types {
    ($(($pt:ident, $native_ty:ty, $dt:expr)),*) => {
        $(
        #[derive(Debug, Copy, Clone)]
        pub struct $pt;

        impl PrimitiveType for $pt {
            const DATA_TYPE: DataType = $dt;
            type Native = $native_ty;
        }
        )*
    };
}

impl_native_types!(i8, i16, i32, i64, f32, f64, bool);

impl_primitive_types!(
    (Int8Type, i8, DataType::Int8),
    (Int16Type, i16, DataType::Int16),
    (Int32Type, i32, DataType::Int32),
    (Int64Type, i64, DataType::Int64),
    (Float32Type, f32, DataType::Float32),
    (Float64Type, f64, DataType::Float64),
    (BooleanType, bool, DataType::Boolean),
    (TimestampType, i64, DataType::Timestamp(None))
);

/// Array builder for fixed-width primitive types.
pub struct PrimitiveBuilder<T: PrimitiveType> {
    data: BytesMut,
    bitmap: BitmapBuilder,
    _mark: PhantomData<T>,
}

impl<T: PrimitiveType> Default for PrimitiveBuilder<T> {
    fn default() -> Self {
        PrimitiveBuilder {
            data: BytesMut::new(),
            bitmap: BitmapBuilder::default(),
            _mark: PhantomData,
        }
    }
}

impl<T: PrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.data.len() / T::byte_width()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: PrimitiveType> PrimitiveBuilder<T> {
    #[inline]
    pub fn with_capacity(size: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(size * T::byte_width()),
            bitmap: BitmapBuilder::default(),
            _mark: PhantomData,
        }
    }

    #[inline]
    pub fn append(&mut self, value: T::Native) {
        self.data.put_slice(unsafe {
            std::slice::from_raw_parts(
                &value as *const <T as PrimitiveType>::Native as *const u8,
                T::byte_width(),
            )
        });
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.bitmap.set(self.data.len() / T::byte_width(), false);
        self.append(<T as PrimitiveType>::Native::default());
    }

    #[inline]
    pub fn append_opt(&mut self, value: Option<T::Native>) {
        match value {
            Some(value) => self.append(value),
            None => self.append_null(),
        }
    }

    pub fn finish(self) -> PrimitiveArray<T> {
        PrimitiveArray::Array {
            data: self.data.freeze(),
            bitmap: if !self.bitmap.is_empty() {
                Some(self.bitmap.finish())
            } else {
                None
            },
            _mark: PhantomData,
        }
    }
}

/// Array whose elements are of primitive types.
pub enum PrimitiveArray<T: PrimitiveType> {
    Array {
        data: Bytes,
        bitmap: Option<Bitmap>,
        _mark: PhantomData<T>,
    },
    Scalar {
        len: usize,
        value: Option<T::Native>,
    },
}

impl<T: PrimitiveType> Debug for PrimitiveArray<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut ls = f.debug_list();
        for value in self.iter() {
            ls.entry(&value);
        }
        ls.finish()
    }
}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            PrimitiveArray::Array { data, .. } => data.len() / T::byte_width(),
            PrimitiveArray::Scalar { len, .. } => *len,
        }
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        if offset > self.len() {
            panic!(
                "offset (is {}) should be <= len (is {})",
                offset,
                self.len()
            );
        }

        if offset + length > self.len() {
            panic!(
                "offset+length (is {}) should be <= len (is {})",
                offset + length,
                self.len()
            );
        }

        match self {
            PrimitiveArray::Array { data, bitmap, .. } => Arc::new(Self::Array {
                data: data.slice((offset * T::byte_width())..(offset + length) * T::byte_width()),
                bitmap: bitmap.as_ref().map(|bitmap| bitmap.offset(offset)),
                _mark: PhantomData,
            }),
            PrimitiveArray::Scalar { value, .. } => Arc::new(Self::Scalar {
                len: length,
                value: *value,
            }),
        }
    }

    #[inline]
    fn is_valid(&self, index: usize) -> bool {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        match self {
            PrimitiveArray::Array { bitmap, .. } => match &bitmap {
                Some(bitmap) => bitmap.is_valid(index),
                None => true,
            },
            PrimitiveArray::Scalar { value, .. } => value.is_some(),
        }
    }

    fn null_count(&self) -> usize {
        if let Some(scalar) = self.to_scalar() {
            if scalar.is_none() {
                self.len()
            } else {
                0
            }
        } else {
            let mut count = 0;
            for i in 0..self.len() {
                if self.is_null(i) {
                    count += 1;
                }
            }
            count
        }
    }
}

impl<A: PrimitiveType> PartialEq for PrimitiveArray<A> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter_opt().eq(other.iter_opt())
    }
}

impl<A: PrimitiveType> FromIterator<<A as PrimitiveType>::Native> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = <A as PrimitiveType>::Native>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = PrimitiveBuilder::with_capacity(iter.size_hint().0);
        for value in iter {
            builder.append(value);
        }
        builder.finish()
    }
}

impl<T: PrimitiveType> PrimitiveArray<T> {
    #[inline]
    pub fn new_scalar(len: usize, value: Option<T::Native>) -> Self {
        Self::Scalar { len, value }
    }

    #[inline]
    pub fn is_scalar_array(&self) -> bool {
        matches!(self, PrimitiveArray::Scalar { .. })
    }

    /// Returns `Some` if the array is scalar array.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use yql_array::Int32Array;
    ///
    /// let array = Int32Array::new_scalar(100, Some(1));
    /// assert_eq!(array.to_scalar(), Some(Some(1)));
    ///
    /// let array = Int32Array::new_scalar(100, None);
    /// assert_eq!(array.to_scalar(), Some(None));
    ///
    /// let array = Int32Array::from_vec(vec![1, 2, 3]);
    /// assert_eq!(array.to_scalar(), None);
    /// ```
    #[inline]
    pub fn to_scalar(&self) -> Option<Option<T::Native>> {
        match self {
            PrimitiveArray::Array { .. } => None,
            PrimitiveArray::Scalar { value, .. } => Some(*value),
        }
    }

    /// Create an empty array.
    #[inline]
    pub fn empty() -> Self {
        std::iter::empty().collect()
    }

    pub fn from_vec(values: Vec<T::Native>) -> Self {
        let mut data = BytesMut::with_capacity(values.len() * T::byte_width());
        data.extend_from_slice(unsafe {
            std::slice::from_raw_parts(
                values.as_ptr() as *const T as *const u8,
                values.len() * T::byte_width(),
            )
        });
        PrimitiveArray::Array {
            data: data.freeze(),
            bitmap: None,
            _mark: PhantomData,
        }
    }

    pub fn from_opt_vec(values: Vec<Option<T::Native>>) -> Self {
        let mut builder = PrimitiveBuilder::<T>::default();
        for value in values {
            builder.append_opt(value);
        }
        builder.finish()
    }

    #[inline]
    fn interval_value(&self, index: usize) -> T::Native {
        match self {
            PrimitiveArray::Array { data, .. } => unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const T::Native, self.len())[index]
            },
            PrimitiveArray::Scalar { value, .. } => value.unwrap_or_default(),
        }
    }

    #[inline]
    pub fn value(&self, index: usize) -> T::Native {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }
        self.interval_value(index)
    }

    #[inline]
    pub fn value_opt(&self, index: usize) -> Option<T::Native> {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        if self.is_valid(index) {
            Some(self.interval_value(index))
        } else {
            None
        }
    }

    #[inline]
    pub fn iter(&self) -> PrimitiveIter<'_, T> {
        PrimitiveIter {
            index: 0,
            array: self,
        }
    }

    #[inline]
    pub fn iter_opt(&self) -> PrimitiveOptIter<'_, T> {
        PrimitiveOptIter {
            index: 0,
            array: self,
        }
    }

    pub fn concat(&self, other: &Self) -> Self {
        if let (Some(scalar_a), Some(scalar_b)) = (self.to_scalar(), other.to_scalar()) {
            if scalar_a == scalar_b {
                return PrimitiveArray::new_scalar(self.len() + other.len(), scalar_a);
            }
        }

        let mut builder = PrimitiveBuilder::<T>::default();
        for value in self.iter_opt().chain(other.iter_opt()) {
            builder.append_opt(value);
        }
        builder.finish()
    }
}

pub struct PrimitiveIter<'a, T: PrimitiveType> {
    index: usize,
    array: &'a PrimitiveArray<T>,
}

impl<'a, T: PrimitiveType> Iterator for PrimitiveIter<'a, T> {
    type Item = T::Native;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.array.len() {
            None
        } else {
            let value = self.array.value(self.index);
            self.index += 1;
            Some(value)
        }
    }
}

impl<'a, T: PrimitiveType> DoubleEndedIterator for PrimitiveIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.array.len() {
            None
        } else {
            let value = self.array.value(self.array.len() - self.index - 1);
            self.index += 1;
            Some(value)
        }
    }
}

pub struct PrimitiveOptIter<'a, T: PrimitiveType> {
    index: usize,
    array: &'a PrimitiveArray<T>,
}

impl<'a, T: PrimitiveType> Iterator for PrimitiveOptIter<'a, T> {
    type Item = Option<T::Native>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.array.len() {
            None
        } else {
            let value = self.array.value_opt(self.index);
            self.index += 1;
            Some(value)
        }
    }
}

impl<'a, T: PrimitiveType> DoubleEndedIterator for PrimitiveOptIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.array.len() {
            None
        } else {
            let value = self.array.value_opt(self.array.len() - self.index - 1);
            self.index += 1;
            Some(value)
        }
    }
}

impl<T: PrimitiveType> Serialize for PrimitiveArray<T> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for value in self.iter_opt() {
            seq.serialize_element(&value)?;
        }
        seq.end()
    }
}

impl<'de, T: PrimitiveType> Deserialize<'de> for PrimitiveArray<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<Option<T::Native>>::deserialize(deserializer)?;
        Ok(Self::from_opt_vec(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayExt, Int32Array, Int32Builder, Scalar};

    fn create_array() -> ArrayRef {
        let mut builder = Int32Builder::default();
        for x in 0..1000 {
            if x % 2 == 0 {
                builder.append(x);
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish())
    }

    fn create_scalar_array() -> ArrayRef {
        Arc::new(Int32Array::new_scalar(1000, Some(1)))
    }

    fn create_null_scalar_array() -> ArrayRef {
        Arc::new(Int32Array::new_scalar(1000, None))
    }

    #[test]
    fn test_array_as_any() {
        let array = create_array();
        assert!(array.as_any().downcast_ref::<Int32Array>().is_some());
    }

    #[test]
    fn test_scalar_array_as_any() {
        let array = create_scalar_array();
        assert!(array.as_any().downcast_ref::<Int32Array>().is_some());
    }

    #[test]
    fn test_array_data_type() {
        let array = create_array();
        assert_eq!(array.data_type(), DataType::Int32);
    }

    #[test]
    fn test_scalar_array_data_type() {
        let array = create_scalar_array();
        assert_eq!(array.data_type(), DataType::Int32);
    }

    #[test]
    fn test_array_len() {
        let array = create_array();
        assert_eq!(array.len(), 1000);
    }

    #[test]
    fn test_scalar_array_len() {
        let array = create_scalar_array();
        assert_eq!(array.len(), 1000);

        let array = create_null_scalar_array();
        assert_eq!(array.len(), 1000);
    }

    #[test]
    fn test_array_slice() {
        let array = create_array();
        let slice = array.slice(0, 10);
        assert_eq!(slice.len(), 10);
        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 0..10 {
            if x % 2 == 0 {
                assert_eq!(array_i32.value_opt(x), Some(x as i32));
            } else {
                assert_eq!(array_i32.value_opt(x), None);
            }
        }

        let slice = array.slice(990, 10);
        assert_eq!(slice.len(), 10);
        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 990..1000 {
            if x % 2 == 0 {
                assert_eq!(array_i32.value_opt(x - 990), Some(x as i32));
            } else {
                assert_eq!(array_i32.value_opt(x - 990), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_slice() {
        let array = create_scalar_array();
        let slice = array.slice(0, 10);
        assert_eq!(slice.len(), 10);

        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 0..10 {
            assert_eq!(array_i32.value_opt(x), Some(1));
        }

        let slice = array.slice(990, 10);
        assert_eq!(slice.len(), 10);
        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 990..1000 {
            assert_eq!(array_i32.value_opt(x - 990), Some(1));
        }
    }

    #[test]
    #[should_panic]
    fn test_array_slice_should_panic_1() {
        let array = create_array();
        array.slice(1000, 1);
    }

    #[test]
    #[should_panic]
    fn test_array_slice_should_panic_2() {
        let array = create_array();
        array.slice(900, 101);
    }

    #[test]
    #[should_panic]
    fn test_array_slice_should_panic_3() {
        let array = create_array();
        array.slice(0, 1001);
    }

    #[test]
    fn test_array_truncate() {
        let array = create_array();
        let slice = array.truncate(10);
        assert_eq!(slice.len(), 10);

        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 0..10 {
            if x % 2 == 0 {
                assert_eq!(array_i32.value_opt(x), Some(x as i32));
            } else {
                assert_eq!(array_i32.value_opt(x), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_truncate() {
        let array = create_scalar_array();
        let slice = array.truncate(10);
        assert_eq!(slice.len(), 10);

        let array_i32 = slice.downcast_ref::<Int32Array>();
        for x in 0..10 {
            assert_eq!(array_i32.value_opt(x), Some(1));
        }
    }

    #[test]
    #[should_panic]
    fn test_array_truncate_should_panic() {
        let array = create_array();
        array.truncate(1001);
    }

    #[test]
    fn test_array_is_valid() {
        let array = create_array();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert!(array.is_valid(x));
            } else {
                assert!(!array.is_valid(x));
            }
        }
    }

    #[test]
    fn test_scalar_array_is_valid() {
        let array = create_scalar_array();
        for x in 0..1000 {
            assert!(array.is_valid(x));
        }

        let array = create_null_scalar_array();
        for x in 0..1000 {
            assert!(!array.is_valid(x));
        }
    }

    #[test]
    fn test_array_is_null() {
        let array = create_array();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert!(!array.is_null(x));
            } else {
                assert!(array.is_null(x));
            }
        }
    }

    #[test]
    fn test_scalar_array_is_null() {
        let array = create_scalar_array();
        for x in 0..1000 {
            assert!(!array.is_null(x));
        }

        let array = create_null_scalar_array();
        for x in 0..1000 {
            assert!(array.is_null(x));
        }
    }

    #[test]
    fn test_array_null_count() {
        let array = create_array();
        assert_eq!(array.null_count(), 500);
    }

    #[test]
    fn test_scalar_array_null_count() {
        let array = create_scalar_array();
        assert_eq!(array.null_count(), 0);

        let array = create_null_scalar_array();
        assert_eq!(array.null_count(), 1000);
    }

    #[test]
    fn test_array_scalar_value() {
        let array = create_array();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array.scalar_value(x), Scalar::Int32(x as i32));
            } else {
                assert_eq!(array.scalar_value(x), Scalar::Null);
            }
        }
    }

    #[test]
    fn test_scalar_array_scalar_value() {
        let array = create_scalar_array();
        for x in 0..1000 {
            assert_eq!(array.scalar_value(x), Scalar::Int32(1));
        }
    }

    #[test]
    fn test_array_is_scalar_array() {
        let array = create_array();
        assert!(!array.downcast_ref::<Int32Array>().is_scalar_array());
    }

    #[test]
    fn test_scalar_array_is_scalar_array() {
        let array = create_scalar_array();
        assert!(array.downcast_ref::<Int32Array>().is_scalar_array());
    }

    #[test]
    fn test_array_to_scalar() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        assert_eq!(array_i32.to_scalar(), None);
    }

    #[test]
    fn test_scalar_array_to_scalar() {
        let array = create_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        assert_eq!(array_i32.to_scalar(), Some(Some(1)));

        let array = create_null_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        assert_eq!(array_i32.to_scalar(), Some(None));
    }

    #[test]
    fn test_array_empty() {
        let array = Int32Array::empty();
        assert!(array.is_empty());
        assert!(!array.is_scalar_array());
    }

    #[test]
    fn test_array_from_vec() {
        let array = Int32Array::from_vec(vec![1, 2, 3, 4, 5]);
        assert_eq!(array.len(), 5);
        let array_i32 = array.downcast_ref::<Int32Array>();
        assert_eq!(array_i32.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_array_from_opt_vec() {
        let array = Int32Array::from_opt_vec(vec![Some(1), None, Some(3), None, Some(5)]);
        assert_eq!(array.len(), 5);
        let array_i32 = array.downcast_ref::<Int32Array>();
        assert_eq!(
            array_i32.iter_opt().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3), None, Some(5)]
        );
    }

    #[test]
    fn test_array_value() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array_i32.value(x), x as i32);
            } else {
                assert_eq!(array_i32.value(x), 0);
            }
        }
    }

    #[test]
    fn test_scalar_array_value() {
        let array = create_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for x in 0..1000 {
            assert_eq!(array_i32.value(x), 1);
        }

        let array = create_null_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for x in 0..1000 {
            assert_eq!(array_i32.value(x), 0);
        }
    }

    #[test]
    fn test_array_value_opt() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array_i32.value_opt(x), Some(x as i32));
            } else {
                assert_eq!(array_i32.value_opt(x), None);
            }
        }
    }

    #[test]
    fn test_array_iter() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for (idx, value) in array_i32.iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(idx as i32, value);
            } else {
                assert_eq!(0, value);
            }
        }
    }

    #[test]
    fn test_array_iter_rev() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for (idx, value) in array_i32.iter().rev().enumerate() {
            let idx = 1000 - (idx as i32) - 1;
            if idx % 2 == 0 {
                assert_eq!(idx, value);
            } else {
                assert_eq!(0, value);
            }
        }
    }

    #[test]
    fn test_scalar_array_iter() {
        let array = create_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for value in array_i32.iter() {
            assert_eq!(value, 1);
        }

        let array = create_null_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for value in array_i32.iter() {
            assert_eq!(value, 0);
        }
    }

    #[test]
    fn test_array_iter_opt() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for (idx, value) in array_i32.iter_opt().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(Some(idx as i32), value);
            } else {
                assert_eq!(None, value);
            }
        }
    }

    #[test]
    fn test_array_iter_opt_rev() {
        let array = create_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for (idx, value) in array_i32.iter_opt().rev().enumerate() {
            let idx = 1000 - (idx as i32) - 1;
            if idx % 2 == 0 {
                assert_eq!(Some(idx), value);
            } else {
                assert_eq!(None, value);
            }
        }
    }

    #[test]
    fn test_scalar_array_iter_opt() {
        let array = create_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for value in array_i32.iter_opt() {
            assert_eq!(value, Some(1));
        }

        let array = create_null_scalar_array();
        let array_i32 = array.downcast_ref::<Int32Array>();
        for value in array_i32.iter_opt() {
            assert_eq!(value, None);
        }
    }

    #[test]
    fn test_array_concat() {
        let array = create_array()
            .downcast_ref::<Int32Array>()
            .concat(create_array().downcast_ref::<Int32Array>());
        assert_eq!(array.len(), 2000);

        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array.value_opt(x), Some(x as i32));
            } else {
                assert_eq!(array.value_opt(x), None);
            }
        }

        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array.value_opt(x + 1000), Some(x as i32));
            } else {
                assert_eq!(array.value_opt(x + 1000), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_concat() {
        let array =
            Int32Array::new_scalar(1000, Some(1)).concat(&Int32Array::new_scalar(1000, Some(2)));
        assert_eq!(array.len(), 2000);
        assert!(!array.is_scalar_array());

        for x in 0..1000 {
            assert_eq!(array.value_opt(x), Some(1));
        }
        for x in 1000..2000 {
            assert_eq!(array.value_opt(x), Some(2));
        }

        let array =
            Int32Array::new_scalar(1000, Some(3)).concat(&Int32Array::new_scalar(500, Some(3)));
        assert_eq!(array.len(), 1500);
        assert!(array.is_scalar_array());
        for x in 0..1500 {
            assert_eq!(array.value_opt(x), Some(3));
        }
    }
}

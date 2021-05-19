use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};

use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::{Array, ArrayBuilder, ArrayRef, DataType};

/// Array builder for string.
#[derive(Default)]
pub struct StringBuilder {
    index_buf: BytesMut,
    content_buf: BytesMut,
    bitmap: BitmapBuilder,
}

impl ArrayBuilder for StringBuilder {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.index_buf.len() / (std::mem::size_of::<u32>() * 2)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.index_buf.is_empty()
    }
}

impl StringBuilder {
    #[inline]
    pub fn with_capacity(size: usize) -> Self {
        Self {
            index_buf: BytesMut::with_capacity(size * std::mem::size_of::<u32>() * 2),
            content_buf: BytesMut::new(),
            bitmap: BitmapBuilder::default(),
        }
    }

    #[inline]
    pub fn append(&mut self, value: &str) {
        self.index_buf
            .put_slice(&(self.content_buf.len() as u32).to_ne_bytes());
        self.index_buf
            .put_slice(&(value.len() as u32).to_ne_bytes());
        self.content_buf.put_slice(value.as_bytes());
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.bitmap.set(
            self.index_buf.len() / (std::mem::size_of::<u32>() * 2),
            false,
        );
        self.append("");
    }

    #[inline]
    pub fn append_opt(&mut self, value: Option<&str>) {
        match value {
            Some(value) => self.append(value),
            None => self.append_null(),
        }
    }

    pub fn finish(self) -> StringArray {
        StringArray::Array {
            offset: 0,
            length: self.index_buf.len()
                / (std::mem::size_of::<u32>() + std::mem::size_of::<u32>()),
            index_buf: self.index_buf,
            content_buf: self.content_buf,
            bitmap: if !self.bitmap.is_empty() {
                Some(self.bitmap.finish())
            } else {
                None
            },
        }
    }
}

/// An array where each element is a variable-sized sequence of bytes representing a string whose maximum length (in bytes) is represented by a u32.
pub enum StringArray {
    Array {
        offset: usize,
        length: usize,
        index_buf: BytesMut,
        content_buf: BytesMut,
        bitmap: Option<Bitmap>,
    },
    Scalar {
        len: usize,
        value: Option<Arc<str>>,
    },
}

impl Debug for StringArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut ls = f.debug_list();
        for value in self.iter() {
            ls.entry(&value);
        }
        ls.finish()
    }
}

impl Array for StringArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn data_type(&self) -> DataType {
        DataType::String
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            StringArray::Array { length, .. } => *length,
            StringArray::Scalar { len, .. } => *len,
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
            StringArray::Array {
                index_buf,
                content_buf,
                bitmap,
                ..
            } => Arc::new(StringArray::Array {
                offset,
                length,
                index_buf: index_buf.clone(),
                content_buf: content_buf.clone(),
                bitmap: bitmap.as_ref().map(|bitmap| bitmap.offset(offset)),
            }),
            StringArray::Scalar { len, value, .. } => Arc::new(Self::Scalar {
                len: length.min(*len),
                value: value.clone(),
            }),
        }
    }

    fn is_valid(&self, index: usize) -> bool {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        match self {
            StringArray::Array { bitmap, .. } => match &bitmap {
                Some(bitmap) => bitmap.is_valid(index),
                None => true,
            },
            StringArray::Scalar { value, .. } => value.is_some(),
        }
    }

    fn null_count(&self) -> usize {
        let mut count = 0;
        for i in 0..self.len() {
            if self.is_null(i) {
                count += 1;
            }
        }
        count
    }
}

impl PartialEq for StringArray {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter_opt().eq(other.iter_opt())
    }
}

impl<A: AsRef<str>> FromIterator<A> for StringArray {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringBuilder::with_capacity(iter.size_hint().0);
        for value in iter {
            builder.append(value.as_ref());
        }
        builder.finish()
    }
}

impl StringArray {
    #[inline]
    pub fn new_scalar(len: usize, value: Option<impl Into<Arc<str>>>) -> Self {
        Self::Scalar {
            len,
            value: value.map(Into::into),
        }
    }

    #[inline]
    pub fn is_scalar_array(&self) -> bool {
        matches!(self, StringArray::Scalar { .. })
    }

    #[inline]
    pub fn to_scalar(&self) -> Option<Option<&str>> {
        match self {
            StringArray::Array { .. } => None,
            StringArray::Scalar { value, .. } => Some(value.as_deref()),
        }
    }

    #[inline]
    pub fn empty() -> Self {
        Self::from_iter(std::iter::empty::<&str>())
    }

    pub fn from_vec<A: AsRef<str>>(values: Vec<A>) -> Self {
        Self::from_iter(values.into_iter())
    }

    pub fn from_opt_vec<A: AsRef<str>>(values: Vec<Option<A>>) -> Self {
        let mut builder = StringBuilder::default();
        for value in values {
            builder.append_opt(value.as_ref().map(AsRef::as_ref));
        }
        builder.finish()
    }

    #[inline]
    pub fn value(&self, index: usize) -> &str {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        match self {
            StringArray::Array {
                offset,
                index_buf,
                content_buf,
                ..
            } => {
                let index = *offset + index;
                let index_data = unsafe {
                    std::slice::from_raw_parts(
                        index_buf.as_ptr() as *const u32,
                        index_buf.len() / std::mem::size_of::<u32>(),
                    )
                };
                let p = index * 2;
                let data_offset = index_data[p];
                let data_length = index_data[p + 1];
                unsafe {
                    std::str::from_utf8_unchecked(
                        &content_buf.as_ref()
                            [data_offset as usize..data_offset as usize + data_length as usize],
                    )
                }
            }
            StringArray::Scalar { value, .. } => value.as_deref().unwrap_or_default(),
        }
    }

    #[inline]
    pub fn value_opt(&self, index: usize) -> Option<&str> {
        if index >= self.len() {
            panic!("index (is {}) should be <= len (is {})", index, self.len());
        }

        if self.is_valid(index) {
            Some(self.value(index))
        } else {
            None
        }
    }

    #[inline]
    pub fn iter(&self) -> StringIter {
        StringIter {
            index: 0,
            array: self,
        }
    }

    #[inline]
    pub fn iter_opt(&self) -> StringOptIter {
        StringOptIter {
            index: 0,
            array: self,
        }
    }

    pub fn concat(&self, other: &Self) -> Self {
        if let (Some(scalar_a), Some(scalar_b)) = (self.to_scalar(), other.to_scalar()) {
            if scalar_a == scalar_b {
                return StringArray::new_scalar(self.len() + other.len(), scalar_a);
            }
        }

        let mut builder = StringBuilder::default();
        for value in self.iter_opt().chain(other.iter_opt()) {
            builder.append_opt(value);
        }
        builder.finish()
    }
}

pub struct StringIter<'a> {
    index: usize,
    array: &'a StringArray,
}

impl<'a> Iterator for StringIter<'a> {
    type Item = &'a str;

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

impl<'a> DoubleEndedIterator for StringIter<'a> {
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

pub struct StringOptIter<'a> {
    index: usize,
    array: &'a StringArray,
}

impl<'a> Iterator for StringOptIter<'a> {
    type Item = Option<&'a str>;

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

impl<'a> DoubleEndedIterator for StringOptIter<'a> {
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

#[cfg(test)]
mod tests {
    use crate::{ArrayExt, Scalar};

    use super::*;

    fn map_to_char(x: usize) -> char {
        ((x % 58) + 65) as u8 as char
    }

    fn map_to_string(x: usize) -> String {
        map_to_char(x).to_string()
    }

    fn create_array() -> ArrayRef {
        let mut builder = StringBuilder::default();

        for x in 0..1000 {
            if x % 2 == 0 {
                builder.append(map_to_string(x).as_str());
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish())
    }

    fn create_scalar_array() -> ArrayRef {
        Arc::new(StringArray::new_scalar(1000, Some("hello")))
    }

    fn create_null_scalar_array() -> ArrayRef {
        Arc::new(StringArray::new_scalar(1000, Option::<&'static str>::None))
    }

    #[test]
    fn test_array_as_any() {
        let array = create_array();
        assert!(array.as_any().downcast_ref::<StringArray>().is_some());
    }

    #[test]
    fn test_scalar_array_as_any() {
        let array = create_scalar_array();
        assert!(array.as_any().downcast_ref::<StringArray>().is_some());
    }

    #[test]
    fn test_array_data_type() {
        let array = create_array();
        assert_eq!(array.data_type(), DataType::String);
    }

    #[test]
    fn test_scalar_array_data_type() {
        let array = create_scalar_array();
        assert_eq!(array.data_type(), DataType::String);
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
        let array_string = slice.downcast_ref::<StringArray>();
        for x in 0..10 {
            if x % 2 == 0 {
                assert_eq!(array_string.value_opt(x), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array_string.value_opt(x), None);
            }
        }

        let slice = array.slice(990, 10);
        assert_eq!(slice.len(), 10);
        let array_string = slice.downcast_ref::<StringArray>();
        for x in 990..1000 {
            if x % 2 == 0 {
                assert_eq!(array_string.value_opt(x - 990), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array_string.value_opt(x - 990), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_slice() {
        let array = create_scalar_array();
        let slice = array.slice(0, 10);
        assert_eq!(slice.len(), 10);

        let array_string = slice.downcast_ref::<StringArray>();
        for x in 0..10 {
            assert_eq!(array_string.value_opt(x), Some("hello"));
        }

        let slice = array.slice(990, 10);
        assert_eq!(slice.len(), 10);
        let array_string = slice.downcast_ref::<StringArray>();
        for x in 990..1000 {
            assert_eq!(array_string.value_opt(x - 990), Some("hello"));
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

        let array_string = slice.downcast_ref::<StringArray>();
        for x in 0..10 {
            if x % 2 == 0 {
                assert_eq!(array_string.value_opt(x), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array_string.value_opt(x), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_truncate() {
        let array = create_scalar_array();
        let slice = array.truncate(10);
        assert_eq!(slice.len(), 10);

        let array_string = slice.downcast_ref::<StringArray>();
        for x in 0..10 {
            assert_eq!(array_string.value_opt(x), Some("hello"));
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
                assert_eq!(array.scalar_value(x), Scalar::String(map_to_string(x).into()));
            } else {
                assert_eq!(array.scalar_value(x), Scalar::Null);
            }
        }
    }

    #[test]
    fn test_scalar_array_scalar_value() {
        let array = create_scalar_array();
        for x in 0..1000 {
            assert_eq!(array.scalar_value(x), Scalar::String("hello".into()));
        }
    }

    #[test]
    fn test_array_is_scalar_array() {
        let array = create_array();
        assert!(!array.downcast_ref::<StringArray>().is_scalar_array());
    }

    #[test]
    fn test_scalar_array_is_scalar_array() {
        let array = create_scalar_array();
        assert!(array.downcast_ref::<StringArray>().is_scalar_array());
    }

    #[test]
    fn test_array_to_scalar() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(array_string.to_scalar(), None);
    }

    #[test]
    fn test_scalar_array_to_scalar() {
        let array = create_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(array_string.to_scalar(), Some(Some("hello")));

        let array = create_null_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(array_string.to_scalar(), Some(None));
    }

    #[test]
    fn test_array_empty() {
        let array = StringArray::empty();
        assert!(array.is_empty());
        assert!(!array.is_scalar_array());
    }

    #[test]
    fn test_array_from_vec() {
        let array = StringArray::from_vec(vec!["a", "b", "c", "d", "e"]);
        assert_eq!(array.len(), 5);
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(array_string.iter().collect::<Vec<_>>(), vec!["a", "b", "c", "d", "e"]);

        let array = StringArray::from_vec(vec!["a", "b", "c", "d", "e"].iter()
            .map(|str| str.to_owned()).collect());
        assert_eq!(array.len(), 5);
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(array_string.iter().collect::<Vec<_>>(),
                   vec!["a", "b", "c", "d", "e"].iter().map(|str| str.to_owned()).collect::<Vec<_>>());
    }

    #[test]
    fn test_array_from_opt_vec() {
        let array = StringArray::from_opt_vec(vec![Some("a"), None, Some("c"), None, Some("e")]);
        assert_eq!(array.len(), 5);
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(
            array_string.iter_opt().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("c"), None, Some("e")]
        );

        let array = StringArray::from_opt_vec(vec![Some("a"), None, Some("c"), None, Some("e")]);
        assert_eq!(array.len(), 5);
        let array_string = array.downcast_ref::<StringArray>();
        assert_eq!(
            array_string.iter_opt().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("c"), None, Some("e")]
        );
    }

    #[test]
    fn test_array_value() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array_string.value(x), map_to_string(x).as_str());
            } else {
                assert_eq!(array_string.value(x), "");
            }
        }
    }

    #[test]
    fn test_scalar_array_value() {
        let array = create_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for x in 0..1000 {
            assert_eq!(array_string.value(x), "hello");
        }

        let array = create_null_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for x in 0..1000 {
            assert_eq!(array_string.value(x), "");
        }
    }

    #[test]
    fn test_array_value_opt() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array_string.value_opt(x), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array_string.value_opt(x), None);
            }
        }
    }

    #[test]
    fn test_array_iter() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for (idx, value) in array_string.iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(map_to_string(idx).as_str(), value);
            } else {
                assert_eq!("", value);
            }
        }
    }

    #[test]
    fn test_array_iter_rev() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for (idx, value) in array_string.iter().rev().enumerate() {
            let idx = 1000 - idx - 1;
            if idx % 2 == 0 {
                assert_eq!(map_to_string(idx).as_str(), value);
            } else {
                assert_eq!("", value);
            }
        }
    }

    #[test]
    fn test_scalar_array_iter() {
        let array = create_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for value in array_string.iter() {
            assert_eq!(value, "hello");
        }

        let array = create_null_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for value in array_string.iter() {
            assert_eq!(value, "");
        }
    }

    #[test]
    fn test_array_iter_opt() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for (idx, value) in array_string.iter_opt().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(Some(map_to_string(idx).as_str()), value);
            } else {
                assert_eq!(None, value);
            }
        }
    }

    #[test]
    fn test_array_iter_opt_rev() {
        let array = create_array();
        let array_string = array.downcast_ref::<StringArray>();
        for (idx, value) in array_string.iter_opt().rev().enumerate() {
            let idx = 1000 - idx - 1;
            if idx % 2 == 0 {
                assert_eq!(Some(map_to_string(idx as usize).as_str()), value);
            } else {
                assert_eq!(None, value);
            }
        }
    }

    #[test]
    fn test_scalar_array_iter_opt() {
        let array = create_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for value in array_string.iter_opt() {
            assert_eq!(value, Some("hello"));
        }

        let array = create_null_scalar_array();
        let array_string = array.downcast_ref::<StringArray>();
        for value in array_string.iter_opt() {
            assert_eq!(value, None);
        }
    }

    #[test]
    fn test_array_concat() {
        let array = create_array()
            .downcast_ref::<StringArray>()
            .concat(create_array().downcast_ref::<StringArray>());
        assert_eq!(array.len(), 2000);

        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array.value_opt(x), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array.value_opt(x), None);
            }
        }

        for x in 0..1000 {
            if x % 2 == 0 {
                assert_eq!(array.value_opt(x + 1000), Some(map_to_string(x).as_str()));
            } else {
                assert_eq!(array.value_opt(x + 1000), None);
            }
        }
    }

    #[test]
    fn test_scalar_array_concat() {
        let array =
            StringArray::new_scalar(1000, Some("hello"))
                .concat(&StringArray::new_scalar(1000, Some("world")));
        assert_eq!(array.len(), 2000);
        assert!(!array.is_scalar_array());

        for x in 0..1000 {
            assert_eq!(array.value_opt(x), Some("hello"));
        }
        for x in 1000..2000 {
            assert_eq!(array.value_opt(x), Some("world"));
        }

        let array =
            StringArray::new_scalar(1000, Some("yql"))
                .concat(&StringArray::new_scalar(500, Some("yql")));
        assert_eq!(array.len(), 1500);
        assert!(array.is_scalar_array());
        for x in 0..1500 {
            assert_eq!(array.value_opt(x), Some("yql"));
        }
    }
}
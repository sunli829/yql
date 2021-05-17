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

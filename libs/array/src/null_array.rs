use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::{Array, ArrayRef, DataType};

/// An Array where all elements are nulls.
#[derive(Clone)]
pub struct NullArray {
    len: usize,
}

impl Debug for NullArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut ls = f.debug_list();
        for _ in 0..self.len {
            ls.entry(&());
        }
        ls.finish()
    }
}

impl Array for NullArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn data_type(&self) -> DataType {
        DataType::Null
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn slice(&self, _offset: usize, length: usize) -> ArrayRef {
        Arc::new(NullArray { len: length })
    }

    #[inline]
    fn is_valid(&self, _index: usize) -> bool {
        false
    }

    #[inline]
    fn null_count(&self) -> usize {
        self.len
    }
}

impl PartialEq for NullArray {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
    }
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        Self { len }
    }

    pub fn concat(&self, other: &Self) -> Self {
        Self {
            len: self.len + other.len,
        }
    }
}

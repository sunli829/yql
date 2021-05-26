use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use serde::de::{Error, SeqAccess, Unexpected, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::array::{Array, ArrayRef, DataType};
use std::prelude::rust_2015::Result::Err;

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

impl Serialize for NullArray {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for _ in 0..self.len {
            seq.serialize_element(&())?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for NullArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(NullArrayVisitor(0))
    }
}

struct NullArrayVisitor(usize);

impl<'de> Visitor<'de> for NullArrayVisitor {
    type Value = usize;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("NullArray")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, <A as SeqAccess<'de>>::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(()) = seq.next_element::<()>()? {
            self.0 += 1;
        }
        Ok(self.0)
    }
}

use std::fmt::{self, Formatter};

use serde::de::{DeserializeSeed, Error, SeqAccess, Unexpected, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::array::{ArrayRef, DataType, Int8Type, NullArray, PrimitiveArray};
use crate::dataset::{DataSet, Field, SchemaRef};

impl Serialize for DataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(2)?;
        tuple.serialize_element(&self.schema())?;
        tuple.serialize_element(self.columns())?;
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for DataSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, DataSetVisitor)
    }
}

struct DataSetVisitor;

impl<'de> Visitor<'de> for DataSetVisitor {
    type Value = DataSet;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("DataSet")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let schema = seq
            .next_element::<SchemaRef>()?
            .ok_or_else(|| Error::custom("invalid dataset"))?;
        let columns = seq
            .next_element_seed::<DeColumns>(DeColumns {
                schema: schema.clone(),
                columns: Vec::with_capacity(schema.fields().len()),
            })?
            .ok_or_else(|| Error::custom("invalid dataset"))?;

        todo!()
    }
}

struct DeColumns {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}

impl<'de> DeserializeSeed<'de> for DeColumns {
    type Value = Vec<ArrayRef>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayVisitor(self.schema.fields()))
    }
}

struct ArrayVisitor<'a>(&'a [Field]);

impl<'de, 'a> Visitor<'de> for ArrayVisitor<'a> {
    type Value = ArrayRef;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("Columns")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, <A as SeqAccess<'de>>::Error>
    where
        A: SeqAccess<'de>,
    {
        while !self.0.is_empty() {
            let (field, tail) = self.0.split_first().unwrap();

            match field.data_type {
                DataType::Null => {
                    seq.next_element::<NullArray>()?;
                }
                DataType::Int8 => {}
                DataType::Int16 => {}
                DataType::Int32 => {}
                DataType::Int64 => {}
                DataType::Float32 => {}
                DataType::Float64 => {}
                DataType::Boolean => {}
                DataType::Timestamp(_) => {}
                DataType::String => {}
            }

            self.0 = tail;
        }
        todo!()
    }
}

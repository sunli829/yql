use std::fmt::{self, Formatter};
use std::sync::Arc;

use serde::de::{DeserializeSeed, Error, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::array::{
    ArrayRef, BooleanArray, DataType, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, TimestampArray,
};
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
            })?
            .ok_or_else(|| Error::custom("invalid dataset"))?;
        Ok(DataSet::try_new(schema, columns).map_err(|err| Error::custom(err))?)
    }
}

struct DeColumns {
    schema: SchemaRef,
}

impl<'de> DeserializeSeed<'de> for DeColumns {
    type Value = Vec<ArrayRef>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ColumnsVisitor(self.schema.fields()))
    }
}

struct ColumnsVisitor<'a>(&'a [Field]);

impl<'de, 'a> Visitor<'de> for ColumnsVisitor<'a> {
    type Value = Vec<ArrayRef>;

    fn expecting(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("Columns")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, <A as SeqAccess<'de>>::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut columns = Vec::with_capacity(self.0.len());

        while !self.0.is_empty() {
            let (field, tail) = self.0.split_first().unwrap();

            match field.data_type {
                DataType::Null => columns.push(Arc::new(
                    seq.next_element::<NullArray>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Int8 => columns.push(Arc::new(
                    seq.next_element::<Int8Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Int16 => columns.push(Arc::new(
                    seq.next_element::<Int16Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Int32 => columns.push(Arc::new(
                    seq.next_element::<Int32Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Int64 => columns.push(Arc::new(
                    seq.next_element::<Int64Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Float32 => columns.push(Arc::new(
                    seq.next_element::<Float32Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Float64 => columns.push(Arc::new(
                    seq.next_element::<Float64Array>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Boolean => columns.push(Arc::new(
                    seq.next_element::<BooleanArray>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::Timestamp(_) => columns.push(Arc::new(
                    seq.next_element::<TimestampArray>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
                DataType::String => columns.push(Arc::new(
                    seq.next_element::<StringArray>()?
                        .ok_or_else(|| Error::custom("expect array"))?,
                ) as ArrayRef),
            }

            self.0 = tail;
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::Schema;

    #[test]
    fn test_serde() {
        let fields = vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::String),
            Field::new("c", DataType::Timestamp(None)),
        ];
        let schema = Arc::new(Schema::try_new(fields).unwrap());

        let columns = vec![
            Arc::new(Int32Array::from_vec(vec![1, 3, 5, 7, 9])) as ArrayRef,
            Arc::new(StringArray::from_vec(vec!["a", "b", "c", "d", "e"])),
            Arc::new(TimestampArray::from_vec(vec![111, 333, 555, 777, 999])),
        ];
        let dataset = DataSet::try_new(schema, columns).unwrap();

        let data = bincode::serialize(&dataset).unwrap();
        let dataset2: DataSet = bincode::deserialize(&data).unwrap();
        assert_eq!(dataset, dataset2);
    }
}

use std::sync::Arc;

use anyhow::Result;
use serde_json::{Map, Value};

use crate::array::{
    ArrayRef, BooleanType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, NullArray, PrimitiveBuilder, PrimitiveType, StringBuilder, TimestampType,
};
use crate::dataset::{DataSet, SchemaRef};

macro_rules! parse_integer {
    ($field:expr, $rows:expr, $columns:expr, $ty:ty) => {{
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity($rows.len());
        for row in &$rows {
            if let Some(value) = row.get(&$field.name) {
                match value.as_i64() {
                    Some(n) => {
                        if n >= <<$ty as PrimitiveType>::Native>::MIN as i64
                            && n <= <<$ty as PrimitiveType>::Native>::MAX as i64
                        {
                            builder.append(n as <$ty as PrimitiveType>::Native);
                        } else {
                            anyhow::bail!(
                                "value of field '{}' has overflowed: expect datatype is {}, actual value is {}",
                                    $field.name, <$ty>::DATA_TYPE, value.to_string(),
                            );
                        }
                    }
                    None => {
                        anyhow::bail!(
                            "failed to parse field '{}': expect datatype is {}, actual value is '{}'",
                            $field.name, <$ty>::DATA_TYPE, value.to_string(),
                        );
                    }
                }
            }
        }
        $columns.push(Arc::new(builder.finish()));
    }}
}

macro_rules! parse_float {
    ($field:expr, $rows:expr, $columns:expr, $ty:ty) => {{
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity($rows.len());
        for row in &$rows {
            if let Some(value) = row.get(&$field.name) {
                match value.as_f64() {
                    Some(n) => {
                        if n >= <<$ty as PrimitiveType>::Native>::MIN as f64
                            && n <= <<$ty as PrimitiveType>::Native>::MAX as f64
                        {
                            builder.append(n as <$ty as PrimitiveType>::Native);
                        } else {
                            anyhow::bail!(
                                "value of field '{}' has overflowed: expect datatype is {}, actual value is {}",
                                    $field.name, <$ty>::DATA_TYPE, value.to_string(),
                            );
                        }
                    }
                    None => {
                        anyhow::bail!(
                            "failed to parse field '{}': expect datatype is {}, actual value is '{}'",
                            $field.name, <$ty>::DATA_TYPE, value.to_string(),
                        );
                    }
                }
            }
        }
        $columns.push(Arc::new(builder.finish()));
    }}
}

pub fn parse_json(schema: SchemaRef, data: &[u8]) -> Result<DataSet> {
    let rows = serde_json::from_slice::<Vec<Map<String, Value>>>(data)?;
    let mut columns = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        match field.data_type {
            DataType::Null => columns.push(Arc::new(NullArray::new(rows.len())) as ArrayRef),
            DataType::Int8 => parse_integer!(field, rows, columns, Int8Type),
            DataType::Int16 => parse_integer!(field, rows, columns, Int16Type),
            DataType::Int32 => parse_integer!(field, rows, columns, Int32Type),
            DataType::Int64 => parse_integer!(field, rows, columns, Int64Type),
            DataType::Float32 => parse_float!(field, rows, columns, Float32Type),
            DataType::Float64 => parse_float!(field, rows, columns, Float64Type),
            DataType::Boolean => {
                let mut builder = PrimitiveBuilder::<BooleanType>::with_capacity(rows.len());
                for row in &rows {
                    if let Some(value) = row.get(&field.name) {
                        match value.as_bool() {
                            Some(n) => builder.append(n),
                            None => {
                                anyhow::bail!(
                                    "failed to parse field '{}': expect datatype is {}, actual value is '{}'",
                                    field.name, BooleanType::DATA_TYPE, value.to_string(),
                                );
                            }
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Timestamp(_) => parse_integer!(field, rows, columns, TimestampType),
            DataType::String => {
                let mut builder = StringBuilder::with_capacity(rows.len());
                for row in &rows {
                    if let Some(value) = row.get(&field.name) {
                        match value.as_str() {
                            Some(n) => builder.append(n),
                            None => {
                                anyhow::bail!(
                                    "failed to parse field '{}': expect datatype is {}, actual value is '{}'",
                                    field.name, DataType::String, value.to_string(),
                                );
                            }
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    DataSet::try_new(schema, columns)
}

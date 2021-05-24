use std::sync::Arc;

use anyhow::Result;

use crate::array::{
    Array, ArrayExt, ArrayRef, BooleanType, DataType, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, PrimitiveArray, PrimitiveBuilder, PrimitiveType, StringArray,
    StringBuilder,
};

macro_rules! numeric_array_cast {
    ($array:expr, $from:ty, $to:ty) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$from>>();
        if let Some(scalar) = array.to_scalar() {
            return Ok(Arc::new(PrimitiveArray::<$to>::new_scalar(
                array.len(),
                scalar.map(|value| value as <$to as PrimitiveType>::Native),
            )));
        }
        let mut builder = PrimitiveBuilder::<$to>::with_capacity($array.len());
        for value in array.iter() {
            builder.append(value as <$to as PrimitiveType>::Native);
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! array_cast_to_string {
    ($array:expr, $from:ty) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$from>>();
        if let Some(scalar) = array.to_scalar() {
            return Ok(Arc::new(StringArray::new_scalar(
                array.len(),
                scalar.map(|value| format!("{}", value)),
            )));
        }
        let mut builder = StringBuilder::with_capacity($array.len());
        for value in array.iter() {
            builder.append(&format!("{}", value));
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub fn array_cast_to(array: ArrayRef, data_type: DataType) -> Result<ArrayRef> {
    use DataType::*;

    match (array.data_type(), data_type) {
        (Int8, Int8) => Ok(array.clone()),
        (Int8, Int16) => numeric_array_cast!(array, Int8Type, Int16Type),
        (Int8, Int32) => numeric_array_cast!(array, Int8Type, Int32Type),
        (Int8, Int64) => numeric_array_cast!(array, Int8Type, Int64Type),
        (Int8, Float32) => numeric_array_cast!(array, Int8Type, Float32Type),
        (Int8, Float64) => numeric_array_cast!(array, Int8Type, Float64Type),
        (Int8, String) => array_cast_to_string!(array, Int8Type),

        (Int16, Int16) => Ok(array.clone()),
        (Int16, Int32) => numeric_array_cast!(array, Int16Type, Int32Type),
        (Int16, Int64) => numeric_array_cast!(array, Int16Type, Int64Type),
        (Int16, Float32) => numeric_array_cast!(array, Int16Type, Float32Type),
        (Int16, Float64) => numeric_array_cast!(array, Int16Type, Float64Type),
        (Int16, String) => array_cast_to_string!(array, Int16Type),

        (Int32, Int32) => Ok(array.clone()),
        (Int32, Int64) => numeric_array_cast!(array, Int32Type, Int64Type),
        (Int32, Float32) => numeric_array_cast!(array, Int32Type, Float32Type),
        (Int32, Float64) => numeric_array_cast!(array, Int32Type, Float64Type),
        (Int32, String) => array_cast_to_string!(array, Int32Type),

        (Int64, Int64) => Ok(array.clone()),
        (Int64, Float32) => numeric_array_cast!(array, Int64Type, Float32Type),
        (Int64, Float64) => numeric_array_cast!(array, Int64Type, Float64Type),
        (Int64, String) => array_cast_to_string!(array, Int64Type),

        (Float32, Float32) => Ok(array.clone()),
        (Float32, Float64) => numeric_array_cast!(array, Float32Type, Float64Type),
        (Float32, String) => array_cast_to_string!(array, Float32Type),

        (Float64, Float64) => Ok(array.clone()),
        (Float64, String) => array_cast_to_string!(array, Float64Type),

        (Boolean, Boolean) => Ok(array.clone()),
        (Boolean, String) => array_cast_to_string!(array, BooleanType),

        (Timestamp(_), Timestamp(_)) => Ok(array.clone()),

        (String, String) => Ok(array.clone()),

        _ => anyhow::bail!(
            "cannot cast type from '{}' to '{}'",
            array.data_type(),
            data_type
        ),
    }
}

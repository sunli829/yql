use std::sync::Arc;

use crate::array::{
    Array, ArrayExt, ArrayRef, BooleanArray, BooleanType, DataType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, NullArray, PrimitiveArray, PrimitiveBuilder,
    StringArray, StringBuilder, TimestampType,
};

macro_rules! filter_primitive_array {
    ($array:expr, $flags:expr, $ty:ty) => {{
        let input = $array.downcast_ref::<PrimitiveArray<$ty>>();
        if let Some(scalar) = input.to_scalar() {
            return Arc::new(PrimitiveArray::<$ty>::new_scalar(
                $flags.iter().filter(|x| *x).count(),
                scalar,
            ));
        }
        let mut builder = PrimitiveBuilder::<$ty>::default();
        for (value, flag) in input.iter_opt().zip($flags.iter()) {
            if flag {
                builder.append_opt(value);
            }
        }
        Arc::new(builder.finish())
    }};
}

pub fn filter(array: ArrayRef, flags: &BooleanArray) -> ArrayRef {
    assert_eq!(array.len(), flags.len());

    match array.data_type() {
        DataType::Null => Arc::new(NullArray::new(flags.iter().filter(|x| *x).count())),
        DataType::Int8 => filter_primitive_array!(array, flags, Int8Type),
        DataType::Int16 => filter_primitive_array!(array, flags, Int16Type),
        DataType::Int32 => filter_primitive_array!(array, flags, Int32Type),
        DataType::Int64 => filter_primitive_array!(array, flags, Int64Type),
        DataType::Float32 => filter_primitive_array!(array, flags, Float32Type),
        DataType::Float64 => filter_primitive_array!(array, flags, Float64Type),
        DataType::Boolean => filter_primitive_array!(array, flags, BooleanType),
        DataType::Timestamp(_) => filter_primitive_array!(array, flags, TimestampType),
        DataType::String => {
            let input = array.downcast_ref::<StringArray>();
            if let Some(scalar) = input.to_scalar() {
                return Arc::new(StringArray::new_scalar(
                    flags.iter().filter(|x| *x).count(),
                    scalar,
                ));
            }
            let mut builder = StringBuilder::default();
            for (value, flag) in input.iter_opt().zip(flags.iter()) {
                if flag {
                    builder.append_opt(value);
                }
            }
            Arc::new(builder.finish())
        }
    }
}

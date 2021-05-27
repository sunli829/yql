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

#[cfg(test)]
mod tests {
    use crate::array::{BooleanBuilder, Int32Array, Int32Builder};

    use super::*;

    fn create_bool_array() -> BooleanArray {
        let mut builder = BooleanBuilder::default();
        (0..1000).for_each(|x| builder.append(x % 2 == 0));
        builder.finish()
    }

    fn create_i32_array() -> ArrayRef {
        let mut builder = Int32Builder::default();
        (0..1000).for_each(|x| builder.append(x));
        Arc::new(builder.finish())
    }

    fn create_scalar_i32_array() -> ArrayRef {
        Arc::new(Int32Array::new_scalar(1000, Some(1)))
    }

    fn map_to_char(x: usize) -> char {
        ((x % 58) + 65) as u8 as char
    }

    fn map_to_string(x: usize) -> String {
        map_to_char(x).to_string()
    }

    fn create_string_array() -> ArrayRef {
        let mut builder = StringBuilder::default();
        (0..1000).for_each(|x| builder.append(map_to_string(x).as_str()));
        Arc::new(builder.finish())
    }

    fn create_scalar_string_array() -> ArrayRef {
        Arc::new(StringArray::new_scalar(1000, Some("hello")))
    }


    #[test]
    fn test_filter_i32_array() {
        let array_i32 = create_i32_array();
        let array_bool = create_bool_array();
        let array = filter(array_i32, &array_bool);
        assert_eq!(array.len(), 500);

        let array_i32 = array.downcast_ref::<Int32Array>();
        for x in 0..array.len() {
            assert_eq!(array_i32.value_opt(x), Some(x as i32 * 2));
        }

        let mut builder = Int32Builder::default();
        (0..1000).step_by(2).for_each(|x| builder.append(x));
        let array_i32_2 = builder.finish();
        assert!(array_i32.eq(&array_i32_2));
    }

    #[test]
    fn test_filter_scalar_i32_array() {
        let scalar_array_i32 = create_scalar_i32_array();
        let array_bool = create_bool_array();
        let scalar_array_i32 = filter(scalar_array_i32, &array_bool);
        assert_eq!(scalar_array_i32.len(), 500);

        let scalar_array_i32 = scalar_array_i32.downcast_ref::<Int32Array>();
        for x in 0..scalar_array_i32.len() {
            assert_eq!(scalar_array_i32.value_opt(x), Some(1));
        }

        let scalar_array_i32_2 = Int32Array::new_scalar(500, Some(1));
        assert!(scalar_array_i32.eq(&scalar_array_i32_2));

        let mut builder = Int32Builder::default();
        (0..500).for_each(|_| builder.append(1));
        let scalar_array_i32_2 = builder.finish();
        assert!(scalar_array_i32.eq(&scalar_array_i32_2));
    }

    #[test]
    fn test_filter_string_array() {
        let array_string = create_string_array();
        let array_bool = create_bool_array();
        let array = filter(array_string, &array_bool);
        assert_eq!(array.len(), 500);

        let array_string = array.downcast_ref::<StringArray>();
        for x in 0..array.len() {
            assert_eq!(array_string.value_opt(x), Some(map_to_string(x * 2).as_str()));
        }

        let mut builder = StringBuilder::default();
        (0..1000).step_by(2).for_each(|x| builder.append(&map_to_string(x)));
        let array_string_2 = builder.finish();
        assert!(array_string.eq(&array_string_2));
    }

    #[test]
    fn test_filter_scalar_string_array() {
        let scalar_array_string = create_scalar_string_array();
        let array_bool = create_bool_array();
        let array = filter(scalar_array_string, &array_bool);
        assert_eq!(array.len(), 500);

        let scalar_array_string = array.downcast_ref::<StringArray>();
        for x in 0..scalar_array_string.len() {
            assert_eq!(scalar_array_string.value_opt(x), Some("hello"));
        }


        let scalar_array_string_2 = StringArray::new_scalar(500, Some("hello"));
        assert!(scalar_array_string.eq(&scalar_array_string_2));

        let mut builder = StringBuilder::default();
        (0..500).for_each(|_| builder.append("hello"));
        let scalar_array_string_2 = builder.finish();
        assert!(scalar_array_string.eq(&scalar_array_string_2));
    }


    #[test]
    #[should_panic]
    fn test_filter_mismatch_len_panic() {
        let array_i32 = create_i32_array();
        let array_bool = BooleanArray::from_vec(vec![true, true, true]);
        let _array = filter(array_i32, &array_bool);
    }
}

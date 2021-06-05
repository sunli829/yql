use std::sync::Arc;

use crate::array::{
    ArrayExt, BooleanType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, NullArray, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    TimestampType,
};
use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

macro_rules! coalesce {
    ($args:expr, $ty:ty) => {{
        let len = $args[0].len();
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity(len);

        for row in 0..len {
            let mut value = None;
            for col in $args {
                if let Some(v) = col.downcast_ref::<PrimitiveArray<$ty>>().value_opt(row) {
                    value = Some(v);
                    break;
                }
            }
            builder.append_opt(value);
        }

        Ok(Arc::new(builder.finish()))
    }};
}

pub const COALESCE: Function = Function {
    namespace: None,
    name: "coalesce",
    signature: &Signature::VariadicEqual,
    return_type: |args| args[0],
    function_type: FunctionType::Stateless(|args| match args[0].data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(args[0].len()))),
        DataType::Int8 => coalesce!(args, Int8Type),
        DataType::Int16 => coalesce!(args, Int16Type),
        DataType::Int32 => coalesce!(args, Int32Type),
        DataType::Int64 => coalesce!(args, Int64Type),
        DataType::Float32 => coalesce!(args, Float32Type),
        DataType::Float64 => coalesce!(args, Float64Type),
        DataType::Boolean => coalesce!(args, BooleanType),
        DataType::Timestamp(_) => coalesce!(args, TimestampType),
        DataType::String => {
            let len = args[0].len();
            let mut builder = StringBuilder::with_capacity(len);

            for row in 0..len {
                let mut value = None;
                for col in args {
                    if let Some(v) = col.downcast_ref::<StringArray>().value_opt(row) {
                        value = Some(v);
                        break;
                    }
                }
                builder.append_opt(value);
            }

            Ok(Arc::new(builder.finish()))
        }
    }),
};

macro_rules! ifnull {
    ($args:expr, $ty:ty) => {{
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity($args[0].len());
        for (a, b) in $args[0]
            .downcast_ref::<PrimitiveArray<$ty>>()
            .iter_opt()
            .zip($args[1].downcast_ref::<PrimitiveArray<$ty>>().iter_opt())
        {
            builder.append_opt(a.or(b));
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub const IFNULL: Function = Function {
    namespace: None,
    name: "ifnull",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[DataType::Null, DataType::Null]),
        Signature::Exact(&[DataType::Int8, DataType::Int8]),
        Signature::Exact(&[DataType::Int16, DataType::Int16]),
        Signature::Exact(&[DataType::Int32, DataType::Int32]),
        Signature::Exact(&[DataType::Int64, DataType::Int64]),
        Signature::Exact(&[DataType::Float32, DataType::Float32]),
        Signature::Exact(&[DataType::Float64, DataType::Float64]),
        Signature::Exact(&[DataType::Boolean, DataType::Boolean]),
        Signature::Exact(&[DataType::Timestamp(None), DataType::Timestamp(None)]),
        Signature::Exact(&[DataType::String, DataType::String]),
    ]),
    return_type: |args| args[0],
    function_type: FunctionType::Stateless(|args| match args[0].data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(args[0].len()))),
        DataType::Int8 => ifnull!(args, Int8Type),
        DataType::Int16 => ifnull!(args, Int16Type),
        DataType::Int32 => ifnull!(args, Int32Type),
        DataType::Int64 => ifnull!(args, Int64Type),
        DataType::Float32 => ifnull!(args, Float32Type),
        DataType::Float64 => ifnull!(args, Float64Type),
        DataType::Boolean => ifnull!(args, BooleanType),
        DataType::Timestamp(_) => ifnull!(args, TimestampType),
        DataType::String => {
            let mut builder = StringBuilder::with_capacity(args[0].len());
            for (a, b) in args[0]
                .downcast_ref::<StringArray>()
                .iter_opt()
                .zip(args[1].downcast_ref::<StringArray>().iter_opt())
            {
                builder.append_opt(a.or(b));
            }
            Ok(Arc::new(builder.finish()))
        }
    }),
};

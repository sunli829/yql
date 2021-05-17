use std::sync::Arc;

use yql_array::{
    Array, ArrayExt, BooleanType, DataType, Float32Type, Float64Array, Float64Builder, Float64Type,
    Int16Type, Int32Type, Int64Builder, Int64Type, Int8Type, NullArray, PrimitiveArray,
    PrimitiveBuilder, Scalar, StringArray, StringBuilder, TimestampType,
};

use crate::func::{AggregateFunction, Function, FunctionType};
use crate::signature::Signature;

pub const AVG: Function = Function {
    name: "avg",
    signature: &Signature::Uniform(1, &[DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(AggregateFunction::<(f64, f64)>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let mut builder = Float64Builder::with_capacity(array.len());
            for value in array.iter_opt() {
                if let Some(value) = value {
                    state.0 += value;
                    state.1 += 1.0;
                }
                builder.append(state.0 / state.1);
            }
            Ok(Arc::new(builder.finish()))
        }))
    }),
};

pub const SUM: Function = Function {
    name: "sum",
    signature: &Signature::Uniform(1, &[DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(AggregateFunction::<f64>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let mut builder = Float64Builder::with_capacity(array.len());
            for value in array.iter_opt() {
                if let Some(value) = value {
                    *state += value;
                }
                builder.append(*state);
            }
            Ok(Arc::new(builder.finish()))
        }))
    }),
};

pub const COUNT: Function = Function {
    name: "count",
    signature: &Signature::Any(1),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(AggregateFunction::<i64>::new(|state, args| {
            let array = &args[0];
            let mut builder = Int64Builder::with_capacity(array.len());
            for i in 0..args[0].len() {
                if args[0].is_valid(i) {
                    *state += 1;
                }
                builder.append(*state);
            }
            Ok(Arc::new(builder.finish()))
        }))
    }),
};

macro_rules! max_min {
    ($array:expr, $state:expr, $ty:ty, $scalar_ty:ident, $func:ident) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity(array.len());
        for value in array.iter_opt() {
            if let Some(value) = value {
                match $state {
                    Scalar::$scalar_ty(state) => {
                        let current_value = (*state).$func(value);
                        builder.append(current_value);
                        *$state = Scalar::$scalar_ty(current_value);
                    }
                    _ => {
                        builder.append(value);
                        *$state = Scalar::$scalar_ty(value);
                    }
                };
            } else {
                match $state {
                    Scalar::$scalar_ty(value) => builder.append(*value),
                    _ => builder.append_null(),
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}
//
macro_rules! make_max_min_func {
    ($ident:ident, $name:literal, $func:ident) => {
        pub const $ident: Function = Function {
            name: $name,
            signature: &Signature::Uniform(
                1,
                &[
                    DataType::Float64,
                    DataType::Float32,
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Int16,
                    DataType::Int8,
                ],
            ),
            return_type: |args| args[0],
            function_type: FunctionType::Stateful(|| {
                Box::new(AggregateFunction::<Scalar>::new(|state, args| {
                    let array = &args[0];
                    match array.data_type() {
                        DataType::Float64 => {
                            max_min!(array, state, Float64Type, Float64, $func)
                        }
                        DataType::Float32 => {
                            max_min!(array, state, Float32Type, Float32, $func)
                        }
                        DataType::Int64 => {
                            max_min!(array, state, Int64Type, Int64, $func)
                        }
                        DataType::Int32 => {
                            max_min!(array, state, Int32Type, Int32, $func)
                        }
                        DataType::Int16 => {
                            max_min!(array, state, Int16Type, Int16, $func)
                        }
                        DataType::Int8 => {
                            max_min!(array, state, Int8Type, Int8, $func)
                        }
                        _ => unreachable!(),
                    }
                }))
            }),
        };
    };
}

make_max_min_func!(MAX, "max", max);
make_max_min_func!(MIN, "min", min);

macro_rules! first_value {
    ($array:expr, $state:expr, $ty:ty, $scalar_ty:ident) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity(array.len());
        for value in array.iter_opt() {
            match value {
                Some(value) => match $state {
                    Scalar::$scalar_ty(first_value) => {
                        builder.append(*first_value);
                    }
                    _ => {
                        *$state = Scalar::$scalar_ty(value);
                        builder.append(value);
                    }
                },
                None => {
                    if let Scalar::$scalar_ty(first_value) = $state {
                        builder.append(*first_value);
                    } else {
                        builder.append_null();
                    }
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub const FIRST: Function = Function {
    name: "first",
    signature: &Signature::Any(1),
    return_type: |args| args[0],
    function_type: FunctionType::Stateful(|| {
        Box::new(AggregateFunction::<Scalar>::new(|state, args| {
            let array = &args[0];
            match array.data_type() {
                DataType::Null => Ok(Arc::new(NullArray::new(array.len()))),
                DataType::Int8 => first_value!(array, state, Int8Type, Int8),
                DataType::Int16 => first_value!(array, state, Int16Type, Int16),
                DataType::Int32 => first_value!(array, state, Int32Type, Int32),
                DataType::Int64 => first_value!(array, state, Int64Type, Int64),
                DataType::Float32 => first_value!(array, state, Float32Type, Float32),
                DataType::Float64 => first_value!(array, state, Float64Type, Float64),
                DataType::Boolean => first_value!(array, state, BooleanType, Boolean),
                DataType::Timestamp(_) => first_value!(array, state, TimestampType, Timestamp),
                DataType::String => {
                    let array = array.downcast_ref::<StringArray>();
                    let mut builder = StringBuilder::with_capacity(array.len());
                    for value in array.iter_opt() {
                        match value {
                            Some(value) => match state {
                                Scalar::String(first_value) => {
                                    builder.append(first_value);
                                }
                                _ => {
                                    *state = Scalar::String(value.into());
                                    builder.append(value);
                                }
                            },
                            None => {
                                if let Scalar::String(first_value) = state {
                                    builder.append(first_value);
                                } else {
                                    builder.append_null();
                                }
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }))
    }),
};

macro_rules! last_value {
    ($array:expr, $state:expr, $ty:ty, $scalar_ty:ident) => {{
        let array = $array.downcast_ref::<PrimitiveArray<$ty>>();
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity(array.len());
        for value in array.iter_opt() {
            match value {
                Some(value) => {
                    *$state = Scalar::$scalar_ty(value);
                    builder.append(value);
                }
                None => {
                    if let Scalar::$scalar_ty(first_value) = $state {
                        builder.append(*first_value);
                    } else {
                        builder.append_null();
                    }
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub const LAST: Function = Function {
    name: "last",
    signature: &Signature::Any(1),
    return_type: |args| args[0],
    function_type: FunctionType::Stateful(|| {
        Box::new(AggregateFunction::<Scalar>::new(|state, args| {
            let array = &args[0];
            match array.data_type() {
                DataType::Null => Ok(Arc::new(NullArray::new(array.len()))),
                DataType::Int8 => last_value!(array, state, Int8Type, Int8),
                DataType::Int16 => last_value!(array, state, Int16Type, Int16),
                DataType::Int32 => last_value!(array, state, Int32Type, Int32),
                DataType::Int64 => last_value!(array, state, Int64Type, Int64),
                DataType::Float32 => last_value!(array, state, Float32Type, Float32),
                DataType::Float64 => last_value!(array, state, Float64Type, Float64),
                DataType::Boolean => last_value!(array, state, BooleanType, Boolean),
                DataType::Timestamp(_) => last_value!(array, state, TimestampType, Timestamp),
                DataType::String => {
                    let array = array.downcast_ref::<StringArray>();
                    let mut builder = StringBuilder::with_capacity(array.len());
                    for value in array.iter_opt() {
                        match value {
                            Some(value) => {
                                *state = Scalar::String(value.into());
                                builder.append(value);
                            }
                            None => {
                                if let Scalar::String(last_value) = state {
                                    builder.append(last_value);
                                } else {
                                    builder.append_null();
                                }
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }))
    }),
};

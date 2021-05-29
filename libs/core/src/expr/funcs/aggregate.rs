use std::sync::Arc;

use crate::array::{
    Array, ArrayExt, BooleanType, DataType, Float32Type, Float64Array, Float64Builder, Float64Type,
    Int16Type, Int32Type, Int64Builder, Int64Type, Int8Type, NullArray, PrimitiveArray,
    PrimitiveBuilder, Scalar, StringArray, StringBuilder, TimestampType,
};
use crate::expr::func::{Function, FunctionType, StatefulFunction};
use crate::expr::signature::Signature;

pub const AVG: Function = Function {
    namespace: None,
    name: "avg",
    signature: &Signature::Exact(&[DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<(f64, f64)>::new(|state, args| {
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
    namespace: None,
    name: "sum",
    signature: &Signature::Exact(&[DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<f64>::new(|state, args| {
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
    namespace: None,
    name: "count",
    signature: &Signature::Any(1),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<i64>::new(|state, args| {
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

macro_rules! make_max_min_func {
    ($ident:ident, $name:literal, $func:ident) => {
        pub const $ident: Function = Function {
            namespace: None,
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
                Box::new(StatefulFunction::<Scalar>::new(|state, args| {
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
    namespace: None,
    name: "first",
    signature: &Signature::Any(1),
    return_type: |args| args[0],
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<Scalar>::new(|state, args| {
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
    namespace: None,
    name: "last",
    signature: &Signature::Any(1),
    return_type: |args| args[0],
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<Scalar>::new(|state, args| {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_unary_func(func: &Function, first: (Vec<f64>, Vec<f64>), second: (Vec<f64>, Vec<f64>)) {
        let mut f = func.function_type.create_stateful_fun();
        let array = f
            .call(&[Arc::new(Float64Array::from_vec(first.0))])
            .unwrap();
        assert_eq!(
            array
                .downcast_ref::<Float64Array>()
                .iter()
                .collect::<Vec<_>>(),
            first.1
        );

        let state = f.save_state().unwrap();
        let mut f = func.function_type.create_stateful_fun();
        f.load_state(state).unwrap();

        let array = f
            .call(&[Arc::new(Float64Array::from_vec(second.0))])
            .unwrap();
        assert_eq!(
            array
                .downcast_ref::<Float64Array>()
                .iter()
                .collect::<Vec<_>>(),
            second.1
        );
    }

    #[test]
    fn test_avg() {
        test_unary_func(
            &AVG,
            (vec![1.0, 2.0, 3.0], vec![1.0, 1.5, 2.0]),
            (vec![4.0, 5.0, 6.0], vec![2.5, 3.0, 3.5]),
        );
    }

    #[test]
    fn test_sum() {
        test_unary_func(
            &SUM,
            (vec![1.0, 2.0, 3.0], vec![1.0, 3.0, 6.0]),
            (vec![4.0, 5.0, 6.0], vec![10.0, 15.0, 21.0]),
        );
    }

    #[test]
    fn test_max() {
        test_unary_func(
            &MAX,
            (vec![10.0, 5.0, 30.0], vec![10.0, 10.0, 30.0]),
            (vec![7.0, 20.0, 35.0], vec![30.0, 30.0, 35.0]),
        );
    }

    #[test]
    fn test_min() {
        test_unary_func(
            &MIN,
            (vec![10.0, 5.0, 30.0], vec![10.0, 5.0, 5.0]),
            (vec![7.0, 3.0, 35.0], vec![5.0, 3.0, 3.0]),
        );
    }

    #[test]
    fn test_first() {
        test_unary_func(
            &FIRST,
            (vec![10.0, 5.0, 30.0], vec![10.0, 10.0, 10.0]),
            (vec![7.0, 3.0, 35.0], vec![10.0, 10.0, 10.0]),
        );
    }

    #[test]
    fn test_last() {
        test_unary_func(
            &LAST,
            (vec![10.0, 5.0, 30.0], vec![10.0, 5.0, 30.0]),
            (vec![7.0, 3.0, 35.0], vec![7.0, 3.0, 35.0]),
        );
    }
}

use std::sync::Arc;

use yql_dataset::array::{
    Array, ArrayExt, DataType, Float32Array, Float32Builder, Float64Array, Float64Builder,
};

use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

macro_rules! make_math_func {
    ($ident:ident, $name:literal, $func:ident) => {
        pub const $ident: Function = Function {
            name: $name,
            signature: &Signature::Uniform(1, &[DataType::Float64, DataType::Float32]),
            return_type: |args| args[0],
            function_type: FunctionType::Stateless(|args| {
                let array = &args[0];
                match array.data_type() {
                    DataType::Float32 => {
                        let array = array.downcast_ref::<Float32Array>();
                        if let Some(scalar) = array.to_scalar() {
                            return Ok(Arc::new(Float32Array::new_scalar(
                                array.len(),
                                scalar.map(|x| x.$func()),
                            )));
                        }
                        let mut builder = Float32Builder::with_capacity(array.len());
                        for value in array.iter_opt() {
                            match value {
                                Some(value) => builder.append(value.$func()),
                                None => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Float64 => {
                        let array = array.downcast_ref::<Float64Array>();
                        if let Some(scalar) = array.to_scalar() {
                            return Ok(Arc::new(Float64Array::new_scalar(
                                array.len(),
                                scalar.map(|x| x.$func()),
                            )));
                        }
                        let mut builder = Float64Builder::with_capacity(array.len());
                        for value in array.iter_opt() {
                            match value {
                                Some(value) => builder.append(value.$func()),
                                None => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => unreachable!(),
                }
            }),
        };
    };
}

make_math_func!(SQRT, "sqrt", sqrt);
make_math_func!(SIN, "sin", sin);
make_math_func!(COS, "cos", cos);
make_math_func!(TAN, "tan", tan);
make_math_func!(ASIN, "asin", asin);
make_math_func!(ACOS, "acos", acos);
make_math_func!(ATAN, "atan", atan);
make_math_func!(FLOOR, "floor", floor);
make_math_func!(CEIL, "ceil", ceil);
make_math_func!(ROUND, "round", round);
make_math_func!(TRUNC, "trunc", trunc);
make_math_func!(ABS, "abs", abs);
make_math_func!(SIGNUM, "signum", signum);
make_math_func!(EXP, "exp", exp);
make_math_func!(LN, "ln", ln);
make_math_func!(LOG2, "log2", log2);
make_math_func!(LOG10, "log10", log10);

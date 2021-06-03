use std::fmt::Write;
use std::sync::Arc;

use crate::array::{ArrayExt, DataType, Int64Array, StringArray, StringBuilder};
use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

pub const CHR: Function = Function {
    namespace: None,
    name: "chr",
    signature: &Signature::Uniform(1, &[DataType::Int64]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        let array = &args[0];
        let array_i64 = array.downcast_ref::<Int64Array>();
        let mut builder = StringBuilder::with_capacity(array.len());

        for ch in array_i64.iter().map(|x| char::from_u32(x as u32)) {
            match ch {
                Some(ch) => {
                    let mut s = String::new();
                    s.write_char(ch)?;
                    builder.append(&s);
                }
                None => {
                    builder.append_null();
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

pub const CONCAT: Function = Function {
    namespace: None,
    name: "concat",
    signature: &Signature::Variadic(&[DataType::String]),
    return_type: |_| DataType::String,
    function_type: FunctionType::Stateless(|args| {
        anyhow::ensure!(
            args.len() > 0,
            "`concat` function requires at least one argument."
        );
        let len = args[0].len();
        let mut buf = Vec::new();
        let mut builder = StringBuilder::with_capacity(len);

        for row in 0..len {
            buf.clear();

            for col in args {
                if let Some(value) = col.downcast_ref::<StringArray>().value_opt(row) {
                    buf.push(value);
                }
            }

            builder.append(&buf.concat());
        }

        Ok(Arc::new(builder.finish()))
    }),
};

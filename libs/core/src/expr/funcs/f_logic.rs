use std::sync::Arc;

use crate::array::{Array, ArrayExt, BooleanBuilder, DataType, Float64Array};
use crate::expr::func::{Function, FunctionType};
use crate::expr::signature::Signature;

pub const F_BETWEEN: Function = Function {
    namespace: Some("f"),
    name: "between",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Float64, DataType::Float64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateless(|args| {
        let a = args[0].downcast_ref::<Float64Array>();
        let b = args[1].downcast_ref::<Float64Array>();
        let c = args[2].downcast_ref::<Float64Array>();
        let mut builder = BooleanBuilder::with_capacity(a.len());

        for ((a, b), c) in a.iter_opt().zip(b.iter_opt()).zip(c.iter_opt()) {
            if let (Some(a), Some(b), Some(c)) = (a, b, c) {
                builder.append((a > b && a < c) || (a > c && a < b));
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }),
};

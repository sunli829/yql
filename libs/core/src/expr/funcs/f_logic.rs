use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::array::{Array, ArrayExt, BooleanBuilder, DataType, Float64Array, Int64Array};
use crate::expr::func::{Function, FunctionType, StatefulFunction};
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

#[derive(Default, Clone, Serialize, Deserialize)]
struct CrossState {
    prev: Option<(f64, f64)>,
}

pub const F_CROSS: Function = Function {
    namespace: Some("f"),
    name: "cross",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Float64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<CrossState>::new(|state, args| {
            let a = args[0].downcast_ref::<Float64Array>();
            let b = args[1].downcast_ref::<Float64Array>();
            let mut builder = BooleanBuilder::with_capacity(a.len());

            for (a, b) in a.iter().zip(b.iter()) {
                let res = match state.prev {
                    Some((pa, pb)) if pa < pb => Some(a >= b),
                    Some((pa, pb)) if (pa - pb).abs() < f64::EPSILON => Some(a > b),
                    Some(_) => Some(false),
                    None => None,
                };
                builder.append_opt(res);
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct LongCrossState {
    index: usize,
    count: usize,
}

pub const F_LONGCROSS: Function = Function {
    namespace: Some("f"),
    name: "longcross",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Float64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<LongCrossState>::new(|state, args| {
            let a = args[0].downcast_ref::<Float64Array>();
            let b = args[1].downcast_ref::<Float64Array>();
            let n = args[2].downcast_ref::<Int64Array>();
            let mut builder = BooleanBuilder::with_capacity(a.len());

            for ((a, b), n) in a.iter().zip(b.iter()).zip(n.iter()) {
                if n > 0 {
                    let res = if a < b {
                        state.count += 1;
                        false
                    } else {
                        let n = n.max(1);
                        let res = state.count >= n as usize;
                        state.count = 0;
                        res
                    };
                    if state.index < n as usize {
                        builder.append_null();
                    } else {
                        builder.append(res);
                    }
                } else {
                    builder.append_null();
                }

                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

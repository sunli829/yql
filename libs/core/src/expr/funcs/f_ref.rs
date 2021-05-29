use std::collections::VecDeque;
use std::sync::Arc;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::array::{
    ArrayExt, BooleanArray, BooleanBuilder, BooleanType, DataType, Float32Type, Float64Array,
    Float64Builder, Float64Type, Int16Type, Int32Type, Int64Array, Int64Builder, Int64Type,
    Int8Type, PrimitiveArray, PrimitiveBuilder, Scalar, StringArray, StringBuilder, TimestampType,
};
use crate::expr::func::{Function, FunctionType, StatefulFunction};
use crate::expr::funcs::utils::VecDequeExt;
use crate::expr::signature::Signature;

#[derive(Default, Clone, Serialize, Deserialize)]
struct AllState {
    values: VecDeque<bool>,
    failed: bool,
    success_count: usize,
}

pub const F_ALL: Function = Function {
    namespace: Some("f"),
    name: "all",
    signature: &Signature::Exact(&[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<AllState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = BooleanBuilder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    if x {
                        state.success_count += 1;
                    }

                    if let Some(rx) = state.values.push_back_limit(x, n as usize) {
                        if rx {
                            state.success_count -= 1;
                        }
                    }

                    if state.values.len() == n as usize {
                        builder.append(state.success_count == n as usize);
                    } else {
                        builder.append_null();
                    }
                } else {
                    if state.failed {
                        builder.append(false);
                    }

                    let mut new_failed = state.failed;
                    let result = if !x {
                        new_failed = true;
                        false
                    } else {
                        true
                    };
                    state.failed = new_failed;
                    builder.append(result);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct AnyState {
    values: VecDeque<bool>,
    succeeded: bool,
    success_count: usize,
}

pub const F_ANY: Function = Function {
    namespace: Some("f"),
    name: "any",
    signature: &Signature::Exact(&[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<AnyState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = BooleanBuilder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    if x {
                        state.success_count += 1;
                    }

                    if let Some(rx) = state.values.push_back_limit(x, n as usize) {
                        if rx {
                            state.success_count -= 1;
                        }
                    }

                    if state.values.len() == n as usize {
                        builder.append(state.success_count > 0);
                    } else {
                        builder.append_null();
                    }
                } else {
                    if state.succeeded {
                        builder.append(false);
                    }

                    let mut new_succeeded = state.succeeded;
                    let result = if x {
                        new_succeeded = true;
                        true
                    } else {
                        false
                    };
                    state.succeeded = new_succeeded;
                    builder.append(result);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct BarsLastState {
    index: usize,
    prev: Option<usize>,
}

pub const F_BARSLAST: Function = Function {
    namespace: Some("f"),
    name: "barslast",
    signature: &Signature::Exact(&[DataType::Boolean]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<BarsLastState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let mut builder = Int64Builder::default();

            for x in array.iter() {
                match (x, state.prev) {
                    (true, _) => {
                        state.prev = Some(state.index);
                        builder.append(0);
                    }
                    (false, Some(prev)) => builder.append((state.index - prev) as i64),
                    (false, None) => builder.append_null(),
                }
                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct BarsSinceState {
    index: usize,
    prev: Option<usize>,
}

pub const F_BARSSINCE: Function = Function {
    namespace: Some("f"),
    name: "barssince",
    signature: &Signature::Exact(&[DataType::Boolean]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<BarsLastState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let mut builder = Int64Builder::default();

            for x in array.iter() {
                match (x, state.prev) {
                    (_, Some(prev)) => builder.append((state.index - prev) as i64),
                    (true, None) => {
                        state.prev = Some(state.index);
                        builder.append(0);
                    }
                    (false, None) => builder.append_null(),
                }
                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct CountState {
    values: VecDeque<bool>,
    count: usize,
}

pub const F_COUNT: Function = Function {
    namespace: Some("f"),
    name: "count",
    signature: &Signature::Exact(&[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<CountState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Int64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    if x {
                        state.count += 1;
                    }
                    if let Some(rx) = state.values.push_back_limit(x, n as usize) {
                        if rx {
                            state.count -= 1;
                        }
                    }

                    if state.values.len() == n as usize {
                        builder.append(state.count as i64);
                    } else {
                        builder.append_null();
                    }
                } else {
                    if x {
                        state.count += 1;
                    }
                    builder.append(state.count as i64);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct DmaState {
    x: Option<f64>,
}

pub const F_DMA: Function = Function {
    namespace: Some("f"),
    name: "dma",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<DmaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let a = args[1].downcast_ref::<Float64Array>();
            let mut builder = Float64Builder::default();

            for (x, a) in array.iter().zip(a.iter()) {
                let a = if a > 1.0 {
                    1.0
                } else if a < 0.0 {
                    0.0
                } else {
                    a
                };
                let nx = match state.x {
                    Some(px) => a * x + (1.0 - a) * px,
                    None => x,
                };
                state.x = Some(nx);
                builder.append(nx);
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct EmaState {
    x: Option<f64>,
}

pub const F_EMA: Function = Function {
    namespace: Some("f"),
    name: "ema",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<EmaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                let nx = match state.x {
                    Some(px) if n == 0 => px,
                    Some(px) => (x * 2.0 + (n - 1) as f64 * px) / (n + 1) as f64,
                    None => x,
                };
                state.x = Some(nx);
                builder.append(nx);
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct FilterState {
    filter_all: bool,
    count: Option<usize>,
}

pub const F_FILTER: Function = Function {
    namespace: Some("f"),
    name: "filter",
    signature: &Signature::Exact(&[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Boolean,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<FilterState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = BooleanBuilder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    if let Some(count) = &mut state.count {
                        *count -= 1;
                        if *count == 0 {
                            state.count = None;
                        }
                        builder.append(false);
                    } else if x {
                        state.count = Some(n as usize);
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                } else if state.filter_all {
                    builder.append(false);
                } else if x {
                    state.filter_all = true;
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct HhvState {
    max: Option<f64>,
    values: VecDeque<f64>,
}

pub const F_HHV: Function = Function {
    namespace: Some("f"),
    name: "hhv",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<HhvState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    builder.append(
                        state
                            .values
                            .iter()
                            .copied()
                            .map(OrderedFloat)
                            .max()
                            .unwrap()
                            .0,
                    );
                } else {
                    match state.max {
                        Some(max_value) if x > max_value => {
                            state.max = Some(x);
                            builder.append(x);
                        }
                        Some(max_value) => {
                            builder.append(max_value);
                        }
                        None => {
                            state.max = Some(x);
                            builder.append(x);
                        }
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct LlvState {
    min: Option<f64>,
    values: VecDeque<f64>,
}

pub const F_LLV: Function = Function {
    namespace: Some("f"),
    name: "llv",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<LlvState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    builder.append(
                        state
                            .values
                            .iter()
                            .copied()
                            .map(OrderedFloat)
                            .min()
                            .unwrap()
                            .0,
                    );
                } else {
                    match state.min {
                        Some(min_value) if x < min_value => {
                            state.min = Some(x);
                            builder.append(x);
                        }
                        Some(min_value) => {
                            builder.append(min_value);
                        }
                        None => {
                            state.min = Some(x);
                            builder.append(x);
                        }
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct HhvBarsState {
    index: usize,
    max: Option<(f64, usize)>,
    values: VecDeque<f64>,
}

pub const F_HHVBARS: Function = Function {
    namespace: Some("f"),
    name: "hhvbars",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<HhvBarsState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Int64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    let max_idx = state
                        .values
                        .iter()
                        .cloned()
                        .enumerate()
                        .fold(None, |acc, (idx, x)| match acc {
                            Some((max_value, _)) if x > max_value => Some((x, idx)),
                            Some(_) => acc,
                            None => Some((x, idx)),
                        })
                        .unwrap()
                        .1;
                    builder.append((state.values.len() - max_idx - 1) as i64);
                } else {
                    match state.max {
                        Some((max_value, _)) if x > max_value => {
                            state.max = Some((x, state.index));
                            builder.append(0);
                        }
                        Some((_, max_idx)) => {
                            builder.append((state.index - max_idx) as i64);
                        }
                        None => {
                            state.max = Some((x, state.index));
                            builder.append(0);
                        }
                    }
                }

                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct LlvBarsState {
    index: usize,
    min: Option<(f64, usize)>,
    values: VecDeque<f64>,
}

pub const F_LLVBARS: Function = Function {
    namespace: Some("f"),
    name: "llvbars",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<LlvBarsState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Int64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    let min_idx = state
                        .values
                        .iter()
                        .cloned()
                        .enumerate()
                        .fold(None, |acc, (idx, x)| match acc {
                            Some((min_value, _)) if x < min_value => Some((x, idx)),
                            Some(_) => acc,
                            None => Some((x, idx)),
                        })
                        .unwrap()
                        .1;
                    builder.append((state.values.len() - min_idx - 1) as i64);
                } else {
                    match state.min {
                        Some((min_value, _)) if x < min_value => {
                            state.min = Some((x, state.index));
                            builder.append(0);
                        }
                        Some((_, min_idx)) => {
                            builder.append((state.index - min_idx) as i64);
                        }
                        None => {
                            state.min = Some((x, state.index));
                            builder.append(0);
                        }
                    }
                }

                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct LastState {
    count: usize,
}

pub const F_LAST: Function = Function {
    namespace: Some("f"),
    name: "last",
    signature: &Signature::Exact(&[DataType::Boolean]),
    return_type: |_| DataType::Int64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<LastState>::new(|state, args| {
            let array = args[0].downcast_ref::<BooleanArray>();
            let mut builder = Int64Builder::default();

            for x in array.iter() {
                if x {
                    state.count += 1;
                } else {
                    state.count = 0;
                }
                builder.append(state.count as i64);
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct MaState {
    sum: f64,
    sum_bars: usize,
    values: VecDeque<f64>,
}

pub const F_MA: Function = Function {
    namespace: Some("f"),
    name: "ma",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<MaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    if state.values.len() == n as usize {
                        builder.append(state.values.iter().sum::<f64>() / n as f64);
                    } else {
                        builder.append_null();
                    }
                } else {
                    state.sum_bars += 1;
                    state.sum += x;
                    builder.append(state.sum / state.sum_bars as f64);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct MemaState {
    y: Option<f64>,
}

pub const F_MEMA: Function = Function {
    namespace: Some("f"),
    name: "mema",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<MemaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                match state.y {
                    Some(py) => {
                        let new_py = if n >= 1 {
                            (x + (n - 1) as f64 * py) / n as f64
                        } else {
                            py
                        };
                        state.y = Some(new_py);
                        builder.append(new_py);
                    }
                    None => {
                        state.y = Some(x);
                        builder.append(x);
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct RefState {
    values: VecDeque<Scalar>,
}

macro_rules! ref_values {
    ($state:expr, $array:expr, $n:expr, $ty:ty, $scalar_ty:ident) => {{
        let mut builder = PrimitiveBuilder::<$ty>::with_capacity($array.len());
        for (x, n) in $array
            .downcast_ref::<PrimitiveArray<$ty>>()
            .iter_opt()
            .zip($n.iter())
        {
            $state.values.push_back_limit(
                match x {
                    Some(x) => Scalar::$scalar_ty(x),
                    None => Scalar::Null,
                },
                n as usize + 1,
            );
            if $state.values.len() == n as usize + 1 {
                match $state.values.iter().next() {
                    Some(Scalar::$scalar_ty(value)) => builder.append(*value),
                    _ => builder.append_null(),
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub const F_REF: Function = Function {
    namespace: Some("f"),
    name: "ref",
    signature: &Signature::OneOf(&[
        Signature::Exact(&[DataType::Float64, DataType::Int64]),
        Signature::Exact(&[DataType::Float32, DataType::Int64]),
        Signature::Exact(&[DataType::Int64, DataType::Int64]),
        Signature::Exact(&[DataType::Int32, DataType::Int64]),
        Signature::Exact(&[DataType::Int16, DataType::Int64]),
        Signature::Exact(&[DataType::Int8, DataType::Int64]),
        Signature::Exact(&[DataType::Boolean, DataType::Int64]),
        Signature::Exact(&[DataType::Timestamp(None), DataType::Int64]),
        Signature::Exact(&[DataType::String, DataType::Int64]),
    ]),
    return_type: |args| args[0],
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<RefState>::new(|state, args| {
            let array = &args[0];
            let n = args[1].downcast_ref::<Int64Array>();

            match array.data_type() {
                DataType::Null => unreachable!(),
                DataType::Int8 => ref_values!(state, array, n, Int8Type, Int8),
                DataType::Int16 => ref_values!(state, array, n, Int16Type, Int16),
                DataType::Int32 => ref_values!(state, array, n, Int32Type, Int32),
                DataType::Int64 => ref_values!(state, array, n, Int64Type, Int64),
                DataType::Float32 => ref_values!(state, array, n, Float32Type, Float32),
                DataType::Float64 => ref_values!(state, array, n, Float64Type, Float64),
                DataType::Boolean => ref_values!(state, array, n, BooleanType, Boolean),
                DataType::Timestamp(_) => ref_values!(state, array, n, TimestampType, Timestamp),
                DataType::String => {
                    let mut builder = StringBuilder::with_capacity(array.len());
                    for (x, n) in array.downcast_ref::<StringArray>().iter_opt().zip(n.iter()) {
                        state.values.push_back_limit(
                            match x {
                                Some(x) => Scalar::String(x.into()),
                                None => Scalar::Null,
                            },
                            n as usize + 1,
                        );
                        if state.values.len() == n as usize + 1 {
                            match state.values.iter().next() {
                                Some(Scalar::String(value)) => builder.append(value),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct SmaState {
    y: Option<f64>,
}

pub const F_SMA: Function = Function {
    namespace: Some("f"),
    name: "sma",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64, DataType::Float64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<SmaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let m = args[1].downcast_ref::<Float64Array>();
            let mut builder = Float64Builder::default();

            for ((x, n), m) in array.iter().zip(n.iter()).zip(m.iter()) {
                match state.y {
                    Some(py) => {
                        let new_py = if n > 0 {
                            (m * x + (n as f64 - m) * py) / n as f64
                        } else {
                            py
                        };
                        state.y = Some(new_py);
                        builder.append(new_py);
                    }
                    None => {
                        state.y = Some(x);
                        builder.append(0.0);
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct SumState {
    sum: f64,
    values: VecDeque<f64>,
}

pub const F_SUM: Function = Function {
    namespace: Some("f"),
    name: "sum",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<SumState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    if state.values.len() == n as usize {
                        builder.append(state.values.iter().copied().sum::<f64>());
                    } else {
                        builder.append_null();
                    }
                } else {
                    state.sum += x;
                    builder.append(state.sum);
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct WmaState {
    index: usize,
    sum: f64,
    sum_bars: usize,
    values: VecDeque<f64>,
}

pub const F_WMA: Function = Function {
    namespace: Some("f"),
    name: "wma",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<WmaState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n > 0 {
                    state.values.push_back_limit(x, n as usize);
                    if state.values.len() == n as usize {
                        let mut sum = 0.0;
                        let mut sum_bars = 0;
                        for (idx, value) in state.values.iter().copied().enumerate() {
                            sum += (idx + 1) as f64 * value;
                            sum_bars += idx + 1;
                        }
                        builder.append(sum / sum_bars as f64);
                    } else {
                        builder.append_null();
                    }
                } else {
                    let new_sum = state.sum + (state.index + 1) as f64 * x;
                    let new_sum_bars = state.sum_bars + state.index + 1;
                    state.sum = new_sum;
                    state.sum_bars = new_sum_bars;
                    builder.append(new_sum / new_sum_bars as f64);
                }

                state.index += 1;
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

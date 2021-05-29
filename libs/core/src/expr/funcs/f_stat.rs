#![allow(clippy::suspicious_operation_groupings)]

use std::collections::VecDeque;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::array::{ArrayExt, DataType, Float64Array, Float64Builder, Int64Array};
use crate::expr::func::{Function, FunctionType, StatefulFunction};
use crate::expr::funcs::utils::VecDequeExt;
use crate::expr::signature::Signature;

#[derive(Default, Clone, Serialize, Deserialize)]
struct AveDevState {
    values: VecDeque<f64>,
}

pub const F_AVEDEV: Function = Function {
    namespace: Some("f"),
    name: "avedev",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<AveDevState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let d = state.values.iter().sum::<f64>() / state.values.len() as f64;
                    let sum = state
                        .values
                        .iter()
                        .copied()
                        .fold(0.0, |acc, value| acc + f64::abs(value - d));
                    builder.append(sum / (state.values.len() as f64));
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct DevSqState {
    values: VecDeque<f64>,
}

pub const F_DEVSQ: Function = Function {
    namespace: Some("f"),
    name: "devsq",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<DevSqState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let d = state.values.iter().sum::<f64>() / state.values.len() as f64;
                    let mut sum = 0.0;
                    for value in state.values.iter().copied() {
                        sum += (value - d) * (value - d);
                    }
                    builder.append(sum);
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct ForcastState {
    values: VecDeque<f64>,
}

fn lsm(values: &VecDeque<f64>) -> Option<(f64, f64)> {
    let count = values.len();
    let mut xsum = 0.0;
    let mut ysum = 0.0;
    let mut xysum = 0.0;
    let mut x2sum = 0.0;
    for (idx, value) in values.iter().copied().enumerate() {
        xsum += (idx + 1) as f64;
        ysum += value;
        xysum += (idx + 1) as f64 * value;
        x2sum += (idx + 1) as f64 * (idx + 1) as f64;
    }

    if (xsum * xsum - (count as f64) * x2sum).abs() > f64::EPSILON {
        if xsum * xsum - (count as f64) * x2sum == 0.0 {
            return None;
        }

        return Some((
            (xsum * ysum - (count as f64) * xysum) / (xsum * xsum - (count as f64) * x2sum),
            (xysum * xsum - ysum * x2sum) / (xsum * xsum - (count as f64) * x2sum),
        ));
    }

    None
}

pub const F_FORCAST: Function = Function {
    namespace: Some("f"),
    name: "forcast",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<ForcastState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    if let Some((m, b)) = lsm(&state.values) {
                        builder.append(m * state.values.len() as f64 + b);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct SlopeState {
    values: VecDeque<f64>,
}

pub const F_SLOPE: Function = Function {
    namespace: Some("f"),
    name: "slope",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<SlopeState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut xy = 0.0;
                    let mut xx = 0.0;
                    let mut x = 0.0;
                    let mut y = 0.0;

                    for (idx, value) in state.values.iter().copied().enumerate() {
                        let k = (idx + 1) as f64;
                        x += k;
                        y += value;
                        xy += k * value;
                        xx += k * k;
                    }

                    let n = state.values.len() as f64;
                    let rv = n * xx - x * x;
                    let rv = (n * xy - x * y) / rv;
                    builder.append(rv);
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct StdState {
    values: VecDeque<f64>,
}

pub const F_STD: Function = Function {
    namespace: Some("f"),
    name: "std",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<StdState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut xx = 0.0;
                    let mut x = 0.0;

                    for value in state.values.iter().copied() {
                        x += value;
                        xx += value * value;
                    }

                    let n = state.values.len() as f64;
                    let t = (n * xx - x * x) / (n * (n - 1.0));
                    builder.append(t.sqrt());
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct StdDevState {
    values: VecDeque<f64>,
}

pub const F_STDDEV: Function = Function {
    namespace: Some("f"),
    name: "stddev",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<StdDevState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut sum = 0.0;
                    let d = state.values.iter().sum::<f64>() / state.values.len() as f64;

                    for value in state.values.iter().copied() {
                        sum += (value - d) * (value - d);
                    }
                    let t = sum / (state.values.len() as f64);
                    builder.append(t.sqrt());
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct StdpState {
    values: VecDeque<f64>,
}

pub const F_STDP: Function = Function {
    namespace: Some("f"),
    name: "stdp",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<StdpState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut x = 0.0;
                    let mut xx = 0.0;

                    for value in state.values.iter().copied() {
                        x += value;
                        xx += value * value;
                    }
                    let n = state.values.len() as f64;
                    let t = (n * xx - x * x) / (n * n);
                    builder.append(t.sqrt());
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct VarState {
    values: VecDeque<f64>,
}

pub const F_VAR: Function = Function {
    namespace: Some("f"),
    name: "var",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<VarState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut x = 0.0;
                    let mut xx = 0.0;

                    for value in state.values.iter().copied() {
                        x += value;
                        xx += value * value;
                    }
                    let n = state.values.len() as f64;
                    let t = (n * xx - x * x) / (n * (n - 1.0));
                    builder.append(t);
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct VarpState {
    values: VecDeque<f64>,
}

pub const F_VARP: Function = Function {
    namespace: Some("f"),
    name: "varp",
    signature: &Signature::Exact(&[DataType::Float64, DataType::Int64]),
    return_type: |_| DataType::Float64,
    function_type: FunctionType::Stateful(|| {
        Box::new(StatefulFunction::<VarState>::new(|state, args| {
            let array = args[0].downcast_ref::<Float64Array>();
            let n = args[1].downcast_ref::<Int64Array>();
            let mut builder = Float64Builder::default();

            for (x, n) in array.iter().zip(n.iter()) {
                if n < 2 {
                    builder.append_null();
                    continue;
                }

                state.values.push_back_limit(x, n as usize);
                if state.values.len() == n as usize {
                    let mut x = 0.0;
                    let mut xx = 0.0;

                    for value in state.values.iter().copied() {
                        x += value;
                        xx += value * value;
                    }
                    let n = state.values.len() as f64;
                    let t = (n * xx - x * x) / (n * n);
                    builder.append(t);
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

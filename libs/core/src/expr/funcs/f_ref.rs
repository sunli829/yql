use std::collections::VecDeque;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::array::{
    ArrayExt, BooleanArray, BooleanBuilder, DataType, Float64Array, Float64Builder, Int64Array,
    Int64Builder,
};
use crate::expr::func::{AggregateFunction, Function, FunctionType};
use crate::expr::signature::Signature;

trait VecDequeExt<T> {
    fn push_back_limit(&mut self, x: T, limit: usize) -> Option<T>;
}

impl<T> VecDequeExt<T> for VecDeque<T> {
    fn push_back_limit(&mut self, x: T, limit: usize) -> Option<T> {
        if self.len() == limit {
            let res = self.pop_front();
            self.push_back(x);
            res
        } else {
            self.push_back(x);
            None
        }
    }
}

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
        Box::new(AggregateFunction::<AllState>::new(|state, args| {
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
        Box::new(AggregateFunction::<AnyState>::new(|state, args| {
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
        Box::new(AggregateFunction::<BarsLastState>::new(|state, args| {
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
        Box::new(AggregateFunction::<BarsLastState>::new(|state, args| {
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
        Box::new(AggregateFunction::<CountState>::new(|state, args| {
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
        Box::new(AggregateFunction::<DmaState>::new(|state, args| {
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
        Box::new(AggregateFunction::<EmaState>::new(|state, args| {
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
        Box::new(AggregateFunction::<FilterState>::new(|state, args| {
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
                } else {
                    if state.filter_all {
                        builder.append(false);
                    } else if x {
                        state.filter_all = true;
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                }
            }

            Ok(Arc::new(builder.finish()))
        }))
    }),
};

use std::collections::VecDeque;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::array::{ArrayExt, BooleanArray, BooleanBuilder, DataType, Int64Array};
use crate::expr::func::{AggregateFunction, Function, FunctionType};
use crate::expr::signature::Signature;

trait VecDequeExt<T> {
    fn push_back_limit(&mut self, x: T, limit: usize) -> Option<T>;
}

impl<T> VecDequeExt<T> for VecDeque<T> {
    fn push_back_limit(&mut self, x: T, limit: usize) -> Option<T> {
        self.push_back(x);
        if self.len() == limit {
            self.pop_front()
        } else {
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

pub const ALL: Function = Function {
    namespace: Some("f"),
    name: "all",
    signature: &Signature::Uniform(1, &[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Float64,
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

pub const ANY: Function = Function {
    namespace: Some("f"),
    name: "any",
    signature: &Signature::Uniform(1, &[DataType::Boolean, DataType::Int64]),
    return_type: |_| DataType::Float64,
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

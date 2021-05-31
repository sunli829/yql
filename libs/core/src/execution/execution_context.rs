use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub num_input_rows: usize,
}

pub struct ExecutionContext {
    metrics: Mutex<ExecutionMetrics>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            metrics: Default::default(),
        }
    }

    pub(crate) fn update_metrics(&self, mut f: impl FnMut(&mut ExecutionMetrics)) {
        f(&mut *self.metrics.lock());
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics.lock().clone()
    }
}

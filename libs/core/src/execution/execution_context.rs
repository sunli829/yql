use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

use crate::execution::storage::Storage;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub num_input_rows: usize,
    pub num_output_rows: usize,
}

pub struct ExecutionContext {
    pub(crate) name: String,
    pub(crate) checkpoint_interval: Duration,
    pub(crate) storage: Option<Box<dyn Storage>>,
    metrics: Mutex<ExecutionMetrics>,
}

impl ExecutionContext {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            checkpoint_interval: Duration::from_secs(60 * 5),
            storage: None,
            metrics: Default::default(),
        }
    }

    pub fn with_storage(self, storage: impl Storage) -> Self {
        Self {
            storage: Some(Box::new(storage)),
            ..self
        }
    }

    pub(crate) fn update_metrics(&self, mut f: impl FnMut(&mut ExecutionMetrics)) {
        f(&mut *self.metrics.lock());
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.metrics.lock().clone()
    }
}

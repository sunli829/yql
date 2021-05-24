use tokio::time::Duration;

use crate::execution::storage::Storage;

pub struct ExecutionContext {
    pub(crate) name: String,
    pub(crate) checkpoint_interval: Duration,
    pub(crate) storage: Option<Box<dyn Storage>>,
}

impl ExecutionContext {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            checkpoint_interval: Duration::from_secs(60 * 5),
            storage: None,
        }
    }

    pub fn with_storage(self, storage: impl Storage) -> Self {
        Self {
            storage: Some(Box::new(storage)),
            ..self
        }
    }
}

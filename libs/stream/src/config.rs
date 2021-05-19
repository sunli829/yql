use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

pub type LoadStateFn = Box<dyn Fn() -> Result<Vec<u8>> + Send + Sync + 'static>;

pub type SaveStateFn = Box<dyn Fn(Vec<u8>) -> Result<()> + Send + Sync + 'static>;

pub type StreamConfigRef = Arc<StreamConfig>;

pub struct StreamConfig {
    pub name: String,
    pub checkpoint_interval: Duration,
    pub load_state_fn: Option<LoadStateFn>,
    pub save_state_fn: Option<SaveStateFn>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            name: "noname".to_string(),
            checkpoint_interval: Duration::from_secs(60 * 5),
            load_state_fn: None,
            save_state_fn: None,
        }
    }
}

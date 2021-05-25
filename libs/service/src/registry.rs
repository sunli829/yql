use std::collections::HashMap;

use tokio::sync::oneshot;

struct TaskInfo {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub struct Registry {
    streams: HashMap<String, TaskInfo>,
}

impl Registry {
    pub fn add(&mut self, name: &str, shutdown_tx: oneshot::Sender<()>) {
        self.streams.insert(
            name.to_string(),
            TaskInfo {
                shutdown_tx: Some(shutdown_tx),
            },
        );
    }

    pub fn is_running(&self, name: &str) -> bool {
        self.streams.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) {
        self.streams.remove(name);
    }

    pub fn stop(&mut self, name: &str) {
        if let Some(info) = self.streams.get_mut(name) {
            if let Some(tx) = info.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }
    }
}

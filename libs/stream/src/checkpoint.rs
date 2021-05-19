use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::{Barrier, Notify};

pub struct CheckPointBarrier {
    node_state: Mutex<HashMap<usize, Vec<u8>>>,
    barrier: Barrier,
    notify: Notify,
    node_count: usize,
    exit: bool,
}

impl CheckPointBarrier {
    pub(crate) fn new(node_count: usize, source_count: usize, exit: bool) -> Self {
        Self {
            node_state: Default::default(),
            barrier: Barrier::new(source_count),
            notify: Default::default(),
            node_count,
            exit,
        }
    }

    pub fn source_barrier(&self) -> &Barrier {
        &self.barrier
    }

    pub fn is_saved(&self, id: usize) -> bool {
        self.node_state.lock().contains_key(&id)
    }

    pub fn is_exit(&self) -> bool {
        self.exit
    }

    pub fn set_state(&self, id: usize, state: Option<Vec<u8>>) {
        let mut node_state = self.node_state.lock();
        node_state.insert(id, state.unwrap_or_default());
        if node_state.len() == self.node_count {
            self.notify.notify_one();
        }
    }

    pub async fn wait(&self) {
        self.notify.notified().await
    }

    pub fn take_state(&self) -> HashMap<usize, Vec<u8>> {
        std::mem::take(&mut *self.node_state.lock())
    }
}

use std::collections::VecDeque;

pub trait VecDequeExt<T> {
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

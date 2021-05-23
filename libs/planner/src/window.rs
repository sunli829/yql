#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Window {
    Fixed { length: i64 },
    Sliding { length: i64, interval: i64 },
}

impl Window {
    pub fn windows(self, timestamp: i64) -> Vec<(i64, i64)> {
        match self {
            Window::Fixed { length } => {
                let start = timestamp / length * length;
                vec![(start, start + length)]
            }
            Window::Sliding { length, interval } => {
                let mut windows = Vec::new();
                let mut time = timestamp / interval * interval;
                let end_time = time + length;
                while time < end_time {
                    windows.push((time, time + length));
                    time += interval;
                }
                windows
            }
        }
    }
}

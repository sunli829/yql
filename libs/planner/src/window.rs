use chrono::{DateTime, Duration, DurationRound};
use chrono_tz::Tz;

#[derive(Copy, Clone, PartialEq)]
pub enum Window {
    Fixed {
        length: Duration,
    },
    Sliding {
        length: Duration,
        interval: Duration,
    },
}

impl Window {
    pub(crate) fn start_time(&self, time: DateTime<Tz>) -> DateTime<Tz> {
        match self {
            Window::Fixed { length } => time.duration_trunc(*length).unwrap(),
            Window::Sliding { interval, .. } => time.duration_trunc(*interval).unwrap(),
        }
    }

    pub(crate) fn end_time(&self, time: DateTime<Tz>) -> DateTime<Tz> {
        match self {
            Window::Fixed { length } => time.duration_trunc(*length).unwrap() + *length,
            Window::Sliding { interval, length } => {
                time.duration_trunc(*interval).unwrap() + *length
            }
        }
    }
}

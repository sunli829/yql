use chrono::TimeZone;
use chrono::{DateTime, Datelike, Duration, NaiveDate};
use chrono_tz::Tz;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum Period {
    Day,
    Week,
    Month,
    Year,
}

impl Period {
    pub fn window(&self, time: DateTime<Tz>) -> (DateTime<Tz>, DateTime<Tz>) {
        let start = match self {
            Period::Day => time.date().and_hms(0, 0, 0),
            Period::Week => {
                (time.date() - Duration::days(time.date().weekday() as i64)).and_hms(0, 0, 0)
            }
            Period::Month => time.date().with_day(1).unwrap().and_hms(0, 0, 0),
            Period::Year => time
                .date()
                .with_month(1)
                .unwrap()
                .with_day(1)
                .unwrap()
                .and_hms(0, 0, 0),
        };

        let end = match self {
            Period::Day => start + Duration::days(1),
            Period::Week => start + Duration::days(7),
            Period::Month => {
                if start.month() < 12 {
                    start.with_month(start.month() + 1).unwrap()
                } else {
                    start
                        .with_year(start.year() + 1)
                        .and_then(|t| t.with_month(1))
                        .unwrap()
                }
            }
            Period::Year => start.with_year(start.year() + 1).unwrap(),
        };

        (start, end)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum Window {
    Fixed { length: i64 },
    Sliding { length: i64, interval: i64 },
    Period { period: Period, timezone: Tz },
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
            Window::Period { period, timezone } => {
                let datetime = timezone.timestamp_millis(timestamp);
                let (start, end) = period.window(datetime);
                vec![(start.timestamp_millis(), end.timestamp_millis())]
            }
        }
    }
}

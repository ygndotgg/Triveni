// {
//    "device_id": "sensor-17",
//    "ts_ms": 1711600000123,
//    "temperature": 28.4,
//    "humidity": 61.2,
//    "site": "warehouse-a"
//  }

use std::fmt::{self};

use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct TelemetryMessage {
    pub device_id: String,
    pub ts_ms: i64,
    pub temperature: f64,
    pub humidity: f64,
    pub event_date: String,
}

impl fmt::Display for TelemetryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TelemetryMessage[id:{},tms:{},temp:{:.2},humd:{:.2}]",
            self.device_id, self.ts_ms, self.temperature, self.humidity
        )
    }
}

pub fn event_date_from_ts_ms(ts_ms: i64) -> Result<String, String> {
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ts_ms).ok_or_else(|| {
        format!(
            "invalid
        timestamp millis:{ts_ms}"
        )
    })?;
    Ok(dt.format("%Y-%m-%d").to_string())
}

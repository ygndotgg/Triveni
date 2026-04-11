use crate::message::TelemetryMessage;

pub struct MessageBatcher {
    buffer: Vec<TelemetryMessage>,
    max_batch_size: usize,
}

impl Default for MessageBatcher {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            max_batch_size: 100,
        }
    }
}

impl MessageBatcher {
    pub fn push(&mut self, msg: TelemetryMessage) -> Option<Vec<TelemetryMessage>> {
        self.buffer.push(msg);
        if self.buffer.len() >= self.max_batch_size {
            let flushed = std::mem::take(&mut self.buffer);
            Some(flushed)
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<Vec<TelemetryMessage>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MessageBatcher;
    use crate::message::TelemetryMessage;

    fn message(idx: i64) -> TelemetryMessage {
        TelemetryMessage {
            device_id: format!("device-{idx}"),
            ts_ms: idx,
            temperature: 20.0,
            humidity: 40.0,
            event_date: "2026-04-11".to_string(),
        }
    }

    #[test]
    fn flush_returns_remaining_messages() {
        let mut batcher = MessageBatcher::default();
        for idx in 0..3 {
            assert!(batcher.push(message(idx)).is_none());
        }

        let flushed = batcher.flush().expect("expected buffered messages");
        assert_eq!(flushed.len(), 3);
        assert!(batcher.flush().is_none());
    }
}

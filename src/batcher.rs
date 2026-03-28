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
}

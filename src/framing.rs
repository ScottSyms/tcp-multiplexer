use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::Decoder;

#[derive(Clone)]
pub struct LineCodec {
    max_line_bytes: usize,
}

impl LineCodec {
    pub fn new(max_line_bytes: usize) -> Self {
        Self { max_line_bytes }
    }
}

impl Decoder for LineCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(pos) = src.iter().position(|&b| b == b'\n') {
            let line_len = pos + 1;
            if line_len > self.max_line_bytes + 1 {
                let excess = src.split_to(line_len);
                tracing::warn!(
                    bytes = excess.len(),
                    "line exceeds max_line_bytes, dropping"
                );
                metrics::counter!("tcp_broker_lines_truncated_total").increment(1);
                return Ok(None);
            }
            let line = src.split_to(line_len).freeze();
            return Ok(Some(line));
        }

        if src.len() > self.max_line_bytes {
            tracing::warn!(
                bytes = src.len(),
                "incomplete line exceeds max_line_bytes, resetting buffer"
            );
            src.clear();
            metrics::counter!("tcp_broker_lines_truncated_total").increment(1);
        }

        Ok(None)
    }
}

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

use crate::message::Message;

pub struct BoundedQueue {
    inner: Mutex<VecDeque<Message>>,
    current_bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    push_notify: Notify,
    pop_notify: Notify,
}

impl BoundedQueue {
    pub fn new(max_messages: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(VecDeque::with_capacity(max_messages)),
            current_bytes: AtomicUsize::new(0),
            max_messages,
            max_bytes,
            push_notify: Notify::new(),
            pop_notify: Notify::new(),
        })
    }

    pub async fn push(&self, msg: Message) {
        let msg_size = msg.data.len();

        loop {
            let mut inner = self.inner.lock().await;
            if inner.len() < self.max_messages
                && self.current_bytes.load(Ordering::Acquire) + msg_size <= self.max_bytes
            {
                let was_empty = inner.is_empty();
                inner.push_back(msg);
                self.current_bytes.fetch_add(msg_size, Ordering::Release);
                drop(inner);
                if was_empty {
                    self.pop_notify.notify_one();
                }
                return;
            }
            drop(inner);
            self.push_notify.notified().await;
        }
    }

    pub async fn try_push(&self, msg: Message) -> bool {
        let msg_size = msg.data.len();
        let mut inner = self.inner.lock().await;

        if inner.len() < self.max_messages
            && self.current_bytes.load(Ordering::Relaxed) + msg_size <= self.max_bytes
        {
            let was_empty = inner.is_empty();
            inner.push_back(msg);
            self.current_bytes.fetch_add(msg_size, Ordering::Release);
            drop(inner);
            if was_empty {
                self.pop_notify.notify_one();
            }
            return true;
        }
        false
    }

    pub async fn push_drop_oldest(&self, msg: Message) -> bool {
        let msg_size = msg.data.len();
        if msg_size > self.max_bytes {
            return false;
        }

        let mut inner = self.inner.lock().await;

        while !inner.is_empty()
            && (inner.len() >= self.max_messages
                || self.current_bytes.load(Ordering::Relaxed) + msg_size > self.max_bytes)
        {
            if let Some(oldest) = inner.pop_front() {
                self.current_bytes
                    .fetch_sub(oldest.data.len(), Ordering::Relaxed);
                metrics::counter!("tcp_broker_messages_dropped_total").increment(1);
            }
        }

        let was_empty = inner.is_empty();
        inner.push_back(msg);
        self.current_bytes.fetch_add(msg_size, Ordering::Release);
        drop(inner);
        if was_empty {
            self.pop_notify.notify_one();
        }
        true
    }

    pub async fn pop(&self) -> Message {
        loop {
            let mut inner = self.inner.lock().await;
            if let Some(msg) = inner.pop_front() {
                let msg_size = msg.data.len();
                self.current_bytes.fetch_sub(msg_size, Ordering::Release);
                let was_full = inner.len() >= self.max_messages.saturating_sub(1);
                let remaining = inner.len();
                drop(inner);
                if was_full {
                    self.push_notify.notify_one();
                }
                if remaining == 0 {
                    self.pop_notify.notify_one();
                }
                return msg;
            }
            drop(inner);
            self.pop_notify.notified().await;
        }
    }

    pub fn len(&self) -> usize {
        self.inner
            .try_lock()
            .map(|inner| inner.len())
            .unwrap_or(0)
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }
}

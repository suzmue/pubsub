use crate::service::loadtest::{CheckResponse, MessageIdentifier};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct MetricsTracker {
    buckets: Arc<Vec<AtomicU64>>,
    error_count: Arc<AtomicU64>,
    received_messages: Arc<Mutex<Vec<MessageIdentifier>>>,
    include_ids: Arc<AtomicU64>,
}

impl MetricsTracker {
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(128);
        for _ in 0..128 {
            buckets.push(AtomicU64::new(0));
        }

        Self {
            buckets: Arc::new(buckets),
            error_count: Arc::new(AtomicU64::new(0)),
            received_messages: Arc::new(Mutex::new(Vec::new())),
            include_ids: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn set_include_ids(&self, include_ids: bool) {
        self.include_ids.store(if include_ids { 1 } else { 0 }, Ordering::Relaxed);
    }

    fn bucket_for(latency: Duration) -> usize {
        let millis = latency.as_secs_f64() * 1000.0;
        if millis <= 1.0 {
            return 0;
        }
        let bucket = (millis.ln() / 1.5_f64.ln()).floor() as usize;
        if bucket >= 128 {
            127
        } else {
            bucket
        }
    }

    pub fn record_success(&self, latency: Duration, id: Option<MessageIdentifier>) {
        let bucket = Self::bucket_for(latency);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);

        if self.include_ids.load(Ordering::Relaxed) == 1 {
            if let Some(id) = id {
                if let Ok(mut messages) = self.received_messages.lock() {
                    messages.push(id);
                }
            }
        }
    }

    pub fn record_latency(&self, latency: Duration) {
        self.record_success(latency, None);
    }

    pub fn increment_error_count(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn export_to(&self, response: &mut CheckResponse) {
        let mut bucket_values = Vec::with_capacity(self.buckets.len());
        let mut last_non_zero = 0;
        let mut found_non_zero = false;
        
        for (i, bucket) in self.buckets.iter().enumerate() {
            let val = bucket.swap(0, Ordering::Relaxed);
            bucket_values.push(val as i64);
            if val > 0 {
                last_non_zero = i;
                found_non_zero = true;
            }
        }
        
        if found_non_zero {
            bucket_values.truncate(last_non_zero + 1);
            response.bucket_values = bucket_values;
        } else {
            response.bucket_values = Vec::new();
        }
        
        if self.include_ids.load(Ordering::Relaxed) == 1 {
            if let Ok(mut messages) = self.received_messages.lock() {
                response.received_messages = std::mem::take(&mut *messages);
            }
        }

        response.failed = self.error_count.swap(0, Ordering::Relaxed) as i64;
    }
}

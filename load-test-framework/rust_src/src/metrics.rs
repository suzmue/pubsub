use crate::service::loadtest::CheckResponse;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const NUM_BUCKETS: usize = 64;

#[derive(Clone)]
pub struct MetricsTracker {
    buckets: Arc<Vec<AtomicU64>>,
    message_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
}

impl MetricsTracker {
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(AtomicU64::new(0));
        }

        Self {
            buckets: Arc::new(buckets),
            message_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_latency(&self, latency: Duration) {
        let millis = latency.as_secs_f64() * 1000.0;
        let index = if millis <= 1.0 {
            0
        } else {
            (millis.ln() / 1.5_f64.ln()).floor() as usize + 1
        };

        if index < NUM_BUCKETS {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        self.message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_error_count(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn export_to(&self, response: &mut CheckResponse) {
        response.bucket_values = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed) as i64)
            .collect();
        // Not populating received_messages for now, as it requires more state.
        response.failed = self.error_count.load(Ordering::Relaxed) as i64;
    }
}

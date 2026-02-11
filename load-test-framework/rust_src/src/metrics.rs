use crate::service::loadtest::{CheckResponse, MessageIdentifier};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

pub enum MetricUpdate {
    Success {
        latency: Duration,
        id: Option<MessageIdentifier>,
    },
    Failure,
}

#[derive(Clone)]
pub struct MetricsTracker {
    buckets: Arc<Mutex<Vec<u64>>>,
    error_count: Arc<AtomicU64>,
    received_messages: Arc<Mutex<Vec<MessageIdentifier>>>,
    include_ids: Arc<AtomicBool>,
    update_tx: mpsc::UnboundedSender<MetricUpdate>,
}

impl MetricsTracker {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<MetricUpdate>();
        let buckets = Arc::new(Mutex::new(Vec::with_capacity(128)));
        let error_count = Arc::new(AtomicU64::new(0));
        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let include_ids = Arc::new(AtomicBool::new(false));

        let buckets_clone = buckets.clone();
        let error_count_clone = error_count.clone();
        let received_messages_clone = received_messages.clone();
        let include_ids_clone = include_ids.clone();

        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                match update {
                    MetricUpdate::Success { latency, id } => {
                        let bucket = Self::bucket_for(latency);
                        let mut buckets = buckets_clone.lock().unwrap();
                        while buckets.len() <= bucket {
                            buckets.push(0);
                        }
                        buckets[bucket] += 1;

                        if include_ids_clone.load(Ordering::Relaxed) {
                            if let Some(id) = id {
                                if let Ok(mut messages) = received_messages_clone.lock() {
                                    messages.push(id);
                                }
                            }
                        }
                    }
                    MetricUpdate::Failure => {
                        error_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        Self {
            buckets,
            error_count,
            received_messages,
            include_ids,
            update_tx: tx,
        }
    }

    pub fn set_include_ids(&self, include_ids: bool) {
        self.include_ids.store(include_ids, Ordering::Relaxed);
    }

    fn bucket_for(latency: Duration) -> usize {
        let millis = latency.as_secs_f64() * 1000.0;
        if millis <= 0.0 {
            return 0;
        }
        let bucket = (millis.ln() / 1.5_f64.ln()).floor() as isize;
        if bucket < 0 {
            0
        } else {
            bucket as usize
        }
    }

    pub fn record_success(&self, latency: Duration, id: Option<MessageIdentifier>) {
        let _ = self.update_tx.send(MetricUpdate::Success { latency, id });
    }

    pub fn record_latency(&self, latency: Duration) {
        self.record_success(latency, None);
    }

    pub fn increment_error_count(&self) {
        let _ = self.update_tx.send(MetricUpdate::Failure);
    }

    pub fn export_to(&self, response: &mut CheckResponse) {
        if let Ok(mut buckets) = self.buckets.lock() {
            response.bucket_values = std::mem::take(&mut *buckets).into_iter().map(|v| v as i64).collect();
        }
        
        if self.include_ids.load(Ordering::Relaxed) {
            if let Ok(mut messages) = self.received_messages.lock() {
                response.received_messages = std::mem::take(&mut *messages);
            }
        }

        response.failed = self.error_count.swap(0, Ordering::Relaxed) as i64;
    }
}

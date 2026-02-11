use crate::metrics::MetricsTracker;
use crate::service::loadtest::MessageIdentifier;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, interval};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use futures::stream::{StreamExt, FuturesUnordered};

pub struct PublisherTask {
    project_id: String,
    topic_id: String,
    rate: f32,
    batch_duration: Option<Duration>,
    batch_size: i32,
    message_size: i32,
    worker_id: usize,
}

impl PublisherTask {
    pub fn new(
        project_id: String,
        topic_id: String,
        rate: f32,
        batch_duration: Option<Duration>,
        batch_size: i32,
        message_size: i32,
        worker_id: usize,
    ) -> Self {
        Self {
            project_id,
            topic_id,
            rate,
            batch_duration,
            batch_size,
            message_size,
            worker_id,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let topic_name = format!("projects/{}/topics/{}", self.project_id, self.topic_id);
        let mut builder = Publisher::builder(topic_name)
            .with_grpc_subchannel_count(4); 
        
        let delay = self.batch_duration.unwrap_or(Duration::from_millis(10));
        builder = builder.set_delay_threshold(delay);
        
        if self.batch_size > 0 {
            builder = builder.set_message_count_threshold(self.batch_size as u32);
        }
        builder = builder.set_byte_threshold(9500000);
        
        let publisher = builder.build().await.unwrap();
        let data = Bytes::from(vec![0u8; self.message_size as usize]);
        
        let client_id = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64).wrapping_add(self.worker_id as i64);
        let client_id_str = client_id.to_string();

        let mut ticker = if self.rate > 0.0 && self.rate.is_finite() {
            Some(interval(Duration::from_secs_f64(1.0 / self.rate as f64)))
        } else {
            None
        };

        // If no rate is specified, we limit outstanding requests per worker.
        let semaphore = if ticker.is_none() {
            Some(Arc::new(Semaphore::new(10000)))
        } else {
            None
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<(google_cloud_pubsub::model_ext::PublishFuture, Instant, i32, Option<tokio::sync::OwnedSemaphorePermit>)>();

        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::new();
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some((fut, start, seq, permit)) => {
                                futures.push(async move {
                                    let res = fut.await;
                                    drop(permit);
                                    (res, start, seq)
                                });
                            }
                            None => break,
                        }
                    }
                    Some((result, publish_start, seq_num)) = futures.next() => {
                        if result.is_ok() {
                            metrics_clone.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                                publisher_client_id: client_id,
                                sequence_number: seq_num,
                            }));
                        } else {
                            metrics_clone.increment_error_count();
                        }
                    }
                }
            }
            // Process remaining futures after rx is closed
            while let Some((result, publish_start, seq_num)) = futures.next().await {
                if result.is_ok() {
                    metrics_clone.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                        publisher_client_id: client_id,
                        sequence_number: seq_num,
                    }));
                } else {
                    metrics_clone.increment_error_count();
                }
            }
        });

        let mut sequence_number: i32 = 0;
        loop {
            let permit = if let Some(t) = &mut ticker {
                t.tick().await;
                None
            } else if let Some(s) = &semaphore {
                Some(s.clone().acquire_owned().await.unwrap())
            } else {
                unreachable!()
            };

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let msg = Message::new()
                .set_data(data.clone())
                .set_attributes([
                    ("sendTime", now.to_string()),
                    ("clientId", client_id_str.clone()),
                    ("sequenceNumber", sequence_number.to_string()),
                ]);

            let publish_start = Instant::now();
            let fut = publisher.publish(msg);
            
            if tx.send((fut, publish_start, sequence_number, permit)).is_err() {
                break;
            }

            sequence_number = sequence_number.wrapping_add(1);
        }
    }
}

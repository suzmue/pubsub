use crate::metrics::MetricsTracker;
use crate::service::loadtest::MessageIdentifier;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, interval};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures::stream::{StreamExt, FuturesUnordered};

pub struct PublisherTask {
    project_id: String,
    topic_id: String,
    rate: f32,
    batch_duration: Option<Duration>,
    batch_size: i32,
    message_size: i32,
    num_workers: usize,
}

impl PublisherTask {
    pub fn new(
        project_id: String,
        topic_id: String,
        rate: f32,
        batch_duration: Option<Duration>,
        batch_size: i32,
        message_size: i32,
        num_workers: usize,
    ) -> Self {
        Self {
            project_id,
            topic_id,
            rate,
            batch_duration,
            batch_size,
            message_size,
            num_workers,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let topic_name = format!("projects/{}/topics/{}", self.project_id, self.topic_id);
        let mut builder = Publisher::builder(topic_name)
            .with_grpc_subchannel_count(8);
        
        // Match Go's default delay threshold if not provided.
        let delay = self.batch_duration.unwrap_or(Duration::from_millis(10));
        builder = builder.set_delay_threshold(delay);
        
        if self.batch_size > 0 {
            builder = builder.set_message_count_threshold(self.batch_size as u32);
        }
        builder = builder.set_byte_threshold(9500000);
        
        let publisher = Arc::new(builder.build().await.unwrap());
        let data = Bytes::from(vec![0u8; self.message_size as usize]);
        
        let mut worker_handles = Vec::with_capacity(self.num_workers);
        let per_worker_rate = if self.rate > 0.0 {
            Some(self.rate / self.num_workers as f32)
        } else {
            None
        };

        for i in 0..self.num_workers {
            let publisher = publisher.clone();
            let metrics = metrics.clone();
            let data = data.clone();
            
            let client_id = (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64).wrapping_add(i as i64);
            let client_id_str = client_id.to_string();

            worker_handles.push(tokio::spawn(async move {
                let mut sequence_number: i32 = 0;
                let mut ticker = per_worker_rate.map(|r| interval(Duration::from_secs_f64(1.0 / r as f64)));
                
                // If no rate is specified, we limit outstanding requests to avoid OOM.
                // 5000 outstanding requests per worker is a reasonable starting point.
                let semaphore = if per_worker_rate.is_none() {
                    Some(Arc::new(Semaphore::new(5000)))
                } else {
                    None
                };

                let mut futures = FuturesUnordered::new();

                loop {
                    tokio::select! {
                        permit = async {
                            if let Some(t) = &mut ticker {
                                t.tick().await;
                                None
                            } else if let Some(s) = &semaphore {
                                Some(s.clone().acquire_owned().await.unwrap())
                            } else {
                                unreachable!()
                            }
                        } => {
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
                            
                            let seq_num = sequence_number;
                            futures.push(async move {
                                let result = fut.await;
                                drop(permit);
                                (result, publish_start, seq_num)
                            });

                            sequence_number = sequence_number.wrapping_add(1);
                        }
                        Some((result, publish_start, seq_num)) = futures.next() => {
                            if result.is_ok() {
                                metrics.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                                    publisher_client_id: client_id,
                                    sequence_number: seq_num,
                                }));
                            } else {
                                metrics.increment_error_count();
                            }
                        }
                    }
                }
            }));
        }

        futures::future::join_all(worker_handles).await;
    }
}

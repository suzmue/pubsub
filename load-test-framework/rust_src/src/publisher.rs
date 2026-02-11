use crate::metrics::MetricsTracker;
use crate::service::loadtest::MessageIdentifier;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::model::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

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
        
        if let Some(duration) = self.batch_duration {
            builder = builder.set_delay_threshold(duration);
        }
        if self.batch_size > 0 {
            builder = builder.set_message_count_threshold(self.batch_size as u32);
        }
        builder = builder.set_byte_threshold(9500000);
        
        let publisher = Arc::new(builder.build().await.unwrap());
        let data = Bytes::from(vec![0u8; self.message_size as usize]);
        
        let mut worker_handles = Vec::with_capacity(self.num_workers);

        for i in 0..self.num_workers {
            let publisher = publisher.clone();
            let metrics = metrics.clone();
            let data = data.clone();
            let rate = self.rate / self.num_workers as f32;
            
            let client_id = (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64).wrapping_add(i as i64);
            let client_id_str = client_id.to_string();

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(google_cloud_pubsub::model_ext::PublishFuture, Instant, i32)>();
            
            let metrics_for_processor = metrics.clone();
            tokio::spawn(async move {
                let mut futures = futures::stream::FuturesUnordered::new();
                loop {
                    tokio::select! {
                        msg = rx.recv() => {
                            match msg {
                                Some((p, publish_start, seq_num)) => {
                                    futures.push(async move {
                                        (p.await, publish_start, seq_num)
                                    });
                                }
                                None => break,
                            }
                        }
                        Some((result, publish_start, seq_num)) = futures.next() => {
                            match result {
                                Ok(_) => {
                                    metrics_for_processor.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                                        publisher_client_id: client_id,
                                        sequence_number: seq_num,
                                    }));
                                }
                                Err(_) => {
                                    metrics_for_processor.increment_error_count();
                                }
                            }
                        }
                    }
                }
                while let Some((result, publish_start, seq_num)) = futures.next().await {
                    match result {
                        Ok(_) => {
                            metrics_for_processor.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                                publisher_client_id: client_id,
                                sequence_number: seq_num,
                            }));
                        }
                        Err(_) => {
                            metrics_for_processor.increment_error_count();
                        }
                    }
                }
            });

            worker_handles.push(tokio::task::spawn_blocking(move || {
                let mut sequence_number: i32 = 0;
                let mut last_yield = Instant::now();
                loop {
                    if rate > 0.0 && rate.is_finite() {
                        let target_interval = Duration::from_secs_f64(1.0 / rate as f64);
                        let elapsed = last_yield.elapsed();
                        if elapsed < target_interval {
                            std::thread::sleep(target_interval - elapsed);
                        }
                        last_yield = Instant::now();
                    }

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
                    let p = publisher.publish(msg);
                    
                    if tx.send((p, publish_start, sequence_number)).is_err() {
                        break;
                    }

                    sequence_number = sequence_number.wrapping_add(1);
                }
            }));
        }

        futures::future::join_all(worker_handles).await;
    }
}

use crate::metrics::MetricsTracker;
use crate::service::loadtest::MessageIdentifier;
use google_cloud_pubsub::client::{BasePublisher};
use google_cloud_pubsub::model::Message;
use std::time::{Duration, SystemTime};
use tokio::time::{interval, Instant};
use bytes::Bytes;

pub struct PublisherTask {
    project_id: String,
    topic_id: String,
    rate: f32,
    batch_duration: Option<Duration>,
    batch_size: i32,
    message_size: i32,
}

impl PublisherTask {
    pub fn new(
        project_id: String,
        topic_id: String,
        rate: f32,
        batch_duration: Option<Duration>,
        batch_size: i32,
        message_size: i32,
    ) -> Self {
        Self {
            project_id,
            topic_id,
            rate,
            batch_duration,
            batch_size,
            message_size,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let client = BasePublisher::builder().with_grpc_subchannel_count(4).build().await.unwrap();
        let mut publisher_builder = client.publisher(format!("projects/{}/topics/{}", self.project_id, self.topic_id));
        
        if let Some(duration) = self.batch_duration {
            publisher_builder = publisher_builder.set_delay_threshold(duration);
        }
        if self.batch_size > 0 {
            publisher_builder = publisher_builder.set_message_count_threshold(self.batch_size as u32);
        }
        publisher_builder = publisher_builder.set_byte_threshold(9500000);
        
        let publisher = publisher_builder.build();
        
        let mut sequence_number: i32 = 0;
        let mut ticker = if self.rate > 0.0 && self.rate.is_finite() {
            Some(interval(Duration::from_secs_f64(1.0 / self.rate as f64)))
        } else {
            None
        };

        let client_id = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        let data = Bytes::from(vec![0u8; self.message_size as usize]);

        loop {
            if let Some(ticker) = &mut ticker {
                ticker.tick().await;
            } else if self.batch_size > 0 && sequence_number % self.batch_size == 0 {
                tokio::task::yield_now().await;
            }
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let mut attributes = std::collections::HashMap::new();
            attributes.insert("sendTime".to_string(), now.to_string());
            attributes.insert("clientId".to_string(), client_id.to_string());
            attributes.insert("sequenceNumber".to_string(), sequence_number.to_string());
            
            let msg = Message::new()
                .set_data(data.clone())
                .set_attributes(attributes);

            let publish_start = Instant::now();
            let fut = publisher.publish(msg);
            
            let metrics = metrics.clone();
            let seq_num = sequence_number;
            tokio::spawn(async move {
                if fut.await.is_ok() {
                    metrics.record_success(publish_start.elapsed(), Some(MessageIdentifier {
                        publisher_client_id: client_id,
                        sequence_number: seq_num,
                    }));
                } else {
                    metrics.increment_error_count();
                }
            });

            sequence_number = sequence_number.wrapping_add(1);
        }
    }
}

use crate::metrics::MetricsTracker;
use google_cloud_pubsub::client::{BasePublisher};
use google_cloud_pubsub::model::PubsubMessage;
use std::time::{Duration, SystemTime};
use tokio::time::interval;

pub struct PublisherTask {
    project_id: String,
    topic_id: String,
    rate: f32,
}

impl PublisherTask {
    pub fn new(project_id: String, topic_id: String, rate: f32) -> Self {
        Self {
            project_id,
            topic_id,
            rate,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let client = BasePublisher::builder().build().await.unwrap();
        let publisher = client.publisher(format!("projects/{}/topics/{}", self.project_id, self.topic_id)).build();
        let mut sequence_number = 0;
        let mut ticker = if self.rate > 0.0 && self.rate.is_finite() {
            Some(interval(Duration::from_secs_f64(1.0 / self.rate as f64)))
        } else {
            None
        };

        loop {
            if let Some(ticker) = &mut ticker {
                ticker.tick().await;
            }
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let mut attributes = std::collections::HashMap::new();
            attributes.insert("sendTime".to_string(), now.to_string());
            attributes.insert("clientId".to_string(), "rust-test-client".to_string());
            attributes.insert("sequenceNumber".to_string(), sequence_number.to_string());
            let msg = PubsubMessage::new()
                .set_data(vec![0; 100])
                .set_attributes(attributes);

            let fut = publisher.publish(msg);
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let start_time = SystemTime::now();
                if fut.await.is_ok() {
                    metrics.record_latency(start_time.elapsed().unwrap());
                } else {
                    metrics.increment_error_count();
                }
            });

            sequence_number += 1;
        }
    }
}

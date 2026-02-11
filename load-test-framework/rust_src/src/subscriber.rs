use crate::metrics::MetricsTracker;
use google_cloud_pubsub::client::Subscriber;
use std::time::{Duration, SystemTime};

pub struct SubscriberTask {
    project_id: String,
    subscription_id: String,
}

impl SubscriberTask {
    pub fn new(project_id: String, subscription_id: String) -> Self {
        Self {
            project_id,
            subscription_id,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let subscriber: Subscriber = Subscriber::builder().build().await.unwrap();
        let mut stream = subscriber.streaming_pull(format!("projects/{}/subscriptions/{}", self.project_id, self.subscription_id)).start();

        while let Some((m, h)) = stream.next().await.transpose().unwrap() {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            if let Some(send_time_str) = m.attributes.get("sendTime") {
                if let Ok(send_time) = send_time_str.parse::<u128>() {
                    let latency = Duration::from_millis((now - send_time) as u64);
                    metrics.record_latency(latency);
                }
            }
            if let Some(client_id_str) = m.attributes.get("clientId") {
                if let Ok(client_id) = client_id_str.parse::<u64>() {
                    metrics.record_client_id(client_id);
                }
            }
            if let Some(sequence_number_str) = m.attributes.get("sequenceNumber") {
                if let Ok(sequence_number) = sequence_number_str.parse::<u64>() {
                    metrics.record_sequence_number(sequence_number);
                }
            }
            h.ack();
        }
    }
}

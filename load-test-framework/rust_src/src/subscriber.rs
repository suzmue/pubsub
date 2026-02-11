use crate::metrics::MetricsTracker;
use crate::service::loadtest::MessageIdentifier;
use google_cloud_pubsub::client::Subscriber;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct SubscriberTask {
    project_id: String,
    subscription_id: String,
    worker_id: usize,
}

impl SubscriberTask {
    pub fn new(project_id: String, subscription_id: String, worker_id: usize) -> Self {
        Self {
            project_id,
            subscription_id,
            worker_id,
        }
    }

    pub async fn run(&self, metrics: MetricsTracker) {
        let subscriber: Subscriber = Subscriber::builder()
            .with_grpc_subchannel_count(4)
            .build()
            .await
            .unwrap();
            
        let mut stream = subscriber
            .streaming_pull(format!("projects/{}/subscriptions/{}", self.project_id, self.subscription_id))
            .set_max_outstanding_messages(100000)
            .set_max_outstanding_bytes(100 * 1024 * 1024)
            .start();

        while let Some(result) = stream.next().await {
            let (m, h): (google_cloud_pubsub::model::Message, google_cloud_pubsub::subscriber::handler::Handler) = match result {
                Ok(v) => v,
                Err(_) => {
                    metrics.increment_error_count();
                    continue;
                }
            };

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let mut latency = None;
            if let Some(send_time_str) = m.attributes.get("sendTime") {
                if let Ok(send_time) = send_time_str.parse::<u128>() {
                    latency = Some(Duration::from_millis(now.saturating_sub(send_time) as u64));
                }
            }

            let mut publisher_client_id = 0;
            if let Some(client_id_str) = m.attributes.get("clientId") {
                if let Ok(client_id) = client_id_str.parse::<i64>() {
                    publisher_client_id = client_id;
                }
            }

            let mut sequence_number = 0;
            if let Some(sequence_number_str) = m.attributes.get("sequenceNumber") {
                if let Ok(seq_num) = sequence_number_str.parse::<i32>() {
                    sequence_number = seq_num;
                }
            }

            if let Some(lat) = latency {
                metrics.record_success(lat, Some(MessageIdentifier {
                    publisher_client_id,
                    sequence_number,
                }));
            } else {
                metrics.record_latency(Duration::from_millis(0));
            }

            h.ack();
        }
    }
}

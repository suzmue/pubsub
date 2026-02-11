use crate::metrics::MetricsTracker;
use crate::publisher::PublisherTask;
use crate::subscriber::SubscriberTask;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

pub mod loadtest {
    tonic::include_proto!("google.pubsub.loadtest");
}

pub struct LoadtestWorkerImpl {
    metrics: MetricsTracker,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl LoadtestWorkerImpl {
    pub fn new() -> Self {
        Self {
            metrics: MetricsTracker::new(),
            task: Arc::new(Mutex::new(None)),
        }
    }
}

#[tonic::async_trait]
impl loadtest::loadtest_worker_server::LoadtestWorker for LoadtestWorkerImpl {
    async fn start(
        &self,
        request: Request<loadtest::StartRequest>,
    ) -> Result<Response<loadtest::StartResponse>, Status> {
        let metrics = self.metrics.clone();
        let request = request.into_inner();
        let task: JoinHandle<()> = match request.client_options {
            Some(loadtest::start_request::ClientOptions::PublisherOptions(options)) => {
                let task = PublisherTask::new(
                    request.project.clone(),
                    request.topic.clone(),
                    options.rate               );
                tokio::spawn(async move { task.run(metrics).await })
            }
            Some(loadtest::start_request::ClientOptions::SubscriberOptions(_)) => {
                let subscription = match request.options {
                    Some(loadtest::start_request::Options::PubsubOptions(options)) => options.subscription,
                    None => return Err(Status::invalid_argument("No pubsub options specified")),
                };
                let task = SubscriberTask::new(request.project, subscription);
                tokio::spawn(async move { task.run(metrics).await })
            }
            None => return Err(Status::invalid_argument("No task specified")),
        };
        *self.task.lock().unwrap() = Some(task);
        Ok(Response::new(loadtest::StartResponse {}))
    }

    async fn check(
        &self,
        _request: Request<loadtest::CheckRequest>,
    ) -> Result<Response<loadtest::CheckResponse>, Status> {
        let is_finished = self.task.lock().unwrap().as_ref().map_or(true, |t| t.is_finished());
        let mut response = loadtest::CheckResponse {
            is_finished,
            ..Default::default()
        };
        self.metrics.export_to(&mut response);
        Ok(Response::new(response))
    }
}

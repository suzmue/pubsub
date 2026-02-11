use crate::metrics::MetricsTracker;
use crate::publisher::PublisherTask;
use crate::subscriber::SubscriberTask;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::{Request, Response, Status};

pub mod loadtest {
    tonic::include_proto!("google.pubsub.loadtest");
}

pub struct LoadtestWorkerImpl {
    metrics: MetricsTracker,
    task: Arc<Mutex<Option<Vec<JoinHandle<()>>>>> ,
    start_time: Arc<Mutex<Option<SystemTime>>>,
}

impl LoadtestWorkerImpl {
    pub fn new() -> Self {
        Self {
            metrics: MetricsTracker::new(),
            task: Arc::new(Mutex::new(None)),
            start_time: Arc::new(Mutex::new(None)),
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
        
        metrics.set_include_ids(request.include_ids);

        let start_time = match request.start_time {
            Some(t) => UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32),
            None => SystemTime::now(),
        };
        *self.start_time.lock().unwrap() = Some(start_time);

        let test_duration = match request.test_duration {
            Some(d) => std::time::Duration::new(d.seconds as u64, d.nanos as u32),
            None => std::time::Duration::from_secs(3600), // Default 1 hour
        };

        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        
        // One task per CPU as requested.
        let num_workers = num_cpus;

        let mut worker_handles = Vec::with_capacity(num_workers);

        match request.client_options {
            Some(loadtest::start_request::ClientOptions::PublisherOptions(options)) => {
                let batch_duration = options.batch_duration.map(|d| {
                    std::time::Duration::new(d.seconds as u64, d.nanos as u32)
                });
                // Divide total rate by number of workers (CPUs)
                let per_worker_rate = options.rate / num_workers as f32;

                for i in 0..num_workers {
                    let task = PublisherTask::new(
                        request.project.clone(),
                        request.topic.clone(),
                        per_worker_rate,
                        batch_duration,
                        options.batch_size,
                        options.message_size,
                        i,
                    );
                    let metrics = metrics.clone();
                    worker_handles.push(tokio::spawn(async move {
                        if let Ok(duration) = start_time.duration_since(SystemTime::now()) {
                            tokio::time::sleep(duration).await;
                        }
                        let _ = timeout(test_duration, task.run(metrics)).await;
                    }));
                }
            }
            Some(loadtest::start_request::ClientOptions::SubscriberOptions(_)) => {
                let subscription = match request.options {
                    Some(loadtest::start_request::Options::PubsubOptions(options)) => options.subscription,
                    None => return Err(Status::invalid_argument("No pubsub options specified")),
                };
                for i in 0..num_workers {
                    let task = SubscriberTask::new(request.project.clone(), subscription.clone(), i);
                    let metrics = metrics.clone();
                    worker_handles.push(tokio::spawn(async move {
                        if let Ok(duration) = start_time.duration_since(SystemTime::now()) {
                            tokio::time::sleep(duration).await;
                        }
                        let _ = timeout(test_duration, task.run(metrics)).await;
                    }));
                }
            }
            None => return Err(Status::invalid_argument("No task specified")),
        };

        *self.task.lock().unwrap() = Some(worker_handles);
        Ok(Response::new(loadtest::StartResponse {}))
    }

    async fn check(
        &self,
        _request: Request<loadtest::CheckRequest>,
    ) -> Result<Response<loadtest::CheckResponse>, Status> {
        let is_finished = self.task.lock().unwrap().as_ref().map_or(true, |tasks| {
            tasks.iter().all(|t| t.is_finished())
        });
        let running_duration = self.start_time.lock().unwrap().map(|t| {
            match SystemTime::now().duration_since(t) {
                Ok(elapsed) => {
                    prost_types::Duration {
                        seconds: elapsed.as_secs() as i64,
                        nanos: elapsed.subsec_nanos() as i32,
                    }
                }
                Err(e) => {
                    let negative_elapsed = e.duration();
                    prost_types::Duration {
                        seconds: -(negative_elapsed.as_secs() as i64),
                        nanos: -(negative_elapsed.subsec_nanos() as i32),
                    }
                }
            }
        });

        let mut response = loadtest::CheckResponse {
            is_finished,
            running_duration,
            ..Default::default()
        };
        self.metrics.export_to(&mut response);
        Ok(Response::new(response))
    }
}

use tonic::transport::Server;
use clap::Parser;
use service::loadtest::loadtest_worker_server::LoadtestWorkerServer;

pub mod metrics;
pub mod publisher;
pub mod service;
pub mod subscriber;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("[::]:{}", args.port).parse().unwrap();
    let worker = service::LoadtestWorkerImpl::new();
    let server = LoadtestWorkerServer::new(worker);

    println!("Starting server on {}", addr);
    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}

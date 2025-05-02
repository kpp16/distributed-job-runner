use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tonic::{transport::Server, Request, Response, Status};

use task::task_service_server::{TaskService, TaskServiceServer};
use task::{TaskRequest, TaskResponse};

use std::time::Duration;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub mod task {
    tonic::include_proto!("task");
}

#[derive(Debug)]
struct TaskClient {
    segment: i32,
    topic_name: String,
    broker: String,
}

#[derive(Debug)]
struct JobRecord {
    job_id: String
}

impl TaskClient {
    fn new(segment: i32) -> Self {
        Self {
            segment,
            topic_name: "task_topic".to_string(),
            broker: "localhost:9092".to_string(),
        }
    }

    async fn produce(&self, job_record: JobRecord) -> Result<()> {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", self.broker.clone())
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        let delivery_status = producer.send(
            FutureRecord::to(&self.topic_name)
                .payload(&format!("Message {}", job_record.job_id))
                .key(&format!("Key {}", self.segment)),
            Duration::from_secs(0)
        ).await;

        tracing::info!("Delivery status: {:?}", delivery_status);
        Ok(())
    }

    async fn process_job(&self, job_id: String) -> Result<(String, String)> {
        tracing::info!("Processing job_id: {:?}", job_id);
        self.produce(JobRecord {job_id: job_id.clone()}).await?;

        Ok((format!("Job {} processed successfully", job_id), job_id))
    }
}

#[tonic::async_trait]
impl TaskService for TaskClient {
    async fn submit_task(&self, request: Request<TaskRequest>) -> std::result::Result<Response<TaskResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let execution_time = req.execution_time;

        tracing::info!(
            "Client {} received job: {} (execution time: {})",
            self.segment, job_id, execution_time
        );

        match self.process_job(job_id).await {
            Ok(res) => {
                Ok(Response::new(TaskResponse {
                    status: res.1
                }))
            },
            Err(e) => {
                Err(Status::internal(format!("Failed to process job {}", e)))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let segment = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1".to_string())
        .parse::<i32>()
        .unwrap_or(1);
    
    let port = 50050 + segment;
    let addr = format!("[::1]:{}", port).parse()?;
    
    let client = TaskClient::new(segment);

    println!("Client for segment {} starting on {}", segment, addr);
    
    Server::builder()
        .add_service(TaskServiceServer::new(client))
        .serve(addr)
        .await?;

    Ok(())
}

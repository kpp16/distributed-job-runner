use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tonic::{transport::Server, Request, Response, Status};

use task::task_service_server::{TaskService, TaskServiceServer};
use task::{TaskRequest, TaskResponse};

use std::time::Duration;
use crate::task::{HeartbeatRequest, HeartbeatResponse};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub mod task {
    tonic::include_proto!("task");
}

#[derive(Debug, Default)]
pub struct TaskClientService {
    segment: i32,
    topic_name: String,
    broker: String,
}

#[derive(Debug, Default)]
struct JobRecord {
    job_id: String
}

impl TaskClientService {
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

#[tonic::async_trait] // This macro is essential for implementing async traits
impl TaskService for TaskClientService {
    // Implementation for the SubmitTask RPC
    async fn submit_task(
        &self,
        request: Request<TaskRequest>, // The incoming request message is wrapped in tonic::Request
    ) -> std::result::Result<Response<TaskResponse>, Status> { // The response is wrapped in tonic::Response and Result
        let req = request.into_inner();
        let job_id = req.job_id;
        let execution_time = req.execution_time;

        tracing::info!(
            "Client {} received job: {} (execution time: {})",
            self.segment, job_id, execution_time
        );

        // Use the process_job helper method
        match self.process_job(job_id.clone()).await { // Clone job_id for the Ok branch
            Ok((_, processed_job_id)) => { // Destructure the tuple from process_job
                Ok(Response::new(TaskResponse {
                    status: format!("Job {} processed successfully", processed_job_id) // Use processed_job_id
                }))
            },
            Err(e) => {
                // Log the error and return a gRPC Status error
                tracing::error!("Error processing job {}: {}", job_id, e);
                Err(Status::internal(format!("Failed to process job {}: {}", job_id, e)))
            }
        }
    }

    // Implementation for the Heartbeat RPC (Corrected name and message types)
    async fn heartbeat( // Changed method name to heartbeat
                        &self,
                        request: Request<HeartbeatRequest>, // Changed request message type
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> { // Changed response message type
        println!("Got a heartbeat");

        // Extract the inner message (using the correct type)
        let heartbeat_request = request.into_inner();

        // Process the heartbeat (e.g., update a timestamp, check if the client is alive)
        println!("Received heartbeat value: {}", heartbeat_request.heartbeat);

        // Create the response message (using the correct type)
        let reply = HeartbeatResponse {
            alive: true, // Indicate that the server is alive
        };
        // Return the response
        Ok(Response::new(reply))
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
    
    let client = TaskClientService::new(segment);

    println!("Client for segment {} starting on {}", segment, addr);
    
    Server::builder()
        .add_service(TaskServiceServer::new(client))
        .serve(addr)
        .await?;

    Ok(())
}

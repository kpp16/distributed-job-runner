use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime};
use tokio::time;
use uuid::Uuid;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::CqlTimestamp;
use futures::stream::TryStreamExt;

use serde::de::StdError;

pub mod task {
    tonic::include_proto!("task");
}
fn parse_run_interval_to_mins(input: &str) -> i64 {
    let (unit_char, num_str) = input.split_at(1);
    let number: i64 = match num_str.parse::<i64>() {
        Ok(n) => n,
        Err(_) => return -1, // Return -1 in case of a parsing error
    };

    match unit_char.to_uppercase().as_str() {
        "D" => {
            if number > 0 {
                number * 24 * 60 // days → minutes
            } else {
                -1
            }
        }
        "H" => {
            if number > 0 && number < 24 {
                number * 60 // hours → minutes
            } else {
                -1
            }
        }
        "M" => {
            if number > 0 && number < 60 {
                number // already in minutes
            } else {
                -1
            }
        }
        _ => -1,
    }
}

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

#[derive(Debug)]
struct JobRecord {
    job_id: Uuid,
    next_execution_time: i64,
    segment: i32,
}

struct Orchestrator {
    session: Session,
    client_connections: Arc<Mutex<HashMap<i32, task::task_service_client::TaskServiceClient<tonic::transport::Channel>>>>,
}

impl Orchestrator {
    async fn new() -> Result<Self> {
        let session: Session = SessionBuilder::new()
                                .known_node("127.0.0.1:9042")
                                .build()
                                .await?;
        Ok(Self {
            session,
            client_connections: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn connect_to_clients(&mut self, client_addresses: HashMap<i32, String>) -> Result<()> {
        let mut connections = self.client_connections.lock().unwrap();

        for (segment, address) in client_addresses {
            tracing::info!("Connecting client for segment {}: {}", segment, address);

            let client = task::task_service_client::TaskServiceClient::connect(address).await?;
            connections.insert(segment, client);
        }
        Ok(())
    }

    async fn fetch_due_jobs(&self) -> Result<Vec<JobRecord>> {
        let now_mins = (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() / 60) as i64;
        let session = &self.session;
        let mut results: Vec<JobRecord> = Vec::new();

        let mut iter = session.query_iter("SELECT next_execution_time, job_id, segment FROM dist_task_runner.task_schedule WHERE next_execution_time <= ?", (now_mins, ))
            .await?
            .rows_stream::<(i64, Uuid, i32)>()?;

        while let Some(read_row) = iter.try_next().await? {
            let execution_time = read_row.0;
            let job_id = read_row.1;
            let segment = read_row.2;

            results.push(JobRecord{
                job_id, next_execution_time: execution_time, segment,
            });

            // calculate and update the next execution time
            let int_res = session.query_unpaged("SELECT run_interval FROM dist_task_runner.task_schedule WHERE job_od = ?", (job_id,))
                .await?
                .into_rows_result()?;

            let mut run_int: String = "".to_string();

            let mut iter_int = int_res.rows::<(String,)>()?;

            while let Some(read_int_row) = iter_int.next().transpose()? {
                run_int = read_int_row.0;
                break;
            }

            let next_time_delta = parse_run_interval_to_mins(&run_int);

            if next_time_delta != -1 {
                let next_time_ms = now_mins + next_time_delta;
                let next_time_ts = CqlTimestamp(next_time_ms);
                session.query_unpaged("UPDATE dist_task_runner.task_schedule SET next_execution_time = ? WHERE job_id = ?", (next_time_ts, job_id))
                    .await?;
            }
        }
        Ok(results)
    }

    async fn dispatch_jobs(&mut self, jobs: Vec<JobRecord>) -> Result<()> {
        for job in jobs {
            let segment = job.segment;

            if let Some(client) = self.client_connections.lock().unwrap().get(&job.segment) {
                let mut client = client.clone();

                tracing::info!("Dispatching job {:?} to segment {}", job, segment);

                let request = tonic::Request::new(task::TaskRequest {
                    job_id: job.job_id.to_string(),
                    execution_time: job.next_execution_time,
                });

                match client.submit_task(request).await {
                    Ok(response) => {
                        tracing::info!(
                            "Job {} successfully dispatched to client {}: {}",
                            job.job_id, segment, response.into_inner().status
                        );
                    },
                    Err(e) => {
                        tracing::error!("Failed to dispatch job {:?} to segment {}: {}", job, segment, e);
                    }
                }
            } else {
                // TODO: redistribute segments hashmap
                tracing::error!("No client connected for segment {}", segment);
            }

        }
        Ok(())
    }

    async fn run(&mut self, interval_seconds: u64) {
        let mut interval = time::interval(time::Duration::from_secs(interval_seconds));

        loop {
            interval.tick().await;

            match self.fetch_due_jobs().await {
                Ok(jobs) => {
                    if !jobs.is_empty() {
                        if let Err(e) = self.dispatch_jobs(jobs).await {
                            tracing::error!("Failed to dispatch jobs: {}", e);
                        }
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to fetch jobs: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let mut orchestrator = Orchestrator::new().await?;

    let mut client_addresses = HashMap::new();
    
    client_addresses.insert(1, "http://[::1]:50051".to_string());
    client_addresses.insert(2, "http://[::1]:50052".to_string());
    client_addresses.insert(3, "http://[::1]:50053".to_string());
    client_addresses.insert(4, "http://[::1]:50054".to_string());
    client_addresses.insert(5, "http://[::1]:50055".to_string());

    orchestrator.connect_to_clients(client_addresses).await?;

    tracing::info!("Orchestrator started. Checking for jobs every 60 seconds...");
    orchestrator.run(60).await;

    Ok(())
}
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::time;
use uuid::Uuid;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::CqlTimestamp;
use futures::stream::TryStreamExt;
use rand::prelude::IndexedRandom;
use serde::de::StdError;
use crate::task::{HeartbeatRequest};
use tokio::{select};

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

#[derive(Debug, Clone)]
struct JobRecord {
    job_id: Uuid,
    next_execution_time: i64,
    segment: i32,
}

#[derive(Clone)]
struct ClientStatus {
    last_execution_time: SystemTime,
    status: bool,
}

struct Orchestrator {
    session: Session,
    client_connections: Arc<Mutex<HashMap<String, task::task_service_client::TaskServiceClient<tonic::transport::Channel>>>>,
    client_statuses: Arc<Mutex<HashMap<String, ClientStatus>>>,
    client_addresses: Arc<Mutex<HashMap<String, Vec<i32>>>>,
    segment_addresses: Arc<Mutex<HashMap<i32, String>>>
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
            client_statuses: Arc::new(Mutex::new(HashMap::new())),
            client_addresses: Arc::new(Mutex::new(HashMap::new())),
            segment_addresses: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn initial_connect_to_clients(&mut self, client_addresses: HashMap<String, Vec<i32>>) -> Result<()> {
        let mut connections = self.client_connections.lock().unwrap();

        self.client_addresses = Arc::new(Mutex::new(client_addresses.clone()));

        for (address, segments) in client_addresses {
            for segment in segments.clone() {
                tracing::info!("Connecting client for segment {}: {}", segment, address);

                let client = task::task_service_client::TaskServiceClient::connect(address.clone()).await?;
                connections.insert(address.clone(), client);
                self.segment_addresses.lock().unwrap().insert(segment, address.clone());
            }
        }
        Ok(())
    }

    async fn rebalance_segment_jobs(&mut self) -> Result<()> {
        tracing::info!("rebalance segment jobs");

        let mut dead_segments: Vec<i32> = Vec::new();
        let mut alive_connections: Vec<String> = Vec::new();

        for (connection, client_status) in &self.client_statuses.lock().unwrap().clone() {
            if !client_status.status {
                let cur_dead_segments = self.client_addresses.lock().unwrap().get(connection).unwrap().clone();
                for dead_segment in cur_dead_segments {
                    dead_segments.push(dead_segment.clone());
                }
                self.client_connections.lock().unwrap().remove(connection).unwrap();
            } else {
                alive_connections.push(connection.clone());
            }
        }

        for dead_segment in dead_segments {
            let mut rng = rand::rng();
            let conn = alive_connections.choose(&mut rng);

            match conn {
                Some(conn) => {
                    tracing::info!("rebalancing dead segment {} to {}", dead_segment, conn);
                    let mut segments = self.client_addresses.lock().unwrap().get(&conn.clone()).unwrap_or(&vec![]).clone();
                    segments.push(dead_segment);
                    self.client_addresses.lock().unwrap().insert(conn.clone(), segments.clone());
                    self.segment_addresses.lock().unwrap().insert(dead_segment, conn.clone());
                },
                _ => {
                    tracing::error!("Failed to rebalance connection");
                }
            }
        }

        Ok(())
    }

    async fn perform_heartbeat(&mut self) -> Result<()> {
        let mut connections = self.client_connections.lock().unwrap().clone();

        for (connection, client) in connections.iter_mut() {
            let heart_beat_request = HeartbeatRequest {
                heartbeat: 1,
            };
            tracing::info!("Sending heartbeat request");
            let tonic_request = tonic::Request::new(heart_beat_request);

            let default_cs = ClientStatus {
                last_execution_time: SystemTime::now(),
                status: false
            };
            let mut client_status: ClientStatus = self.client_statuses.lock().unwrap().get(connection).unwrap_or(&default_cs.clone()).clone();

            match client.heartbeat(tonic_request).await {
                Ok(response) => {
                    tracing::info!("Received heartbeat response");
                    let response = response.into_inner();
                    client_status = ClientStatus {
                        last_execution_time: SystemTime::now(),
                        status: response.alive
                    };
                    self.client_statuses.lock().unwrap().insert(connection.clone(), client_status);
                },
                Err(e) => {
                    tracing::error!("Failed to receive heartbeat response for connection: {}. Error: {}", connection, e);
                    self.client_statuses.lock().unwrap().insert(connection.clone(), client_status.clone());
                    self.rebalance_segment_jobs().await?;
                }
            }
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

            if let Some(client_addr) = self.segment_addresses.lock().unwrap().get(&job.segment) {
                let mut client = self.client_connections.lock().unwrap().get(client_addr).unwrap().clone();

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
                // Shouldn't arrive here due to the heart beat system running more frequently than dispatch jobs
                // but IDK
                tracing::error!("No client connected for segment {}. Retry submitting job", segment);
            }
        }
        Ok(())
    }

    async fn run(&mut self, job_interval_secs: u64, heartbeat_interval_secs: u64) {
        let mut job_interval = time::interval(Duration::from_secs(job_interval_secs));
        let mut heartbeat_interval = time::interval(Duration::from_secs(heartbeat_interval_secs));

        loop {
            select! {
                _ = job_interval.tick() => {
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
                _ = heartbeat_interval.tick() => {
                    if let Err(e) = self.perform_heartbeat().await {
                        tracing::error!("Heartbeat failed: {}", e);
                    }
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
    
    client_addresses.insert("http://[::1]:50051".to_string(), vec![1]);
    client_addresses.insert("http://[::1]:50052".to_string(), vec![2]);
    client_addresses.insert("http://[::1]:50053".to_string(), vec![3]);
    client_addresses.insert("http://[::1]:50054".to_string(), vec![4]);
    client_addresses.insert("http://[::1]:50055".to_string(), vec![5]);

    orchestrator.initial_connect_to_clients(client_addresses).await?;

    tracing::info!("Orchestrator started. Checking for jobs every 60 seconds...");
    orchestrator.run(60, 10).await;

    Ok(())
}
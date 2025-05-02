use axum::{
    routing::post,
    Json, Router
};

use serde::{Deserialize, Serialize};
use tracing_subscriber;
use uuid::Uuid;

// use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
#[derive(Debug)]
struct JobRequest {
    user_id: i32,
    is_recurring: bool,
    run_interval: String,
    max_retry_count: i32,
}

#[derive(Serialize)]
struct JobResponse {
    job_id: Uuid,
    status: String,
}

#[derive(Debug)]
struct JobTableType {
    user_id: i32,
    job_id: Uuid,
    is_recurring: bool,
    run_interval: String,
    max_retry_count: i32,
    created_time: i64
}

#[derive(Debug)]
pub enum IntervalParseError {
    InvalidFormat,
    InvalidNumber,
    OutOfRange,
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
                return number * 24 * 60 // days → minutes
            } else {
                return -1
            }
        }
        "H" => {
            if number > 0 && number < 24 {
                return number * 60 // hours → minutes
            } else {
                return -1
            }
        }
        "M" => {
            if number > 0 && number < 60 {
                return number // already in minutes
            } else {
                return -1
            }
        }
        _ => return -1,
    }
}

fn get_unix_time_mins() -> i64 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    let minutes = since_epoch.as_secs() / 60;
    return minutes as i64;
}

async fn insert_job_table(payload: JobTableType) -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("CASS_URI")
    .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
                            .known_node(uri)
                            .build()
                            .await?;
    
    tracing::info!("job_table: Trying to insert table data: {:#?}", payload);

    let res = session
        .query_unpaged("INSERT INTO dist_task_runner.job_table (user_id, job_id, is_recurring, run_interval, max_retry_count, created_time) VALUES (?, ?, ?, ?, ?, ?)",
        (payload.user_id, payload.job_id, payload.is_recurring, payload.run_interval, payload.max_retry_count, payload.created_time)).await?;
    
    tracing::info!("job_table: Query result: {:#?}", res);

    Ok(())
}

async fn insert_task_schedule(job_id: Uuid, timestamp: i64) -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("CASS_URI")
    .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
                            .known_node(uri)
                            .build()
                            .await?;
    
    tracing::info!("task_schedule: Trying to insert table data: {:#?}", (job_id, timestamp));

    let res = session
        .query_unpaged("INSERT INTO dist_task_runner.task_schedule (next_execution_time, job_id) VALUES (?, ?)",
        (timestamp, job_id)).await?;
    
    tracing::info!("task_schedule: Query result: {:#?}", res);

    Ok(())
}

// Modified handler function that directly integrates with Axum's routing
async fn handle_submit_job(
    Json(payload): Json<JobRequest>,
) -> Json<JobResponse> {
    tracing::info!("Receive payload: {:#?}", payload);
    let job_id = Uuid::new_v4();
    let unix_create_time = get_unix_time_mins();
    
    // Prepare the table data
    let table_data = JobTableType {
        user_id: payload.user_id,
        job_id,
        is_recurring: payload.is_recurring,
        run_interval: payload.run_interval.clone(),
        max_retry_count: payload.max_retry_count,
        created_time: unix_create_time,
    };

    let interval_mins = parse_run_interval_to_mins(&payload.run_interval);
        
    // Check if interval is valid
    if interval_mins == -1 {
        tracing::info!("Invalid run interval: {}", payload.run_interval);
        return Json(JobResponse {
            job_id,
            status: "error".into(),
        });
    }    

    // Insert job record
    if let Err(e) = insert_job_table(table_data).await {
        tracing::info!("Error inserting data: {e:?}");
        return Json(JobResponse {
            job_id,
            status: "error".into(),
        });
    }
    
    tracing::info!("Successfully inserted data");
    
    // Handle recurring jobs
    if payload.is_recurring {
        
        // Calculate next run time
        let next_run = unix_create_time + interval_mins;
        
        // Schedule the next run
        if let Err(e) = insert_task_schedule(job_id, next_run).await {
            tracing::info!("Error inserting task_schedule: {e:?}");
            return Json(JobResponse {
                job_id,
                status: "error".into(),
            });
        }
    }
    
    // Return success
    Json(JobResponse {
        job_id,
        status: "submitted".into(),
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    
    // Create the router with the handler function directly
    let app = Router::new()
        .route("/submit_job", post(handle_submit_job));
        
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("Listening on 0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

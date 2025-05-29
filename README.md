# Distributed Job Runner
A scalable, fault-tolerant distributed job scheduling system built with Rust, Apache Cassandra, and Apache Kafka. Designed to decouple job intake, scheduling, and execution using modular microservices, enabling high availability and observability across services.

## ✨ Overview
The Distributed Job Runner is composed of three key services:

1. Job Service: Handles HTTP job submission requests and stores metadata in Apache Cassandra.
2. Scheduling Service: Periodically polls the database, delegates jobs to workers via RPC, and sends jobs to Kafka.
3. Scheduling Worker Pool: Listens for assignments, sends heartbeat signals, and publishes jobs to Kafka for downstream execution.

Built using Tokio for asynchronous concurrency and gRPC/RPC for inter-service communication. Easily deployable via Kubernetes, with support for scaling, failure detection, and load redistribution.

## Components
### ✅ Job Service (`job_service`)
1. Accepts HTTP job submission requests (job metadata + scheduling info).
2. Persists job and schedule data to Apache Cassandra.
3. Stateless, horizontally scalable via Kubernetes.

### ⏰ Scheduling Service (`scheduling_service`)
1. Runs periodic tasks (every minute) to check for due jobs in Cassandra.
2. Sends job data to active scheduling workers using RPC.
3. Maintains heartbeat monitoring and redistributes jobs if workers fail.

### 🛠️ Scheduling Workers (`scheduling_service_worker`)
1. Receive jobs via RPC, store assigned segment IDs.
2. Publish job payloads to Apache Kafka for execution by downstream consumers.
3. Send regular heartbeats to the scheduler.

## 🗄️ Cassandra Schema
```cql
-- Keyspace
CREATE KEYSPACE IF NOT EXISTS dist_task_runner
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Job metadata
CREATE TABLE dist_task_runner.job_table(
    user_id int,
    job_id uuid,
    is_recurring boolean,
    run_interval text,
    max_retry_count int,
    created_time bigint,
    PRIMARY KEY((user_id), job_id)
) WITH CLUSTERING ORDER BY (job_id ASC);

-- Job scheduling metadata
CREATE TABLE dist_task_runner.task_schedule(
    next_execution_time bigint,
    job_id uuid,
    segment int,
    PRIMARY KEY((next_execution_time, segment), job_id)
) WITH CLUSTERING ORDER BY (job_id ASC);

-- Execution history
CREATE TABLE dist_task_runner.task_history (
    job_id uuid,
    execution_time bigint,
    status text,
    retry_count int,
    last_update_time bigint,
    PRIMARY KEY ((job_id), execution_time)
) WITH CLUSTERING ORDER BY (execution_time ASC);
```

## 🛠️ Technologies Used
1. Rust + Tokio – Async microservices
2. Apache Cassandra – Distributed NoSQL database
3. Apache Kafka – Distributed log/message queue
4. gRPC / RPC – Service communication
5. Kubernetes – Service orchestration and scaling

## Future Work
1. Add Web UI for job monitoring and manual triggering
2. Add distributed tracing and metrics (OpenTelemetry)
3. Support CRON expressions or ISO 8601 repeat rules

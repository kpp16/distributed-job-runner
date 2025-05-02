CREATE KEYSPACE IF NOT EXISTS dist_task_runner 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE dist_task_runner.job_table(
        user_id int,
        job_id uuid,
        is_recurring boolean,
        run_interval text,
        max_retry_count int,
        created_time bigint,
        PRIMARY KEY((user_id), job_id)        
) WITH CLUSTERING ORDER BY (job_id ASC);


CREATE TABLE dist_task_runner.task_schedule(
        next_execution_time bigint,
        job_id uuid,
        segment int,
        PRIMARY KEY((next_execution_time, segment), job_id)
) WITH CLUSTERING ORDER BY (job_id ASC);


CREATE TABLE dist_task_runner.task_history (
    job_id uuid,
    execution_time bigint,
    status text,
    retry_count int,
    last_update_time bigint,
    PRIMARY KEY ((job_id), execution_time)
) WITH CLUSTERING ORDER BY (execution_time ASC);
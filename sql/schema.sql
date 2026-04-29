-- PostgreSQL Schema for ETL Project

-- 1. Metadata Table
CREATE TABLE IF NOT EXISTS run_metadata (
    run_id SERIAL PRIMARY KEY,
    execution_id INTEGER NOT NULL,
    pipeline_name VARCHAR(20) NOT NULL, -- mapreduce, mongodb, hive, pig
    batch_id INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    avg_batch_size FLOAT,
    runtime FLOAT,
    malformed_record_count INTEGER,
    dataset_name VARCHAR(255),
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Daily Traffic Summary (Query 1)
CREATE TABLE IF NOT EXISTS daily_traffic (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    log_date DATE,
    status_code INTEGER,
    request_count BIGINT,
    total_bytes BIGINT
);

-- 3. Top Requested Resources (Query 2)
CREATE TABLE IF NOT EXISTS top_resources (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT
);

-- 4. Hourly Error Analysis (Query 3)
CREATE TABLE IF NOT EXISTS hourly_errors (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES run_metadata(run_id),
    log_date DATE,
    log_hour SMALLINT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate FLOAT,
    distinct_error_hosts BIGINT
);

-- ClickHouse base initialization — runs on first container start regardless of profile.
-- Creates only the MergeTree storage table.
--
-- Kafka Engine + MaterializedView are created by `clickhouse-flink-init` (profiles: [flink])
-- so they only exist when the flink profile is active.
-- With the bento profile, Bento inserts directly into this MergeTree table.

CREATE DATABASE IF NOT EXISTS web_logs;

CREATE TABLE IF NOT EXISTS web_logs.web_logs
(
    request_id  String,
    method      LowCardinality(String),
    path        String,
    status_code UInt16,
    latency_ms  UInt32,
    ip          LowCardinality(String),
    user_agent  String,
    ts          String
)
ENGINE = MergeTree()
ORDER BY (ts, request_id)
SETTINGS index_granularity = 8192;

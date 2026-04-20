-- =============================================================================
-- web-logs-to-clickhouse.sql
-- Flink SQL streaming job: Kafka source (web-logs) → ClickHouse sink
--
-- Submit via:
--   docker compose exec flink-jobmanager \
--     /opt/flink/bin/sql-client.sh -f /opt/flink/sql/web-logs-to-clickhouse.sql
--
-- Flink 1.20  |  ClickHouse 24.8  |  flink-connector-jdbc 3.2.0
-- Uses a DIFFERENT consumer group than the Iceberg job so both jobs consume
-- independently from the same web-logs topic.
-- All DDL statements are idempotent (IF NOT EXISTS) — safe to re-run.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Kafka source table
--    Consumer group: flink-web-logs-clickhouse (distinct from flink-web-logs-iceberg)
--    scan.startup.mode: earliest-offset — consume all buffered messages first
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_web_logs_ch (
    `request_id`  STRING,
    `method`      STRING,
    `path`        STRING,
    `status_code` INT,
    `latency_ms`  INT,
    `ip`          STRING,
    `user_agent`  STRING,
    `ts`          STRING
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'web-logs',
    'properties.bootstrap.servers'      = 'kafka:9092',
    'properties.group.id'               = 'flink-web-logs-clickhouse',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'json',
    'json.ignore-parse-errors'          = 'true'
);

-- ---------------------------------------------------------------------------
-- 2. ClickHouse JDBC sink table
--    JDBC URL: jdbc:clickhouse://clickhouse:8123/web_logs
--    Driver: com.clickhouse.jdbc.ClickHouseDriver (from clickhouse-jdbc-all jar)
--    Buffer: flush every 5s or 1000 rows — balances throughput vs latency
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickhouse_web_logs (
    `request_id`  STRING,
    `method`      STRING,
    `path`        STRING,
    `status_code` INT,
    `latency_ms`  INT,
    `ip`          STRING,
    `user_agent`  STRING,
    `ts`          STRING
) WITH (
    'connector'                     = 'jdbc',
    'url'                           = 'jdbc:clickhouse://clickhouse:8123/web_logs',
    'table-name'                    = 'web_logs',
    'driver'                        = 'com.clickhouse.jdbc.ClickHouseDriver',
    'username'                      = 'default',
    'password'                      = '',
    'sink.buffer-flush.max-rows'    = '1000',
    'sink.buffer-flush.interval'    = '5s',
    'sink.max-retries'              = '3'
);

-- ---------------------------------------------------------------------------
-- 3. Streaming INSERT — runs continuously until cancelled.
--    Rows are buffered and flushed to ClickHouse in batches.
-- ---------------------------------------------------------------------------
INSERT INTO clickhouse_web_logs
SELECT
    `request_id`,
    `method`,
    `path`,
    `status_code`,
    `latency_ms`,
    `ip`,
    `user_agent`,
    `ts`
FROM kafka_web_logs_ch;

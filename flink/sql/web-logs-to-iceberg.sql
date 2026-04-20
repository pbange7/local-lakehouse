-- =============================================================================
-- web-logs-to-iceberg.sql
-- Flink SQL streaming job: Kafka source (web-logs) → Iceberg sink (Nessie/MinIO)
--
-- Submit via:
--   docker compose exec flink-jobmanager \
--     /opt/flink/bin/sql-client.sh -f /opt/flink/sql/web-logs-to-iceberg.sql
--
-- Flink 1.20  |  Iceberg 1.7.2  |  Nessie 0.76.6 (API v1)
-- All DDL statements are idempotent (IF NOT EXISTS) — safe to re-run.
-- Column names are backtick-quoted: `method` and `path` are reserved keywords
-- in Flink SQL and must be quoted to avoid parse errors.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Create the Nessie-backed Iceberg catalog
--    NessieCatalog speaks Nessie REST API v1 (/api/v1).
--    S3FileIO writes Parquet files directly to MinIO.
--    warehouse path: s3://lakehouse/warehouse  (bucket=lakehouse, prefix=warehouse)
-- ---------------------------------------------------------------------------
CREATE CATALOG nessie_catalog WITH (
    'type'                 = 'iceberg',
    'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
    'uri'                  = 'http://nessie:19120/api/v1',
    'ref'                  = 'main',
    'warehouse'            = 's3://lakehouse/warehouse',
    'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'          = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id'     = 'lakehouse',
    's3.secret-access-key' = 'lakehouse123',
    's3.region'            = 'us-east-1'
);

-- ---------------------------------------------------------------------------
-- 2. Create the Iceberg database (= Nessie namespace on branch main)
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS nessie_catalog.web_logs;

-- ---------------------------------------------------------------------------
-- 3. Create the Iceberg sink table
--    Partitioned by status_code for efficient HTTP-status-class queries.
--    Parquet/Snappy is the Iceberg default and the best choice for analytics.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie_catalog.web_logs.web_logs (
    `request_id`  STRING,
    `method`      STRING,
    `path`        STRING,
    `status_code` INT,
    `latency_ms`  INT,
    `ip`          STRING,
    `user_agent`  STRING,
    `ts`          STRING
) PARTITIONED BY (`status_code`)
WITH (
    'write.format.default'                         = 'parquet',
    'write.parquet.compression-codec'              = 'snappy',
    'write.metadata.delete-after-commit.enabled'   = 'true',
    'write.metadata.previous-versions-max'         = '10'
);

-- ---------------------------------------------------------------------------
-- 4. Create the Kafka source table (in default catalog)
--    scan.startup.mode=earliest-offset: consume all buffered messages first,
--    then continue reading new ones.
--    ingest_ts is a PROCTIME computed column (not written to Iceberg).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_web_logs (
    `request_id`  STRING,
    `method`      STRING,
    `path`        STRING,
    `status_code` INT,
    `latency_ms`  INT,
    `ip`          STRING,
    `user_agent`  STRING,
    `ts`          STRING,
    `ingest_ts`   AS PROCTIME()
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'web-logs',
    'properties.bootstrap.servers'      = 'kafka:9092',
    'properties.group.id'               = 'flink-web-logs-iceberg',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'json',
    'json.ignore-parse-errors'          = 'true'
);

-- ---------------------------------------------------------------------------
-- 5. Streaming INSERT — runs continuously until the job is cancelled.
--    ingest_ts (PROCTIME computed column) is excluded from the projection
--    because it has no corresponding column in the Iceberg sink table.
--    Iceberg snapshots are committed on each Flink checkpoint (every 60s).
-- ---------------------------------------------------------------------------
INSERT INTO nessie_catalog.web_logs.web_logs
SELECT
    `request_id`,
    `method`,
    `path`,
    `status_code`,
    `latency_ms`,
    `ip`,
    `user_agent`,
    `ts`
FROM kafka_web_logs;

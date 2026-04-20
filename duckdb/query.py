#!/usr/bin/env python3
"""
DuckDB Iceberg query script
Reads Iceberg tables directly from MinIO S3 storage.
Run via: docker run --rm --network lakehouse_lakehouse ... python:3.12-slim bash -c "pip install duckdb boto3 -q && python query.py"
"""
import os
import sys
import boto3
import duckdb

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "lakehouse")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "lakehouse123")
BUCKET          = "lakehouse"
WAREHOUSE_PREFIX = "warehouse/web_logs/"

# ---------------------------------------------------------------------------
# 1. Discover Iceberg table directory from MinIO
# ---------------------------------------------------------------------------
print(f"[INFO] Discovering Iceberg table path from s3://{BUCKET}/{WAREHOUSE_PREFIX} ...")
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    region_name="us-east-1",
)

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=WAREHOUSE_PREFIX, Delimiter="/")
table_dirs = [
    p["Prefix"] for p in resp.get("CommonPrefixes", [])
    if "/web_logs_" in p["Prefix"]
]

if not table_dirs:
    print(f"[FAIL] No Iceberg table directory found under s3://{BUCKET}/{WAREHOUSE_PREFIX}")
    sys.exit(1)

# Use the first match (most recent table if multiple)
table_path = f"s3://{BUCKET}/{table_dirs[0].rstrip('/')}"
print(f"[INFO] Found table: {table_path}")

# ---------------------------------------------------------------------------
# 2. Configure DuckDB with httpfs + iceberg extensions and MinIO S3 settings
# ---------------------------------------------------------------------------
print("[INFO] Configuring DuckDB extensions and S3 connection...")
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
con.execute(f"SET s3_access_key_id='{MINIO_ACCESS}';")
con.execute(f"SET s3_secret_access_key='{MINIO_SECRET}';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_region='us-east-1';")
# Enable automatic version detection — finds the latest Iceberg metadata snapshot.
# Safe for local dev; in production, pin to a specific snapshot ID instead.
con.execute("SET unsafe_enable_version_guessing=true;")

# ---------------------------------------------------------------------------
# 3. Row count query
# ---------------------------------------------------------------------------
print("[INFO] Running: SELECT count(*) FROM iceberg_scan(...)")
count_result = con.execute(f"SELECT count(*) AS cnt FROM iceberg_scan('{table_path}')").fetchone()
row_count = count_result[0]
if row_count == 0:
    print("[FAIL] DuckDB returned 0 rows from Iceberg scan.")
    sys.exit(1)
print(f"[PASS] Row count: {row_count}")

# ---------------------------------------------------------------------------
# 4. Aggregate by status_code
# ---------------------------------------------------------------------------
print("\n[INFO] Running aggregate query by status_code...")
rows = con.execute(f"""
    SELECT
        status_code,
        count(*)          AS cnt,
        round(avg(latency_ms), 1) AS avg_latency_ms
    FROM iceberg_scan('{table_path}')
    GROUP BY status_code
    ORDER BY status_code
""").fetchall()

print(f"\n  {'status_code':<14} {'count':>10} {'avg_latency_ms':>16}")
print(f"  {'-'*14} {'-'*10} {'-'*16}")
for r in rows:
    print(f"  {r[0]:<14} {r[1]:>10} {r[2]:>16}")

print(f"\n[PASS] DuckDB Iceberg scan complete — {len(rows)} distinct status_code values.")
print(f"\n==> PASS: DuckDB read {row_count} rows from Iceberg table in MinIO.")

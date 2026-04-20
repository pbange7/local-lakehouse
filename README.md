# Local Lakehouse

A fully local, Docker-based lakehouse built with Kafka, Flink, Iceberg, ClickHouse, Trino, DuckDB, FastAPI, and a web UI. Streams synthetic web-access-log events end-to-end: from a Python producer through Flink into Iceberg (MinIO) and ClickHouse, queryable via a SQL workbench at `http://localhost:3000`.

---

## Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| Docker Desktop | 4.x | Allocate ≥ 6 GB RAM in Docker settings |
| bash | any | macOS/Linux |
| curl | any | Used by smoke tests |
| Python 3.12 | optional | Only needed for `make test-unit` / `make test-integration` |

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url> local-lakehouse && cd local-lakehouse

# 2. Generate the Kafka cluster ID (.env is gitignored; .env.example is the template)
make seed

# 3. Start all services (Kafka, MinIO, Nessie, Flink, ClickHouse, Trino, API, UI)
#    First run builds custom images — takes ~3 min on a fast connection
make up

# 4. Submit the Flink streaming job (Kafka → Iceberg)
make flink-submit

# 5. Open the SQL workbench
open http://localhost:3000
```

Data starts flowing into Iceberg after the first Flink checkpoint (~60s). ClickHouse receives events in near-real-time via its native Kafka engine.

---

## Run the Integration Test

```bash
make smoke-all
```

Runs 11 checks across every component and prints a single summary table. Exit code 0 means all pass.

---

## Service Ports

| Port | Service | URL |
|---|---|---|
| 3000 | Web UI (SQL workbench) | http://localhost:3000 |
| 8000 | FastAPI backend | http://localhost:8000/docs |
| 8080 | Trino Web UI | http://localhost:8080 |
| 8082 | Flink Web UI | http://localhost:8082 |
| 8123 | ClickHouse HTTP API | http://localhost:8123/play |
| 9000 | MinIO S3 API | — |
| 9001 | MinIO Console | http://localhost:9001 |
| 19120 | Nessie REST API | http://localhost:19120/api/v2/config |
| 9092 | Kafka (internal) | — |
| 9094 | Kafka (host access) | — |

---

## Key Make Targets

| Target | Description |
|---|---|
| `make seed` | Generate `KAFKA_CLUSTER_ID` into `.env` (one-time setup) |
| `make up` | Start all services, wait for health checks |
| `make down` | Stop and remove containers (volumes preserved) |
| `make clean` | Stop containers and delete all volumes (data loss!) |
| `make build-flink` | Rebuild the custom Flink image (downloads connector JARs) |
| `make flink-submit` | Submit the Kafka → Iceberg streaming SQL job |
| `make clickhouse-submit` | Verify the ClickHouse Kafka consumer is active |
| `make smoke-all` | Consolidated 11-point integration test |
| `make test` | Unit tests + smoke-all + integration tests |
| `make logs-<service>` | Tail logs for a specific service |
| `make status` | Show container health status |
| `make help` | List all available targets |

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for:
- Component diagram (data flow)
- Sequence diagram — ingestion
- Sequence diagram — query
- Full port reference
- Technology stack table

---

## Data Schema

The Python producer emits synthetic web-access-log events at 10 msg/s (configurable via `MESSAGES_PER_SECOND`):

```json
{
  "request_id":  "uuid4",
  "method":      "GET",
  "path":        "/api/products/42",
  "status_code": 200,
  "latency_ms":  87,
  "ip":          "203.0.113.42",
  "user_agent":  "Mozilla/5.0 ...",
  "ts":          "2026-04-20T10:31:00.123Z"
}
```

Status codes and latencies are weighted to simulate realistic traffic distributions.

---

## Example Queries

**DuckDB (direct Iceberg read):**
```sql
SELECT method, status_code, count(*) AS cnt, round(avg(latency_ms), 1) AS avg_ms
FROM web_logs.web_logs
GROUP BY method, status_code
ORDER BY cnt DESC
LIMIT 20;
```

**Trino (federated — Iceberg):**
```sql
SELECT status_code, count(*) AS cnt, avg(latency_ms) AS avg_ms
FROM iceberg.web_logs.web_logs
GROUP BY status_code ORDER BY status_code;
```

**Trino (cross-catalog join — Iceberg + ClickHouse):**
```sql
SELECT i.status_code, i.iceberg_rows, c.clickhouse_rows
FROM (SELECT status_code, count(*) AS iceberg_rows FROM iceberg.web_logs.web_logs GROUP BY 1) i
JOIN (SELECT status_code, count(*) AS clickhouse_rows FROM clickhouse.web_logs.web_logs GROUP BY 1) c
  ON i.status_code = c.status_code
ORDER BY i.status_code;
```

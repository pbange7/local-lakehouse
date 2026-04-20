# Local Lakehouse — Project Plan

> Docker-based local Lakehouse with Kafka, Flink, Iceberg, ClickHouse, DuckDB, Trino, FastAPI, and a web UI.

## Architecture Overview

```
Producer (Python) → Kafka → Flink (Consumer) → Iceberg (RustFS object store)
                                             → ClickHouse (analytics store)

Query Layer: DuckDB / Trino → Iceberg & ClickHouse

API Layer:   FastAPI (Python) → DuckDB / Trino

UI Layer:    Web UI → FastAPI
```

---

## Phases & Tasks

### Phase 1 — Foundation & Infrastructure
| # | Task | Status | Notes |
|---|------|--------|-------|
| 1.1 | Create base `docker-compose.yml` with networking | ⬜ TODO | Shared bridge network for all services |
| 1.2 | Add Kafka (latest, KRaft mode — no ZooKeeper) | ⬜ TODO | Single broker for local dev |
| 1.3 | Add Schema Registry (optional, for Avro/Protobuf) | ⬜ TODO | Confluent or Apicurio |
| 1.4 | Verify Kafka is healthy (topic create/list) | ⬜ TODO | Smoke test script |

### Phase 2 — Python Kafka Producer
| # | Task | Status | Notes |
|---|------|--------|-------|
| 2.1 | Create Python producer project (`producer/`) | ⬜ TODO | `confluent-kafka` or `aiokafka` |
| 2.2 | Implement configurable message generator | ⬜ TODO | JSON messages, configurable rate & schema |
| 2.3 | Dockerize the producer | ⬜ TODO | Dockerfile + compose service |
| 2.4 | Test end-to-end: producer → Kafka topic | ⬜ TODO | Console consumer verification |

### Phase 3 — Object Store (RustFS / MinIO)
| # | Task | Status | Notes |
|---|------|--------|-------|
| 3.1 | Add S3-compatible object store to compose | ⬜ TODO | RustFS or MinIO for local S3 API |
| 3.2 | Create buckets and access credentials | ⬜ TODO | Initialization script |
| 3.3 | Verify S3 API access from other containers | ⬜ TODO | AWS CLI or boto3 smoke test |

### Phase 4 — Apache Iceberg Catalog
| # | Task | Status | Notes |
|---|------|--------|-------|
| 4.1 | Add Iceberg REST catalog (e.g., Nessie or Tabular REST) | ⬜ TODO | Catalog backed by object store |
| 4.2 | Configure catalog to use RustFS/MinIO warehouse | ⬜ TODO | S3 endpoint, bucket, credentials |
| 4.3 | Verify catalog health and namespace creation | ⬜ TODO | REST API smoke test |

### Phase 5 — Flink: Kafka Consumer & Iceberg Writer
| # | Task | Status | Notes |
|---|------|--------|-------|
| 5.1 | Add Flink cluster to compose (JobManager + TaskManager) | ⬜ TODO | Flink 1.18+ with Iceberg connector |
| 5.2 | Write Flink SQL job: Kafka source → Iceberg sink | ⬜ TODO | Streaming insert into Iceberg table |
| 5.3 | Package required JARs (Kafka connector, Iceberg, S3 FS) | ⬜ TODO | Custom Flink image or volume-mount JARs |
| 5.4 | Submit job and verify data lands in Iceberg | ⬜ TODO | Check object store files + catalog metadata |

### Phase 6 — ClickHouse Analytics Store
| # | Task | Status | Notes |
|---|------|--------|-------|
| 6.1 | Add ClickHouse to compose | ⬜ TODO | Single-node, with init SQL |
| 6.2 | Create target tables in ClickHouse | ⬜ TODO | Schema matching Kafka message format |
| 6.3 | Write Flink SQL job: Kafka source → ClickHouse sink | ⬜ TODO | JDBC or ClickHouse Flink connector |
| 6.4 | Verify data in ClickHouse | ⬜ TODO | `clickhouse-client` query |

### Phase 7 — Query Engines (DuckDB & Trino)
| # | Task | Status | Notes |
|---|------|--------|-------|
| 7.1 | Add Trino to compose with Iceberg & ClickHouse catalogs | ⬜ TODO | `catalog/iceberg.properties`, `catalog/clickhouse.properties` |
| 7.2 | Verify Trino can query Iceberg tables | ⬜ TODO | Trino CLI or REST query |
| 7.3 | Verify Trino can query ClickHouse tables | ⬜ TODO | Cross-catalog joins |
| 7.4 | Set up DuckDB with Iceberg extension | ⬜ TODO | Embedded in API layer or standalone |

### Phase 8 — FastAPI Backend
| # | Task | Status | Notes |
|---|------|--------|-------|
| 8.1 | Create FastAPI project (`api/`) | ⬜ TODO | Python project with uvicorn |
| 8.2 | Implement DuckDB query endpoint | ⬜ TODO | `POST /query/duckdb` — SQL passthrough |
| 8.3 | Implement Trino query endpoint | ⬜ TODO | `POST /query/trino` — SQL passthrough |
| 8.4 | Add metadata endpoints (tables, schemas, catalogs) | ⬜ TODO | `GET /catalogs`, `GET /tables/{catalog}` |
| 8.5 | Dockerize the API | ⬜ TODO | Dockerfile + compose service |

### Phase 9 — Web UI
| # | Task | Status | Notes |
|---|------|--------|-------|
| 9.1 | Create basic web UI project (`ui/`) | ⬜ TODO | HTML/JS or React SPA |
| 9.2 | Build SQL editor with engine selector (DuckDB / Trino) | ⬜ TODO | Textarea + execute button |
| 9.3 | Build results table with pagination | ⬜ TODO | Tabular display of query results |
| 9.4 | Add schema browser sidebar | ⬜ TODO | Tree view of catalogs → schemas → tables |
| 9.5 | Dockerize the UI (nginx or node) | ⬜ TODO | Dockerfile + compose service |

### Phase 10 — Integration & Polish
| # | Task | Status | Notes |
|---|------|--------|-------|
| 10.1 | Full `docker compose up` smoke test | ⬜ TODO | All services start and connect |
| 10.2 | Add health checks to all compose services | ⬜ TODO | Depends-on with health conditions |
| 10.3 | Write README with setup instructions | ⬜ TODO | Prerequisites, quick start, architecture |
| 10.4 | Add sample data / demo scenario | ⬜ TODO | Pre-built topic + queries for demo |

---

## Dependencies

```
Phase 1 ──→ Phase 2 (needs Kafka)
Phase 1 ──→ Phase 3 (needs networking)
Phase 3 ──→ Phase 4 (catalog needs object store)
Phase 1 + 4 → Phase 5 (Flink needs Kafka + Iceberg)
Phase 1 ────→ Phase 6 (ClickHouse needs networking; Flink job needs Phase 5)
Phase 4 + 6 → Phase 7 (query engines need data stores)
Phase 7 ────→ Phase 8 (API wraps query engines)
Phase 8 ────→ Phase 9 (UI calls API)
All ────────→ Phase 10 (integration)
```

---

## Tech Stack Summary

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (KRaft) | Event streaming |
| Producer | Python (`confluent-kafka`) | Generate messages |
| Stream Processor | Apache Flink | Consume Kafka, write to sinks |
| Table Format | Apache Iceberg | Open table format on object store |
| Object Store | RustFS / MinIO | S3-compatible local storage |
| Analytics DB | ClickHouse | Fast OLAP queries |
| Query Engine | Trino | Federated SQL across Iceberg + ClickHouse |
| Query Engine | DuckDB | Embedded analytical SQL on Iceberg |
| API | Python FastAPI | REST API for query access |
| UI | Web (HTML/JS or React) | SQL editor and results viewer |
| Orchestration | Docker Compose | Local environment management |

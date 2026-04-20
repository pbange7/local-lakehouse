"""
PyIceberg Writer Service
Receives JSON batches from Bento via HTTP POST and appends them to
an Iceberg table stored in MinIO, using a local SQLite catalog.

Catalog: SQLite (local, ephemeral) — stores Iceberg metadata pointers.
Storage: MinIO S3 at s3://lakehouse/warehouse-bento/
         (separate prefix from Flink's s3://lakehouse/warehouse/)

Note: To query via Trino/DuckDB, use the direct S3 path with iceberg_scan()
or configure a REST/Hive catalog pointing at this warehouse.
"""
import logging
import os
from contextlib import asynccontextmanager

import pyarrow as pa
from fastapi import FastAPI, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────
MINIO_EP     = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_KEY    = os.getenv("MINIO_ACCESS_KEY",  "lakehouse")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY",  "lakehouse123")
WAREHOUSE    = os.getenv("WAREHOUSE",         "s3://lakehouse/warehouse")
# Use a sub-prefix so Bento data doesn't collide with Flink snapshots
BENTO_WAREHOUSE = WAREHOUSE.rstrip("/") + "-bento"
CATALOG_DB   = "/tmp/pyiceberg.db"

# ── Iceberg schema — matches Flink SQL table and ClickHouse MergeTree ─────────
WEB_LOGS_SCHEMA = Schema(
    NestedField(1, "request_id",  StringType(),  required=False),
    NestedField(2, "method",      StringType(),  required=False),
    NestedField(3, "path",        StringType(),  required=False),
    NestedField(4, "status_code", IntegerType(), required=False),
    NestedField(5, "latency_ms",  IntegerType(), required=False),
    NestedField(6, "ip",          StringType(),  required=False),
    NestedField(7, "user_agent",  StringType(),  required=False),
    NestedField(8, "ts",          StringType(),  required=False),
)

WEB_LOGS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=4,        # status_code field id
        field_id=1000,
        transform=IdentityTransform(),
        name="status_code",
    )
)

# ── Arrow schema for pa.Table.from_pylist ─────────────────────────────────────
ARROW_SCHEMA = pa.schema([
    pa.field("request_id",  pa.string()),
    pa.field("method",      pa.string()),
    pa.field("path",        pa.string()),
    pa.field("status_code", pa.int32()),
    pa.field("latency_ms",  pa.int32()),
    pa.field("ip",          pa.string()),
    pa.field("user_agent",  pa.string()),
    pa.field("ts",          pa.string()),
])

# ── Global state ─────────────────────────────────────────────────────────────
_catalog = None
_table   = None


def _get_catalog():
    """
    SQL catalog backed by SQLite (stored in /tmp).
    Data files go to MinIO at the BENTO_WAREHOUSE path.
    """
    return load_catalog(
        "bento",
        **{
            "type":              "sql",
            "uri":               f"sqlite:///{CATALOG_DB}",
            "warehouse":         BENTO_WAREHOUSE,
            "s3.endpoint":       MINIO_EP,
            "s3.access-key-id":  MINIO_KEY,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.region":         "us-east-1",
        },
    )


def _ensure_table(catalog):
    """Create namespace + table if missing; return the table handle."""
    try:
        catalog.create_namespace("web_logs")
        logger.info("Created Iceberg namespace: web_logs")
    except NamespaceAlreadyExistsError:
        pass

    try:
        table = catalog.load_table("web_logs.web_logs")
        logger.info("Loaded existing Iceberg table: web_logs.web_logs (warehouse: %s)", BENTO_WAREHOUSE)
    except NoSuchTableError:
        table = catalog.create_table(
            "web_logs.web_logs",
            schema=WEB_LOGS_SCHEMA,
            partition_spec=WEB_LOGS_PARTITION,
            properties={
                "write.format.default":           "parquet",
                "write.parquet.compression-codec": "snappy",
            },
        )
        logger.info("Created Iceberg table: web_logs.web_logs (warehouse: %s)", BENTO_WAREHOUSE)
    return table


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _catalog, _table
    logger.info("Initialising PyIceberg catalog (SQLite + MinIO S3)...")
    _catalog = _get_catalog()
    _table   = _ensure_table(_catalog)
    logger.info("PyIceberg writer ready — warehouse: %s", BENTO_WAREHOUSE)
    yield
    logger.info("PyIceberg writer shutting down.")


app = FastAPI(title="PyIceberg Writer", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status":    "ok",
        "table":     "web_logs.web_logs",
        "warehouse": BENTO_WAREHOUSE,
        "catalog":   "sql/sqlite",
    }


@app.post("/append")
def append(records: list[dict]):
    """
    Receive a batch of JSON records from Bento and append them
    to the Iceberg table as a new snapshot.
    """
    if not records:
        return {"status": "ok", "count": 0}

    try:
        arrow_table = pa.Table.from_pylist(records, schema=ARROW_SCHEMA)
        _table.append(arrow_table)
        logger.info("Appended %d records → Iceberg snapshot committed", len(records))
        return {"status": "ok", "count": len(records)}
    except Exception as exc:
        logger.exception("Failed to append %d records: %s", len(records), exc)
        raise HTTPException(status_code=500, detail=str(exc))

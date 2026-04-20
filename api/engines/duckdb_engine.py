"""
DuckDB engine: reads Iceberg tables directly from MinIO S3.
On startup, discovers all Iceberg table directories and registers them
as DuckDB views (namespace.table) so callers write plain SQL.
Thread-safe via a Lock — DuckDB is synchronous.
"""
import logging
import threading
import boto3
import duckdb
import config

logger = logging.getLogger(__name__)

_con: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()
_registered_views: list[dict] = []


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _discover_iceberg_tables() -> list[dict]:
    """Return list of {namespace, table, s3_path} for every Iceberg table in the warehouse."""
    s3 = _s3_client()
    tables = []
    prefix = config.MINIO_WAREHOUSE_PREFIX.rstrip("/") + "/"

    # List top-level namespace directories (e.g. warehouse/web_logs/)
    ns_resp = s3.list_objects_v2(Bucket=config.MINIO_BUCKET, Prefix=prefix, Delimiter="/")
    for ns_entry in ns_resp.get("CommonPrefixes", []):
        ns_prefix = ns_entry["Prefix"]                          # e.g. warehouse/web_logs/
        ns_name = ns_prefix[len(prefix):].strip("/")            # e.g. web_logs

        # List table directories inside namespace (e.g. warehouse/web_logs/web_logs_{uuid}/)
        tbl_resp = s3.list_objects_v2(Bucket=config.MINIO_BUCKET, Prefix=ns_prefix, Delimiter="/")
        for tbl_entry in tbl_resp.get("CommonPrefixes", []):
            tbl_prefix = tbl_entry["Prefix"]                    # e.g. warehouse/web_logs/web_logs_{uuid}/
            tbl_dir = tbl_prefix[len(ns_prefix):].strip("/")   # e.g. web_logs_670a34b8-...

            # Derive the logical table name: everything before the first underscore-UUID separator
            # Convention: Nessie creates directories as {table_name}_{uuid}
            # Strip the uuid suffix (last 36 chars incl. the connecting underscore)
            if "_" in tbl_dir and len(tbl_dir) > 36:
                tbl_name = tbl_dir[: -(36 + 1)]  # strip _<uuid> (37 chars)
            else:
                tbl_name = tbl_dir

            s3_path = f"s3://{config.MINIO_BUCKET}/{tbl_prefix.rstrip('/')}"
            tables.append({"namespace": ns_name, "table": tbl_name, "s3_path": s3_path})
            logger.info("Discovered Iceberg table: %s.%s → %s", ns_name, tbl_name, s3_path)

    return tables


async def startup():
    global _con, _registered_views
    logger.info("Initialising DuckDB engine...")
    _con = duckdb.connect(":memory:")

    _con.execute("INSTALL httpfs; LOAD httpfs;")
    _con.execute("INSTALL iceberg; LOAD iceberg;")

    _con.execute(f"SET s3_endpoint='{config.MINIO_ENDPOINT}';")
    _con.execute(f"SET s3_access_key_id='{config.MINIO_ACCESS_KEY}';")
    _con.execute(f"SET s3_secret_access_key='{config.MINIO_SECRET_KEY}';")
    _con.execute("SET s3_use_ssl=false;")
    _con.execute("SET s3_url_style='path';")
    _con.execute("SET s3_region='us-east-1';")
    _con.execute("SET unsafe_enable_version_guessing=true;")

    tables = _discover_iceberg_tables()
    for tbl in tables:
        ns, name, path = tbl["namespace"], tbl["table"], tbl["s3_path"]
        _con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
        _con.execute(
            f'CREATE OR REPLACE VIEW "{ns}"."{name}" AS '
            f"SELECT * FROM iceberg_scan('{path}')"
        )
    _registered_views = tables
    logger.info("DuckDB engine ready. Registered %d view(s).", len(tables))


def shutdown():
    global _con
    if _con:
        _con.close()
        _con = None
    logger.info("DuckDB engine closed.")


def execute(sql: str) -> dict:
    """Execute SQL and return {columns, rows, row_count}. Thread-safe."""
    if _con is None:
        raise RuntimeError("DuckDB engine not initialised")
    with _lock:
        rel = _con.execute(sql)
        columns = [d[0] for d in rel.description] if rel.description else []
        rows = [list(r) for r in rel.fetchall()]
    return {"columns": columns, "rows": rows, "row_count": len(rows)}


def registered_views() -> list[dict]:
    return list(_registered_views)

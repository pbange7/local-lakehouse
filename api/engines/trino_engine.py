"""
Trino engine: federated SQL via the trino-python-client (DBAPI 2.0).
Creates a new connection per query — lightweight and connection-pool-free.
"""
import logging
import trino
import config

logger = logging.getLogger(__name__)


def _connect() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user=config.TRINO_USER,
        http_scheme="http",
    )


def execute(sql: str) -> dict:
    """Execute SQL on Trino and return {columns, rows, row_count}."""
    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = [list(r) for r in cursor.fetchall()]
        return {"columns": columns, "rows": rows, "row_count": len(rows)}
    finally:
        conn.close()


def list_tables() -> list[dict]:
    """Return all user tables from the iceberg and clickhouse Trino catalogs."""
    tables = []
    for catalog in ("iceberg", "clickhouse"):
        try:
            result = execute(
                f"SELECT table_catalog, table_schema, table_name "
                f"FROM {catalog}.information_schema.tables "
                f"WHERE table_schema NOT IN ('information_schema') "
                f"ORDER BY table_schema, table_name"
            )
            for row in result["rows"]:
                tables.append({
                    "catalog": row[0],
                    "schema": row[1],
                    "table": row[2],
                    "fqn": f"{row[0]}.{row[1]}.{row[2]}",
                })
        except Exception as exc:
            logger.warning("Could not list tables from Trino catalog %s: %s", catalog, exc)
    return tables

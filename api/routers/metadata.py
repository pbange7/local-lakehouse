import asyncio
import logging

from fastapi import APIRouter, HTTPException
from engines import duckdb_engine, trino_engine

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok", "engines": ["duckdb", "trino"]}


@router.get("/catalogs")
async def catalogs():
    return [
        {
            "name": "duckdb",
            "type": "duckdb",
            "description": "DuckDB — reads Iceberg tables directly from MinIO S3",
        },
        {
            "name": "trino",
            "type": "trino",
            "description": "Trino — federated SQL across iceberg and clickhouse catalogs",
        },
    ]


@router.get("/tables/{engine}")
async def tables(engine: str):
    if engine == "duckdb":
        views = duckdb_engine.registered_views()
        return [
            {
                "catalog": "duckdb",
                "schema": v["namespace"],
                "table": v["table"],
                "fqn": f'{v["namespace"]}.{v["table"]}',
            }
            for v in views
        ]
    elif engine == "trino":
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, trino_engine.list_tables
            )
            return result
        except Exception as exc:
            logger.exception("Failed to list Trino tables")
            raise HTTPException(status_code=503, detail=str(exc))
    else:
        raise HTTPException(status_code=404, detail=f"Unknown engine: {engine!r}. Use 'duckdb' or 'trino'.")

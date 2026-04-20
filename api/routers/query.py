import asyncio
import re
import time
import logging

import duckdb
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from engines import duckdb_engine, trino_engine

logger = logging.getLogger(__name__)
router = APIRouter()

_LIMIT_RE = re.compile(r"\bLIMIT\b", re.IGNORECASE)


class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int
    elapsed_ms: float
    engine: str


def _apply_limit(sql: str, limit: int) -> str:
    """Append LIMIT clause only if the query doesn't already have one."""
    if _LIMIT_RE.search(sql):
        return sql
    return f"{sql.rstrip('; ')} LIMIT {limit}"


@router.post("/duckdb", response_model=QueryResponse)
async def query_duckdb(req: QueryRequest):
    sql = _apply_limit(req.sql, req.limit)
    t0 = time.monotonic()
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None, duckdb_engine.execute, sql
        )
    except duckdb.Error as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Unexpected DuckDB error")
        raise HTTPException(status_code=500, detail=str(exc))
    elapsed = (time.monotonic() - t0) * 1000
    return QueryResponse(engine="duckdb", elapsed_ms=round(elapsed, 1), **result)


@router.post("/trino", response_model=QueryResponse)
async def query_trino(req: QueryRequest):
    sql = _apply_limit(req.sql, req.limit)
    t0 = time.monotonic()
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None, trino_engine.execute, sql
        )
    except Exception as exc:
        # trino exceptions inherit from Exception; surface as 400 for SQL errors
        msg = str(exc)
        status = 400 if "syntax" in msg.lower() or "does not exist" in msg.lower() else 500
        raise HTTPException(status_code=status, detail=msg)
    elapsed = (time.monotonic() - t0) * 1000
    return QueryResponse(engine="trino", elapsed_ms=round(elapsed, 1), **result)

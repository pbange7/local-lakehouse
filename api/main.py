import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from engines import duckdb_engine
from routers import query, metadata

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await duckdb_engine.startup()
    yield
    duckdb_engine.shutdown()


app = FastAPI(
    title="Lakehouse API",
    description="Query Iceberg (DuckDB) and federated SQL (Trino) via a unified REST API.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(metadata.router, tags=["Metadata"])
app.include_router(query.router, prefix="/query", tags=["Query"])

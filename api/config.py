import os

TRINO_HOST             = os.getenv("TRINO_HOST", "trino")
TRINO_PORT             = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER             = os.getenv("TRINO_USER", "admin")

MINIO_ENDPOINT         = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY       = os.getenv("MINIO_ACCESS_KEY", "lakehouse")
MINIO_SECRET_KEY       = os.getenv("MINIO_SECRET_KEY", "lakehouse123")
MINIO_BUCKET           = os.getenv("MINIO_BUCKET", "lakehouse")
MINIO_WAREHOUSE_PREFIX = os.getenv("MINIO_WAREHOUSE_PREFIX", "warehouse")

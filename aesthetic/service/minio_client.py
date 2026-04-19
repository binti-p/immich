"""
MinIO dual-write buffer for aesthetic-service.
Buffers interaction events and inference log entries, flushes to parquet on threshold.
"""
import asyncio
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prometheus_client import Counter

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")
INTERACTION_FLUSH_SIZE = int(os.environ.get("INTERACTION_FLUSH_SIZE", "100"))
INFERENCE_FLUSH_SIZE = int(os.environ.get("INFERENCE_FLUSH_SIZE", "50"))

# Prometheus
MINIO_FLUSH_TOTAL = Counter(
    "minio_flush_total", "MinIO parquet flushes", ["target"]
)
MINIO_FLUSH_ERRORS = Counter(
    "minio_flush_errors_total", "MinIO flush failures"
)

# In-memory buffers + locks
_interaction_buffer: list[dict[str, Any]] = []
_inference_buffer: list[dict[str, Any]] = []
_interaction_lock = asyncio.Lock()
_inference_lock = asyncio.Lock()
_part_counters: dict[str, int] = {}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _next_part(prefix: str) -> str:
    n = _part_counters.get(prefix, 0)
    _part_counters[prefix] = n + 1
    return f"part-{n:04d}.parquet"


def _flush_to_minio(records: list[dict], prefix: str, target_label: str):
    """Convert records to parquet and upload. Runs synchronously (called from async via executor)."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    part = _next_part(prefix)
    key = f"production-sim/{prefix}/date={date_str}/{part}"

    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3 = _s3_client()
    s3.upload_fileobj(buf, BUCKET, key)
    logger.info(f"[minio] Flushed {len(records)} records to s3://{BUCKET}/{key}")
    MINIO_FLUSH_TOTAL.labels(target=target_label).inc()


async def buffer_interaction(record: dict):
    async with _interaction_lock:
        _interaction_buffer.append(record)
        if len(_interaction_buffer) >= INTERACTION_FLUSH_SIZE:
            batch = _interaction_buffer.copy()
            _interaction_buffer.clear()
            await _flush_async(batch, "interactions", "interactions")


async def buffer_inference(record: dict):
    async with _inference_lock:
        _inference_buffer.append(record)
        if len(_inference_buffer) >= INFERENCE_FLUSH_SIZE:
            batch = _inference_buffer.copy()
            _inference_buffer.clear()
            await _flush_async(batch, "inference-log", "inference_log")


async def _flush_async(records: list[dict], prefix: str, target_label: str):
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _flush_to_minio, records, prefix, target_label)
    except Exception as e:
        logger.error(f"[minio] Flush to {prefix} failed: {e}")
        MINIO_FLUSH_ERRORS.inc()
        # Put records back — best effort
        if prefix == "interactions":
            async with _interaction_lock:
                _interaction_buffer.extend(records)
        else:
            async with _inference_lock:
                _inference_buffer.extend(records)


async def flush_all():
    """Force-flush remaining buffers — call on shutdown."""
    async with _interaction_lock:
        if _interaction_buffer:
            batch = _interaction_buffer.copy()
            _interaction_buffer.clear()
            await _flush_async(batch, "interactions", "interactions")

    async with _inference_lock:
        if _inference_buffer:
            batch = _inference_buffer.copy()
            _inference_buffer.clear()
            await _flush_async(batch, "inference-log", "inference_log")

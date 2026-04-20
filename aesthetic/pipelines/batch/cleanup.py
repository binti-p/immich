"""
Cleanup script — run manually when needed. Does two things:

1. DB cleanup: deletes rows older than 30 days from interaction_events and inference_log
2. MinIO pruning: keeps last 3 versions of model/dataset files, NEVER deletes *_card.json

For post-retrain model promotion and rescore, use promote.py instead.

Usage:
    docker exec aesthetic_service python -m pipelines.batch.cleanup
    docker exec aesthetic_service python -m pipelines.batch.cleanup --skip-db
    docker exec aesthetic_service python -m pipelines.batch.cleanup --skip-minio
"""
import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST = os.environ.get("POSTGRES_HOST", "immich_postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB   = os.environ.get("POSTGRES_DB", "immich")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")

MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://immich_minio:9000")
MINIO_KEY      = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET         = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")

RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))
KEEP_VERSIONS  = int(os.environ.get("KEEP_VERSIONS", "3"))

# Never pruned regardless of version count
PROTECTED_SUFFIXES = ("model_card.json", "dataset_card.json")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


# ── 1. DB cleanup ─────────────────────────────────────────────────────────────
def cleanup_db():
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    log.info(f"[db] Deleting records older than {cutoff.date()} (retention={RETENTION_DAYS}d)")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM interaction_events WHERE "ingestedAt" < %s', (cutoff,)
            )
            ie_deleted = cur.rowcount
            cur.execute(
                'DELETE FROM inference_log WHERE "requestReceivedAt" < %s', (cutoff,)
            )
            il_deleted = cur.rowcount
        conn.commit()
        log.info(f"[db] Deleted {ie_deleted} interaction_events, {il_deleted} inference_log rows")
    except Exception as e:
        conn.rollback()
        log.error(f"[db] Cleanup failed: {e}")
        raise
    finally:
        conn.close()


# ── 2. MinIO version pruning ──────────────────────────────────────────────────
def _list_versioned_prefixes(client, base_prefix: str) -> list[str]:
    resp = client.list_objects_v2(Bucket=BUCKET, Prefix=base_prefix, Delimiter="/")
    prefixes = [
        cp["Prefix"].rstrip("/").split("/")[-1]
        for cp in resp.get("CommonPrefixes", [])
        if cp["Prefix"].rstrip("/").split("/")[-1].startswith("v")
    ]
    return sorted(prefixes)  # ascending — oldest first


def _list_keys_under(client, prefix: str) -> list[str]:
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def prune_versions(client, base_prefix: str, label: str):
    versions = _list_versioned_prefixes(client, base_prefix)
    if len(versions) <= KEEP_VERSIONS:
        log.info(f"[minio] {label}: {len(versions)} versions, nothing to prune")
        return

    to_prune = versions[: len(versions) - KEEP_VERSIONS]
    log.info(f"[minio] {label}: pruning {len(to_prune)} old versions, keeping {KEEP_VERSIONS}")

    for version in to_prune:
        prefix = f"{base_prefix}{version}/"
        for key in _list_keys_under(client, prefix):
            if any(key.endswith(s) for s in PROTECTED_SUFFIXES):
                log.info(f"[minio] Keeping protected: {key}")
                continue
            client.delete_object(Bucket=BUCKET, Key=key)
            log.info(f"[minio] Deleted: {key}")


def cleanup_minio():
    client = _s3()
    prune_versions(client, "models/",   "models")
    prune_versions(client, "datasets/", "datasets")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Aesthetic Hub cleanup")
    parser.add_argument("--skip-db",    action="store_true", help="Skip DB row cleanup")
    parser.add_argument("--skip-minio", action="store_true", help="Skip MinIO version pruning")
    args = parser.parse_args()

    if not args.skip_db:
        cleanup_db()
    if not args.skip_minio:
        cleanup_minio()

    log.info("Cleanup complete.")


if __name__ == "__main__":
    main()

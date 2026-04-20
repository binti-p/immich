"""
Batch pipeline: reads interaction events + CLIP embeddings from the last 7 days,
burst-groups, splits 80/20 by user, writes parquet + dataset_card.json to MinIO.
"""
import io
import json
import logging
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST     = os.environ.get("POSTGRES_HOST", "immich_postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")
PG_DB       = os.environ.get("POSTGRES_DB", "immich")
PG_USER     = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS     = os.environ.get("POSTGRES_PASSWORD", "postgres")

MINIO_ENDPOINT  = os.environ.get("AWS_ENDPOINT_URL", "http://immich_minio:9000")
MINIO_KEY       = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET    = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET          = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")

EVENT_WINDOW_DAYS   = int(os.environ.get("EVENT_WINDOW_DAYS", "7"))
BURST_WINDOW_SECS   = int(os.environ.get("BURST_WINDOW_SECS", "60"))
MIN_USERS           = int(os.environ.get("MIN_USERS", "5"))
MIN_EVENTS          = int(os.environ.get("MIN_EVENTS", "50"))
TRIGGER_TRAINING    = os.environ.get("TRIGGER_TRAINING", "false").lower() == "true"
ARGO_WEBHOOK        = os.environ.get("ARGO_WEBHOOK_URL", "http://129.114.27.253:31234/aesthetic-hub/train")

CLIP_DIM = 768


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


# ── Step 1: Read interaction events ──────────────────────────────────────────
def read_events(conn) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=EVENT_WINDOW_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "assetId"::text  AS asset_id,
                   "userId"::text   AS user_id,
                   "eventType"      AS event_type,
                   label,
                   "eventTime"      AS event_time
            FROM   interaction_events
            WHERE  "eventTime" >= %s
              AND  "deletedAt" IS NULL
            ORDER  BY "userId", "assetId", "eventTime"
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
    log.info(f"Read {len(rows)} raw events (last {EVENT_WINDOW_DAYS} days)")
    return [dict(r) for r in rows]


# ── Step 2: Read CLIP embeddings ──────────────────────────────────────────────
def read_clip(conn, asset_ids: list[str]) -> dict[str, list[float]]:
    if not asset_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            'SELECT "assetId"::text AS asset_id, embedding FROM smart_search WHERE "assetId" = ANY(%s::uuid[])',
            (asset_ids,),
        )
        rows = cur.fetchall()
    result = {}
    for r in rows:
        emb = r["embedding"]
        # asyncpg/psycopg2 may return string or list depending on driver
        if isinstance(emb, str):
            emb = [float(x) for x in emb.strip("[]()").split(",")]
        else:
            emb = list(emb)
        result[r["asset_id"]] = emb
    log.info(f"Read CLIP embeddings for {len(result)}/{len(asset_ids)} assets")
    return result


# ── Step 3: Burst grouping ────────────────────────────────────────────────────
def burst_group(events: list[dict]) -> list[dict]:
    """Per (user_id, asset_id), keep only the last event within each 60s burst."""
    grouped: dict[tuple, list] = defaultdict(list)
    for e in events:
        grouped[(e["user_id"], e["asset_id"])].append(e)

    result = []
    for (uid, aid), evts in grouped.items():
        evts.sort(key=lambda x: x["event_time"])
        kept = []
        i = 0
        while i < len(evts):
            burst_start = evts[i]["event_time"]
            j = i
            while j < len(evts) and (evts[j]["event_time"] - burst_start).total_seconds() <= BURST_WINDOW_SECS:
                j += 1
            # keep the last event in the burst
            kept.append(evts[j - 1])
            i = j
        result.extend(kept)

    log.info(f"After burst grouping: {len(result)} events (from {len(events)} raw)")
    return result


# ── Step 4: Join with embeddings ──────────────────────────────────────────────
def join_embeddings(events: list[dict], clip: dict[str, list[float]]) -> list[dict]:
    joined = [e for e in events if e["asset_id"] in clip]
    for e in joined:
        e["clip_embedding"] = clip[e["asset_id"]]
    dropped = len(events) - len(joined)
    if dropped:
        log.warning(f"Dropped {dropped} events with no CLIP embedding")
    return joined


# ── Step 5: Minimum threshold ─────────────────────────────────────────────────
def check_threshold(events: list[dict]) -> bool:
    users = {e["user_id"] for e in events}
    if len(users) < MIN_USERS or len(events) < MIN_EVENTS:
        log.warning(
            f"Below threshold: {len(users)} users (min {MIN_USERS}), "
            f"{len(events)} events (min {MIN_EVENTS}). Exiting."
        )
        return False
    return True


# ── Step 6: Train/val split 80/20 by user ────────────────────────────────────
def split_by_user(events: list[dict]) -> tuple[list[dict], list[dict]]:
    users = list({e["user_id"] for e in events})
    random.shuffle(users)
    split_idx = max(1, int(len(users) * 0.8))
    train_users = set(users[:split_idx])
    train = [e for e in events if e["user_id"] in train_users]
    val   = [e for e in events if e["user_id"] not in train_users]
    log.info(f"Split: {len(train)} train rows ({split_idx} users), {len(val)} val rows ({len(users)-split_idx} users)")
    return train, val


# ── Step 7: Write to MinIO ────────────────────────────────────────────────────
PARQUET_SCHEMA = pa.schema([
    pa.field("user_id",        pa.string()),
    pa.field("asset_id",       pa.string()),
    pa.field("clip_embedding", pa.list_(pa.float32(), CLIP_DIM)),
    pa.field("label",          pa.float32()),
    pa.field("event_type",     pa.string()),
])


def to_table(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "user_id":        [r["user_id"] for r in rows],
            "asset_id":       [r["asset_id"] for r in rows],
            "clip_embedding": [[float(x) for x in r["clip_embedding"]] for r in rows],
            "label":          [float(r["label"]) for r in rows],
            "event_type":     [r["event_type"] for r in rows],
        },
        schema=PARQUET_SCHEMA,
    )


def upload_parquet(client, table: pa.Table, key: str):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    client.upload_fileobj(buf, BUCKET, key)
    log.info(f"Uploaded s3://{BUCKET}/{key} ({buf.tell()} bytes)")


def upload_json(client, data: dict, key: str):
    body = json.dumps(data, indent=2).encode()
    client.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    log.info(f"Uploaded s3://{BUCKET}/{key}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"datasets/v{today}/personalized-flickr"

    conn = get_conn()
    try:
        events_raw = read_events(conn)
        if not events_raw:
            log.warning("No events found. Exiting.")
            return

        asset_ids = list({e["asset_id"] for e in events_raw})
        clip = read_clip(conn, asset_ids)
    finally:
        conn.close()

    events = burst_group(events_raw)
    events = join_embeddings(events, clip)

    if not check_threshold(events):
        return

    train, val = split_by_user(events)

    # Label distribution
    label_dist: dict[str, int] = defaultdict(int)
    for e in events:
        label_dist[e["event_type"]] += 1

    dataset_card = {
        "version": today,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "event_window_days": EVENT_WINDOW_DAYS,
        "stats": {
            "total_events_raw": len(events_raw),
            "total_events_after_burst_grouping": len(events),
            "unique_users": len({e["user_id"] for e in events}),
            "unique_assets": len({e["asset_id"] for e in events}),
        },
        "splits": {
            "train": {"users": len({e["user_id"] for e in train}), "rows": len(train)},
            "val":   {"users": len({e["user_id"] for e in val}),   "rows": len(val)},
        },
        "event_label_distribution": dict(label_dist),
        "schema": {
            "columns": ["user_id", "asset_id", "clip_embedding", "label", "event_type"],
            "clip_dim": CLIP_DIM,
        },
    }

    client = s3()
    upload_parquet(client, to_table(train), f"{prefix}/train.parquet")
    upload_parquet(client, to_table(val),   f"{prefix}/val.parquet")
    upload_json(client, dataset_card,       f"{prefix}/dataset_card.json")

    log.info(f"Pipeline complete. Dataset version: {today}")

    # Optional: trigger Argo training webhook
    if TRIGGER_TRAINING:
        try:
            resp = requests.post(ARGO_WEBHOOK, timeout=10)
            log.info(f"Argo webhook: {resp.status_code}")
        except Exception as e:
            log.warning(f"Argo webhook failed (non-fatal): {e}")


if __name__ == "__main__":
    main()

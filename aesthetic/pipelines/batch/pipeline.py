"""
Batch pipeline — runs weekly (Sunday 2am UTC).

Checkpoints:
  E1: Ingestion QA   — schema validation, volume gate, signal distribution, CLIP coverage
  E2: Training set QA — held-out test users, split sanity, label parity, embedding norm drift,
                        score-interaction correlation, interaction rate trend
"""
import io
import json
import logging
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from scipy.stats import spearmanr

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

EVENT_WINDOW_DAYS  = int(os.environ.get("EVENT_WINDOW_DAYS", "7"))
BURST_WINDOW_SECS  = int(os.environ.get("BURST_WINDOW_SECS", "60"))
MIN_EVENTS         = int(os.environ.get("MIN_EVENTS", "50"))
MIN_USERS          = int(os.environ.get("MIN_USERS", "5"))
TRIGGER_TRAINING   = os.environ.get("TRIGGER_TRAINING", "false").lower() == "true"
ARGO_WEBHOOK       = os.environ.get("ARGO_WEBHOOK_URL", "http://129.114.27.253:31234/aesthetic-hub/train")

CLIP_DIM = 768
VALID_EVENT_TYPES = {"favorite", "unfavorite", "archive", "delete", "album_add", "download", "share", "view_expanded"}

TEST_PARQUET_KEY    = "datasets/personalized-flickr/test.parquet"
BASELINE_STATS_KEY  = "datasets/baseline_stats.json"


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


# ── DB reads ──────────────────────────────────────────────────────────────────

def read_raw_events(conn) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=EVENT_WINDOW_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "eventId"   AS event_id,
                   "assetId"::text  AS asset_id,
                   "userId"::text   AS user_id,
                   "eventType"      AS event_type,
                   label,
                   "eventTime"      AS event_time
            FROM   interaction_events
            WHERE  "eventTime" >= %s AND "deletedAt" IS NULL
            ORDER  BY "userId", "assetId", "eventTime"
            """,
            (cutoff,),
        )
        return [dict(r) for r in cur.fetchall()]


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
        if isinstance(emb, str):
            emb = [float(x) for x in emb.strip("[]()").split(",")]
        else:
            emb = list(emb)
        result[r["asset_id"]] = emb
    return result


def read_aesthetic_scores(conn, asset_ids: list[str]) -> dict[str, float]:
    """Read current aesthetic scores for correlation check (E2.5)."""
    if not asset_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            'SELECT "assetId"::text AS asset_id, score FROM aesthetic_scores WHERE "assetId" = ANY(%s::uuid[])',
            (asset_ids,),
        )
        return {r["asset_id"]: float(r["score"]) for r in cur.fetchall()}


def read_assets_scored_count(conn) -> int:
    """Count distinct assets with a score (for interaction rate denominator)."""
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(DISTINCT "assetId") FROM aesthetic_scores')
        return cur.fetchone()["count"]


# ── E1: Ingestion QA ──────────────────────────────────────────────────────────

def e1_schema_validate(events: list[dict]) -> tuple[list[dict], dict]:
    """E1.1 — Drop rows with nulls, invalid label, unknown event type. Return clean rows + stats."""
    null_dropped = 0
    unknown_type_dropped = 0
    clean = []

    for e in events:
        # Null check
        if any(e.get(f) is None for f in ("event_id", "asset_id", "user_id", "event_type", "label", "event_time")):
            null_dropped += 1
            continue
        # Label range
        if not (-1.0 <= float(e["label"]) <= 1.0):
            null_dropped += 1
            continue
        # Event type
        if e["event_type"] not in VALID_EVENT_TYPES:
            unknown_type_dropped += 1
            continue
        clean.append(e)

    total = len(events)
    dropped = null_dropped + unknown_type_dropped
    drop_pct = dropped / total if total > 0 else 0.0
    schema_warning = drop_pct > 0.05

    log.info(f"[E1.1] Raw={total}, null_dropped={null_dropped}, unknown_type_dropped={unknown_type_dropped}, "
             f"drop_pct={drop_pct:.1%}{' ⚠ >5%' if schema_warning else ''}")

    return clean, {
        "raw_event_count": total,
        "null_dropped": null_dropped,
        "unknown_type_dropped": unknown_type_dropped,
        "schema_drop_warning": schema_warning,
    }


def e1_signal_distribution(events: list[dict]) -> dict:
    """E1.3 — Check event type distribution."""
    counts: dict[str, int] = defaultdict(int)
    for e in events:
        counts[e["event_type"]] += 1

    total = len(events)
    dominant_warning = any(v / total > 0.70 for v in counts.values()) if total > 0 else False
    inversion_warning = counts.get("favorite", 0) < counts.get("delete", 0)

    if dominant_warning:
        log.warning(f"[E1.3] Dominant event type >70%: {dict(counts)}")
    if inversion_warning:
        log.warning(f"[E1.3] Signal inversion: favorite({counts.get('favorite',0)}) < delete({counts.get('delete',0)})")

    return {
        "dominant_event_type_warning": dominant_warning,
        "signal_inversion_warning": inversion_warning,
        "event_counts": dict(counts),
    }


def e1_clip_coverage(events: list[dict], clip: dict[str, list[float]]) -> tuple[list[dict], dict]:
    """E1.4 — Drop events with no CLIP embedding, flag if >10% dropped."""
    joined = [e for e in events if e["asset_id"] in clip]
    no_clip_dropped = len(events) - len(joined)
    total = len(events)
    clip_coverage_pct = len(joined) / total if total > 0 else 0.0
    clip_warning = no_clip_dropped / total > 0.10 if total > 0 else False

    log.info(f"[E1.4] CLIP coverage={clip_coverage_pct:.1%}, no_clip_dropped={no_clip_dropped}"
             f"{' ⚠ >10%' if clip_warning else ''}")

    return joined, {
        "no_clip_embedding_dropped": no_clip_dropped,
        "clip_coverage_pct": round(clip_coverage_pct, 4),
        "clip_coverage_warning": clip_warning,
    }


# ── Burst grouping ────────────────────────────────────────────────────────────

def burst_group(events: list[dict]) -> list[dict]:
    grouped: dict[tuple, list] = defaultdict(list)
    for e in events:
        grouped[(e["user_id"], e["asset_id"])].append(e)

    result = []
    for evts in grouped.values():
        evts.sort(key=lambda x: x["event_time"])
        i = 0
        while i < len(evts):
            burst_start = evts[i]["event_time"]
            j = i
            while j < len(evts) and (evts[j]["event_time"] - burst_start).total_seconds() <= BURST_WINDOW_SECS:
                j += 1
            result.append(evts[j - 1])
            i = j

    log.info(f"[burst] {len(result)} events after burst grouping")
    return result


# ── E2: Training Set QA ───────────────────────────────────────────────────────

def e2_get_or_create_test_users(all_user_ids: list[str], client) -> set[str]:
    """E2.1 — Load test.parquet to get test user IDs, or create on first run."""
    try:
        obj = client.get_object(Bucket=BUCKET, Key=TEST_PARQUET_KEY)
        table = pq.read_table(io.BytesIO(obj["Body"].read()))
        test_users = set(table.column("user_id").to_pylist())
        log.info(f"[E2.1] Loaded {len(test_users)} existing test users from MinIO")
        return test_users
    except client.exceptions.NoSuchKey:
        pass
    except Exception as e:
        log.warning(f"[E2.1] Could not load test.parquet: {e}")

    # First run — pick 10% of users, min 2, max 10
    n_test = max(2, min(10, int(len(all_user_ids) * 0.10)))
    test_users = set(random.sample(all_user_ids, min(n_test, len(all_user_ids))))
    log.info(f"[E2.1] Bootstrap: selected {len(test_users)} test users from {len(all_user_ids)} total")
    return test_users


def e2_split(events: list[dict], test_users: set[str]) -> tuple[list[dict], list[dict], list[dict]]:
    """Split events into train (80%), val (20%), test (held-out). Split by user."""
    test_events  = [e for e in events if e["user_id"] in test_users]
    other_events = [e for e in events if e["user_id"] not in test_users]

    other_users = list({e["user_id"] for e in other_events})
    random.shuffle(other_users)
    split_idx = max(1, int(len(other_users) * 0.8))
    train_users = set(other_users[:split_idx])

    train = [e for e in other_events if e["user_id"] in train_users]
    val   = [e for e in other_events if e["user_id"] not in train_users]

    return train, val, test_events


def e2_split_sanity(train: list[dict], val: list[dict], test: list[dict], test_users: set[str]) -> dict:
    """E2.2 — Verify no user leakage between splits."""
    train_users = {e["user_id"] for e in train}
    val_users   = {e["user_id"] for e in val}

    overlap_tv = train_users & val_users
    test_in_train = test_users & train_users
    test_in_val   = test_users & val_users

    parity_warning = bool(overlap_tv or test_in_train or test_in_val)
    if overlap_tv:
        log.warning(f"[E2.2] {len(overlap_tv)} users appear in both train and val!")
    if test_in_train or test_in_val:
        log.warning(f"[E2.2] Test users leaked into train/val!")

    log.info(f"[E2.2] train={len(train_users)} users/{len(train)} rows, "
             f"val={len(val_users)} users/{len(val)} rows, "
             f"test={len(test_users)} users/{len(test)} rows")

    return {
        "unique_users_train": len(train_users),
        "unique_users_val":   len(val_users),
        "unique_users_test":  len(test_users),
        "split_parity_warning": parity_warning,
    }


def e2_label_parity(train: list[dict], val: list[dict]) -> dict:
    """E2.3 — Compare mean label per event type between train and val."""
    def mean_by_type(events):
        sums: dict[str, list] = defaultdict(list)
        for e in events:
            sums[e["event_type"]].append(float(e["label"]))
        return {k: sum(v) / len(v) for k, v in sums.items()}

    train_means = mean_by_type(train)
    val_means   = mean_by_type(val)

    parity_warning = False
    for et in set(train_means) | set(val_means):
        t = train_means.get(et, 0.0)
        v = val_means.get(et, 0.0)
        if abs(t - v) > 0.2:
            log.warning(f"[E2.3] Label parity: {et} train_mean={t:.3f} val_mean={v:.3f} diff={abs(t-v):.3f} ⚠")
            parity_warning = True

    return {"split_parity_warning": parity_warning}


def e2_embedding_norm_drift(events: list[dict], clip: dict[str, list[float]], client) -> dict:
    """E2.4 — Check CLIP embedding norm drift vs baseline."""
    norms = [np.linalg.norm(clip[e["asset_id"]]) for e in events if e["asset_id"] in clip]
    if not norms:
        return {"embedding_mean_norm": 0.0, "embedding_std_norm": 0.0, "embedding_norm_drift_warning": False}

    mean_norm = float(np.mean(norms))
    std_norm  = float(np.std(norms))

    drift_warning = False
    try:
        obj = client.get_object(Bucket=BUCKET, Key=BASELINE_STATS_KEY)
        baseline = json.loads(obj["Body"].read())
        b_mean = baseline["embedding_mean_norm"]
        b_std  = baseline["embedding_std_norm"]
        if b_std > 0 and abs(mean_norm - b_mean) > 2 * b_std:
            log.warning(f"[E2.4] Embedding norm drift: current={mean_norm:.3f}, baseline={b_mean:.3f}±{b_std:.3f} ⚠")
            drift_warning = True
    except Exception:
        # First run — save baseline
        baseline_data = {"embedding_mean_norm": mean_norm, "embedding_std_norm": std_norm}
        client.put_object(Bucket=BUCKET, Key=BASELINE_STATS_KEY,
                          Body=json.dumps(baseline_data).encode(), ContentType="application/json")
        log.info(f"[E2.4] Saved baseline embedding stats: mean={mean_norm:.3f}, std={std_norm:.3f}")

    return {
        "embedding_mean_norm": round(mean_norm, 4),
        "embedding_std_norm":  round(std_norm, 4),
        "embedding_norm_drift_warning": drift_warning,
    }


def e2_score_interaction_correlation(events: list[dict], conn) -> dict:
    """E2.5 — Spearman-r between aesthetic_scores and mean interaction label."""
    asset_ids = list({e["asset_id"] for e in events})
    scores = read_aesthetic_scores(conn, asset_ids)

    # Mean label per asset across all events this week
    asset_labels: dict[str, list] = defaultdict(list)
    for e in events:
        asset_labels[e["asset_id"]].append(float(e["label"]))

    # Only assets that have both a score and interactions
    common = [aid for aid in asset_labels if aid in scores]
    if len(common) < 5:
        log.info(f"[E2.5] Not enough scored assets for correlation ({len(common)}), skipping")
        return {"score_interaction_spearman_r": None, "score_interaction_warning": False}

    pred  = [scores[aid] for aid in common]
    label = [sum(asset_labels[aid]) / len(asset_labels[aid]) for aid in common]
    r, _ = spearmanr(pred, label)
    r = round(float(r), 4)

    warning = r < 0.3
    log.info(f"[E2.5] Score-interaction Spearman-r={r}{' ⚠ <0.3' if warning else ''}")
    return {"score_interaction_spearman_r": r, "score_interaction_warning": warning}


def e2_interaction_rate_trend(events: list[dict], conn, client, today: str) -> dict:
    """E2.6 — Compare interaction rate to last week's dataset_card.json."""
    assets_scored = read_assets_scored_count(conn)
    rate = len(events) / assets_scored if assets_scored > 0 else 0.0

    decline_warning = False
    try:
        # Find last week's dataset_card
        resp = client.list_objects_v2(Bucket=BUCKET, Prefix="datasets/v", Delimiter="/")
        versions = sorted(
            [cp["Prefix"].rstrip("/").split("/")[-1] for cp in resp.get("CommonPrefixes", [])
             if cp["Prefix"].rstrip("/").split("/")[-1].startswith("v") and
             cp["Prefix"].rstrip("/").split("/")[-1] != f"v{today}"],
            reverse=True,
        )
        if versions:
            prev_key = f"datasets/{versions[0]}/personalized-flickr/dataset_card.json"
            obj = client.get_object(Bucket=BUCKET, Key=prev_key)
            prev_card = json.loads(obj["Body"].read())
            prev_rate = prev_card.get("quality", {}).get("training_set", {}).get("interaction_rate", 0.0)
            if prev_rate > 0 and (prev_rate - rate) / prev_rate > 0.20:
                log.warning(f"[E2.6] Interaction rate declined >20%: {prev_rate:.3f} → {rate:.3f} ⚠")
                decline_warning = True
    except Exception as e:
        log.info(f"[E2.6] Could not load previous dataset_card for trend: {e}")

    return {"interaction_rate": round(rate, 4), "interaction_rate_decline_warning": decline_warning}


# ── Parquet writing ───────────────────────────────────────────────────────────

PARQUET_SCHEMA = pa.schema([
    pa.field("user_id",        pa.string()),
    pa.field("asset_id",       pa.string()),
    pa.field("clip_embedding", pa.list_(pa.float32(), CLIP_DIM)),
    pa.field("label",          pa.float32()),
    pa.field("event_type",     pa.string()),
])


def to_table(rows: list[dict], clip: dict[str, list[float]]) -> pa.Table:
    return pa.table(
        {
            "user_id":        [r["user_id"] for r in rows],
            "asset_id":       [r["asset_id"] for r in rows],
            "clip_embedding": [[float(x) for x in clip[r["asset_id"]]] for r in rows],
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
    log.info(f"[minio] Uploaded s3://{BUCKET}/{key} ({buf.tell()} bytes)")


def upload_json(client, data: dict, key: str):
    client.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(data, indent=2).encode(), ContentType="application/json")
    log.info(f"[minio] Uploaded s3://{BUCKET}/{key}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"datasets/v{today}/personalized-flickr"
    client = s3()
    conn = get_conn()

    # ── E1: Ingestion QA ──────────────────────────────────────────────────────
    raw_events = read_raw_events(conn)
    log.info(f"Read {len(raw_events)} raw events (last {EVENT_WINDOW_DAYS} days)")

    events, schema_stats = e1_schema_validate(raw_events)
    signal_stats = e1_signal_distribution(events)

    asset_ids = list({e["asset_id"] for e in events})
    clip = read_clip(conn, asset_ids)
    events, clip_stats = e1_clip_coverage(events, clip)

    # Burst grouping
    events = burst_group(events)

    # ── E1.2: Volume gate ─────────────────────────────────────────────────────
    all_user_ids = list({e["user_id"] for e in events})
    test_users = e2_get_or_create_test_users(all_user_ids, client)
    non_test_users = [u for u in all_user_ids if u not in test_users]

    skip_reason = None
    if len(events) < MIN_EVENTS:
        skip_reason = "insufficient_events"
    elif len(non_test_users) < MIN_USERS:
        skip_reason = "insufficient_users"

    if skip_reason:
        log.warning(f"[E1.2] Skipping: {skip_reason} (events={len(events)}, non_test_users={len(non_test_users)})")
        dataset_card = {
            "version": today, "created_at": datetime.now(timezone.utc).isoformat(),
            "skipped": True, "skip_reason": skip_reason,
            "event_window_days": EVENT_WINDOW_DAYS,
            "quality": {"ingestion": {**schema_stats, **clip_stats, **signal_stats}},
        }
        upload_json(client, dataset_card, f"{prefix}/dataset_card.json")
        conn.close()
        return

    # ── E2: Training Set QA ───────────────────────────────────────────────────
    train, val, test_events = e2_split(events, test_users)
    split_stats = e2_split_sanity(train, val, test_events, test_users)
    parity_stats = e2_label_parity(train, val)
    norm_stats = e2_embedding_norm_drift(events, clip, client)
    corr_stats = e2_score_interaction_correlation(events, conn)
    rate_stats = e2_interaction_rate_trend(events, conn, client, today)

    conn.close()

    # ── Write parquets ────────────────────────────────────────────────────────
    upload_parquet(client, to_table(train, clip), f"{prefix}/train.parquet")
    upload_parquet(client, to_table(val, clip),   f"{prefix}/val.parquet")

    # Write test.parquet permanently (only if it doesn't exist yet)
    try:
        client.head_object(Bucket=BUCKET, Key=TEST_PARQUET_KEY)
        log.info(f"[E2.1] test.parquet already exists, skipping write")
    except Exception:
        if test_events:
            upload_parquet(client, to_table(test_events, clip), TEST_PARQUET_KEY)
            log.info(f"[E2.1] Wrote permanent test.parquet ({len(test_events)} rows)")

    # ── dataset_card.json ─────────────────────────────────────────────────────
    label_dist: dict[str, int] = defaultdict(int)
    for e in events:
        label_dist[e["event_type"]] += 1

    dataset_card = {
        "version": today,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "skipped": False,
        "skip_reason": None,
        "event_window_days": EVENT_WINDOW_DAYS,
        "quality": {
            "ingestion": {
                "raw_event_count":            schema_stats["raw_event_count"],
                "null_dropped":               schema_stats["null_dropped"],
                "unknown_type_dropped":       schema_stats["unknown_type_dropped"],
                "no_clip_embedding_dropped":  clip_stats["no_clip_embedding_dropped"],
                "clip_coverage_pct":          clip_stats["clip_coverage_pct"],
                "dominant_event_type_warning": signal_stats["dominant_event_type_warning"],
                "signal_inversion_warning":   signal_stats["signal_inversion_warning"],
            },
            "training_set": {
                "post_burst_group_count":         len(events),
                "unique_users_train":             split_stats["unique_users_train"],
                "unique_users_val":               split_stats["unique_users_val"],
                "unique_users_test":              split_stats["unique_users_test"],
                "split_parity_warning":           split_stats["split_parity_warning"] or parity_stats["split_parity_warning"],
                "embedding_mean_norm":            norm_stats["embedding_mean_norm"],
                "embedding_std_norm":             norm_stats["embedding_std_norm"],
                "embedding_norm_drift_warning":   norm_stats["embedding_norm_drift_warning"],
                "score_interaction_spearman_r":   corr_stats["score_interaction_spearman_r"],
                "score_interaction_warning":      corr_stats["score_interaction_warning"],
                "interaction_rate":               rate_stats["interaction_rate"],
                "interaction_rate_decline_warning": rate_stats["interaction_rate_decline_warning"],
            },
        },
        "splits": {
            "train": {"users": split_stats["unique_users_train"], "rows": len(train)},
            "val":   {"users": split_stats["unique_users_val"],   "rows": len(val)},
            "test":  {"users": split_stats["unique_users_test"],  "rows": len(test_events)},
        },
        "event_label_distribution": dict(label_dist),
        "schema": {
            "columns": ["user_id", "asset_id", "clip_embedding", "label", "event_type"],
            "clip_dim": CLIP_DIM,
        },
    }

    upload_json(client, dataset_card, f"{prefix}/dataset_card.json")
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

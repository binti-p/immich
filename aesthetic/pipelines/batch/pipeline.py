"""
Batch pipeline — runs weekly

Checkpoints:
  E1: Ingestion QA   — schema validation, volume gate, signal distribution, CLIP coverage
  E2: Training set QA — chronological split per user, temporal leakage checks, label parity,
                        embedding norm drift, score-interaction correlation, interaction rate trend

Split strategy:
  - 10% of users held out as permanent test set (created once, reused forever)
  - Remaining 90% of users: chronological split per user at burst level
    - Oldest 80% of bursts → train
    - Newest 20% of bursts → val
  - All users appear in all splits (train/val), test users excluded from train/val
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

EVENT_WINDOW_DAYS  = int(os.environ.get("EVENT_WINDOW_DAYS", "30"))
BURST_WINDOW_SECS  = int(os.environ.get("BURST_WINDOW_SECS", "60"))
MIN_EVENTS         = int(os.environ.get("MIN_EVENTS", "50"))
MIN_USERS          = int(os.environ.get("MIN_USERS", "1"))
MIN_USER_EVENTS    = int(os.environ.get("MIN_USER_EVENTS", "3"))
TRIGGER_TRAINING   = os.environ.get("TRIGGER_TRAINING", "false").lower() == "true"
ARGO_WEBHOOK       = os.environ.get("ARGO_WEBHOOK_URL", "http://129.114.27.253:31234/aesthetic-hub/train")

CLIP_DIM = 768
VALID_EVENT_TYPES = {"favorite", "unfavorite", "archive", "delete", "album_add", "download", "share", "view_expanded"}

TEST_PARQUET_KEY    = "datasets/personalized-flickr/test.parquet"
BASELINE_STATS_KEY  = "datasets/baseline_stats.json"

# Archive retention: keep last N days in Postgres, flush older to MinIO
ARCHIVE_RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "60"))

# Model/dataset retention: keep last N versions in MinIO, delete older artifacts (keep cards)
KEEP_LAST_N_VERSIONS = int(os.environ.get("KEEP_LAST_N_VERSIONS", "3"))


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
    """
    Group events by (user_id, asset_id) and deduplicate within 60s windows.
    Returns events with burst_id added.
    """
    grouped: dict[tuple, list] = defaultdict(list)
    for e in events:
        grouped[(e["user_id"], e["asset_id"])].append(e)

    result = []
    burst_counter = 0
    for evts in grouped.values():
        evts.sort(key=lambda x: x["event_time"])
        i = 0
        while i < len(evts):
            burst_start = evts[i]["event_time"]
            j = i
            while j < len(evts) and (evts[j]["event_time"] - burst_start).total_seconds() <= BURST_WINDOW_SECS:
                j += 1
            # Take last event in burst window
            burst_event = evts[j - 1].copy()
            burst_event["burst_id"] = f"burst_{burst_counter}"
            burst_event["burst_start_time"] = burst_start
            result.append(burst_event)
            burst_counter += 1
            i = j

    log.info(f"[burst] {len(result)} bursts from {len(events)} raw events")
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


def e2_split_chronological(bursts: list[dict], test_users: set[str]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    E2.1 — Chronological split per user at burst level.
    
    For each non-test user:
      - Sort bursts by burst_start_time
      - Oldest 80% of bursts → train
      - Newest 20% of bursts → val
    
    Test users:
      - All their bursts go to test split
    
    Returns: (train_bursts, val_bursts, test_bursts)
    """
    # Separate test users
    test_bursts = [b for b in bursts if b["user_id"] in test_users]
    non_test_bursts = [b for b in bursts if b["user_id"] not in test_users]
    
    # Group by user
    user_bursts: dict[str, list[dict]] = defaultdict(list)
    for b in non_test_bursts:
        user_bursts[b["user_id"]].append(b)
    
    train_bursts = []
    val_bursts = []
    
    for user_id, bursts_list in user_bursts.items():
        # Sort by burst start time (chronological)
        bursts_list.sort(key=lambda b: b["burst_start_time"])
        
        # Split 80/20
        n = len(bursts_list)
        split_idx = max(1, int(n * 0.80))  # At least 1 burst in train
        
        train_bursts.extend(bursts_list[:split_idx])
        val_bursts.extend(bursts_list[split_idx:])
    
    log.info(f"[E2.1] Chronological split: train={len(train_bursts)} bursts, "
             f"val={len(val_bursts)} bursts, test={len(test_bursts)} bursts")
    
    return train_bursts, val_bursts, test_bursts


def e2_split_sanity_checks(train: list[dict], val: list[dict], test: list[dict]) -> dict:
    """
    E2.2 — Three critical checks for chronological split integrity.
    
    1. No temporal leakage per user: train_max_time < val_min_time
    2. No user appears in val without appearing in train: val_users ⊆ train_users
    3. No burst_id appears in more than one split
    
    Returns dict with check results and warnings (does not exit on failure).
    """
    train_users = {b["user_id"] for b in train}
    val_users = {b["user_id"] for b in val}
    test_users = {b["user_id"] for b in test}
    
    train_bursts = {b["burst_id"] for b in train}
    val_bursts = {b["burst_id"] for b in val}
    test_bursts = {b["burst_id"] for b in test}
    
    warnings = []
    
    # ── Check 1: No temporal leakage per user ────────────────────────────────
    temporal_violations = []
    users_in_both = train_users & val_users
    
    for user_id in users_in_both:
        user_train = [b for b in train if b["user_id"] == user_id]
        user_val = [b for b in val if b["user_id"] == user_id]
        
        train_max = max(b["burst_start_time"] for b in user_train)
        val_min = min(b["burst_start_time"] for b in user_val)
        
        if train_max >= val_min:
            temporal_violations.append({
                "user_id": user_id,
                "train_max_time": train_max.isoformat(),
                "val_min_time": val_min.isoformat(),
            })
    
    if temporal_violations:
        log.warning(f"[E2.2] ⚠ CHECK 1 WARNING: Temporal leakage detected for {len(temporal_violations)} users")
        for v in temporal_violations[:5]:  # Show first 5
            log.warning(f"  User {v['user_id']}: train_max={v['train_max_time']} >= val_min={v['val_min_time']}")
        warnings.append("temporal_leakage")
    else:
        log.info(f"[E2.2] ✓ CHECK 1 PASSED: No temporal leakage ({len(users_in_both)} users checked)")
    
    # ── Check 2: val_users ⊆ train_users ─────────────────────────────────────
    val_only_users = val_users - train_users
    
    if val_only_users:
        log.warning(f"[E2.2] ⚠ CHECK 2 WARNING: {len(val_only_users)} users in val but not in train")
        log.warning(f"  Users: {sorted(list(val_only_users))[:10]}")  # Show first 10
        warnings.append("val_without_train")
    else:
        log.info(f"[E2.2] ✓ CHECK 2 PASSED: All val users appear in train")
    
    # ── Check 3: No burst_id overlap ──────────────────────────────────────────
    train_val_overlap = train_bursts & val_bursts
    train_test_overlap = train_bursts & test_bursts
    val_test_overlap = val_bursts & test_bursts
    
    if train_val_overlap or train_test_overlap or val_test_overlap:
        log.warning(f"[E2.2] ⚠ CHECK 3 WARNING: Burst ID overlap detected")
        if train_val_overlap:
            log.warning(f"  train ∩ val: {len(train_val_overlap)} bursts")
        if train_test_overlap:
            log.warning(f"  train ∩ test: {len(train_test_overlap)} bursts")
        if val_test_overlap:
            log.warning(f"  val ∩ test: {len(val_test_overlap)} bursts")
        warnings.append("burst_overlap")
    else:
        log.info(f"[E2.2] ✓ CHECK 3 PASSED: No burst ID overlap")
    
    # ── Log summary ───────────────────────────────────────────────────────────
    if warnings:
        log.warning(f"[E2.2] ⚠ {len(warnings)} sanity check(s) failed: {warnings}")
        log.warning(f"[E2.2] Pipeline will continue but data quality may be compromised")
    else:
        log.info(f"[E2.2] ✓ All sanity checks passed")
    
    return {
        "unique_users_train": len(train_users),
        "unique_users_val": len(val_users),
        "unique_users_test": len(test_users),
        "unique_bursts_train": len(train_bursts),
        "unique_bursts_val": len(val_bursts),
        "unique_bursts_test": len(test_bursts),
        "temporal_leakage_violations": len(temporal_violations),
        "val_without_train_users": len(val_only_users),
        "burst_overlap_count": len(train_val_overlap) + len(train_test_overlap) + len(val_test_overlap),
        "all_checks_passed": len(warnings) == 0,
        "warnings": warnings,
    }


def e2_label_parity(train: list[dict], val: list[dict]) -> dict:
    """E2.3 — Compare mean label per event type between train and val (not critical)."""
    def mean_by_type(bursts):
        sums: dict[str, list] = defaultdict(list)
        for b in bursts:
            sums[b["event_type"]].append(float(b["label"]))
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

    return {"label_parity_warning": parity_warning}


def e2_label_mean_drift(bursts: list[dict], client, today: str) -> dict:
    """E2.NEW — Check if mean label is drifting negative (users expressing more negative signals)."""
    if not bursts:
        return {
            "label_mean": 0.0,
            "label_mean_previous": None,
            "label_mean_drift": None,
            "label_mean_drift_warning": False,
        }
    
    current_mean = float(np.mean([b["label"] for b in bursts]))
    
    drift_warning = False
    prev_mean = None
    drift_amount = None
    
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
            
            # Try new structure first, fall back to old structure
            prev_mean = prev_card.get("drift", {}).get("label_mean")
            if prev_mean is None:
                # Old structure fallback
                prev_mean = prev_card.get("quality", {}).get("training_set", {}).get("mean_label")
            
            if prev_mean is not None:
                drift_amount = prev_mean - current_mean  # Positive = decline
                if drift_amount > 0.15:
                    log.warning(f"[E2.NEW] Label mean drift: {prev_mean:.3f} → {current_mean:.3f} (decline {drift_amount:.3f}) ⚠")
                    drift_warning = True
    except Exception as e:
        log.info(f"[E2.NEW] Could not load previous dataset_card for label drift: {e}")
    
    log.info(f"[E2.NEW] Mean label={current_mean:.3f}{' ⚠ negative drift' if drift_warning else ''}")
    return {
        "label_mean": round(current_mean, 4),
        "label_mean_previous": round(prev_mean, 4) if prev_mean is not None else None,
        "label_mean_drift": round(drift_amount, 4) if drift_amount is not None else None,
        "label_mean_drift_warning": drift_warning,
    }


def e2_embedding_norm_drift(bursts: list[dict], clip: dict[str, list[float]], client) -> dict:
    """E2.4 — Check CLIP embedding norm drift vs baseline (not critical)."""
    norms = [np.linalg.norm(clip[b["asset_id"]]) for b in bursts if b["asset_id"] in clip]
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


def e2_score_interaction_correlation(bursts: list[dict], conn) -> dict:
    """
    E2.5 — Spearman-r between aesthetic_scores and mean interaction label.
    NORTH STAR METRIC: Is the model aligned with user preference?
    """
    asset_ids = list({b["asset_id"] for b in bursts})
    scores = read_aesthetic_scores(conn, asset_ids)

    # Mean label per asset across all bursts this week
    asset_labels: dict[str, list] = defaultdict(list)
    for b in bursts:
        asset_labels[b["asset_id"]].append(float(b["label"]))

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
    log.info(f"[E2.5] ⭐ NORTH STAR: Score-interaction Spearman-r={r}{' ⚠ <0.3' if warning else ' ✓'}")
    return {"score_interaction_spearman_r": r, "score_interaction_warning": warning}


def e2_interaction_rate_trend(bursts: list[dict], conn, client, today: str) -> dict:
    """
    E2.6 — Compare interaction rate to last week's dataset_card.json.
    CRITICAL: Are users engaging at all? Declining engagement = signal pipeline drying up.
    """
    assets_scored = read_assets_scored_count(conn)
    rate = len(bursts) / assets_scored if assets_scored > 0 else 0.0

    decline_warning = False
    prev_rate = None
    rate_change_pct = None
    
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
            
            # Try new structure first, fall back to old structure
            prev_rate = prev_card.get("drift", {}).get("interaction_rate")
            if prev_rate is None:
                # Old structure fallback
                prev_rate = prev_card.get("quality", {}).get("training_set", {}).get("interaction_rate")
            
            if prev_rate is not None and prev_rate > 0:
                rate_change_pct = (prev_rate - rate) / prev_rate  # Positive = decline
                if rate_change_pct > 0.20:
                    log.warning(f"[E2.6] ⚠ CRITICAL: Interaction rate declined {rate_change_pct:.1%}: {prev_rate:.3f} → {rate:.3f}")
                    decline_warning = True
    except Exception as e:
        log.info(f"[E2.6] Could not load previous dataset_card for trend: {e}")

    log.info(f"[E2.6] Interaction rate={rate:.3f}{' ⚠ declining' if decline_warning else ' ✓'}")
    return {
        "interaction_rate": round(rate, 4),
        "interaction_rate_previous": round(prev_rate, 4) if prev_rate is not None else None,
        "interaction_rate_change_pct": round(rate_change_pct, 4) if rate_change_pct is not None else None,
        "interaction_rate_decline_warning": decline_warning,
    }


# ── Parquet writing ───────────────────────────────────────────────────────────

PARQUET_SCHEMA = pa.schema([
    pa.field("user_id",        pa.string()),
    pa.field("asset_id",       pa.string()),
    pa.field("clip_embedding", pa.list_(pa.float32(), CLIP_DIM)),
    pa.field("label",          pa.float32()),
    pa.field("event_type",     pa.string()),
    pa.field("split",          pa.string()),
    pa.field("burst_id",       pa.string()),
])


def filter_sparse_users(bursts: list[dict]) -> tuple[list[dict], dict]:
    counts: dict[str, int] = defaultdict(int)
    for burst in bursts:
        counts[burst["user_id"]] += 1

    excluded_users = {user_id for user_id, count in counts.items() if count < MIN_USER_EVENTS}
    filtered_bursts = [burst for burst in bursts if burst["user_id"] not in excluded_users]
    excluded_rows = len(bursts) - len(filtered_bursts)

    return filtered_bursts, {
        "min_user_events": MIN_USER_EVENTS,
        "excluded_sparse_users": len(excluded_users),
        "excluded_sparse_rows": excluded_rows,
        "excluded_user_ids": sorted(excluded_users),
    }


def to_table(rows: list[dict], clip: dict[str, list[float]], split: str) -> pa.Table:
    return pa.table(
        {
            "user_id":        [r["user_id"] for r in rows],
            "asset_id":       [r["asset_id"] for r in rows],
            "clip_embedding": [[float(x) for x in clip[r["asset_id"]]] for r in rows],
            "label":          [float(r["label"]) for r in rows],
            "event_type":     [r["event_type"] for r in rows],
            "split":          [split for _ in rows],
            "burst_id":       [r["burst_id"] for r in rows],
        },
        schema=PARQUET_SCHEMA,
    )


def upload_parquet(client, table: pa.Table, key: str):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    size_bytes = buf.tell()
    buf.seek(0)
    client.upload_fileobj(buf, BUCKET, key)
    log.info(f"[minio] Uploaded s3://{BUCKET}/{key} ({size_bytes} bytes)")


def upload_json(client, data: dict, key: str):
    client.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(data, indent=2).encode(), ContentType="application/json")
    log.info(f"[minio] Uploaded s3://{BUCKET}/{key}")


def upload_manifest(client, tables: list[pa.Table], prefix: str):
    """Upload retraining manifest (train + val only, no test)."""
    non_empty_tables = [table for table in tables if table.num_rows > 0]
    if not non_empty_tables:
        raise RuntimeError("No rows available to build a retraining manifest")
    manifest_table = non_empty_tables[0] if len(non_empty_tables) == 1 else pa.concat_tables(non_empty_tables)

    manifest_parquet_key = f"{prefix}/retraining_manifest.parquet"
    upload_parquet(client, manifest_table, manifest_parquet_key)

    manifest_df = manifest_table.to_pandas()
    manifest_df["clip_embedding"] = manifest_df["clip_embedding"].apply(
        lambda value: json.dumps([float(item) for item in value])
    )
    buf = io.StringIO()
    manifest_df.to_csv(buf, index=False)
    client.put_object(
        Bucket=BUCKET,
        Key=f"{prefix}/retraining_manifest.csv",
        Body=buf.getvalue().encode(),
        ContentType="text/csv",
    )
    log.info(f"[minio] Uploaded s3://{BUCKET}/{prefix}/retraining_manifest.csv")

    return manifest_table


# ── Archive & Flush ───────────────────────────────────────────────────────────

def flush_interaction_events(conn, client, today: str):
    """
    Archive old interaction_events to MinIO and delete from Postgres.
    Keeps last ARCHIVE_RETENTION_DAYS in Postgres, flushes older to parquet.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_RETENTION_DAYS)
    
    log.info(f"\n[flush] Archiving interaction_events older than {cutoff.date()}...")
    
    # Read old events to archive
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "eventId"   AS event_id,
                   "assetId"::text  AS asset_id,
                   "userId"::text   AS user_id,
                   "eventType"      AS event_type,
                   label,
                   source,
                   "eventTime"      AS event_time,
                   "ingestedAt"     AS ingested_at,
                   "deletedAt"      AS deleted_at
            FROM   interaction_events
            WHERE  "eventTime" < %s
            ORDER  BY "eventTime"
            """,
            (cutoff,),
        )
        old_events = [dict(r) for r in cur.fetchall()]
    
    if not old_events:
        log.info(f"[flush] No old interaction_events to archive")
        return
    
    log.info(f"[flush] Found {len(old_events)} old interaction_events to archive")
    
    # Convert to parquet
    schema = pa.schema([
        pa.field("event_id", pa.string()),
        pa.field("asset_id", pa.string()),
        pa.field("user_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("label", pa.float64()),
        pa.field("source", pa.string()),
        pa.field("event_time", pa.timestamp("us", tz="UTC")),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("deleted_at", pa.timestamp("us", tz="UTC")),
    ])
    
    table = pa.table(
        {
            "event_id": [e["event_id"] for e in old_events],
            "asset_id": [e["asset_id"] for e in old_events],
            "user_id": [e["user_id"] for e in old_events],
            "event_type": [e["event_type"] for e in old_events],
            "label": [float(e["label"]) for e in old_events],
            "source": [e["source"] for e in old_events],
            "event_time": [e["event_time"] for e in old_events],
            "ingested_at": [e["ingested_at"] for e in old_events],
            "deleted_at": [e["deleted_at"] for e in old_events],
        },
        schema=schema,
    )
    
    # Upload to MinIO
    key = f"production-sim/interactions/interaction_events_{today}.parquet"
    upload_parquet(client, table, key)
    
    # Delete from Postgres
    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM interaction_events WHERE "eventTime" < %s',
            (cutoff,),
        )
        deleted_count = cur.rowcount
        conn.commit()
    
    log.info(f"[flush] ✓ Archived {len(old_events)} events, deleted {deleted_count} from Postgres")


def flush_inference_log(conn, client, today: str):
    """
    Archive old inference_log to MinIO and delete from Postgres.
    Keeps last ARCHIVE_RETENTION_DAYS in Postgres, flushes older to parquet.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_RETENTION_DAYS)
    
    log.info(f"\n[flush] Archiving inference_log older than {cutoff.date()}...")
    
    # Read old logs to archive
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "requestId"         AS request_id,
                   "assetId"::text     AS asset_id,
                   "userId"::text      AS user_id,
                   "modelVersion"      AS model_version,
                   "isColdStart"       AS is_cold_start,
                   alpha,
                   "requestReceivedAt" AS request_received_at,
                   "computedAt"        AS computed_at
            FROM   inference_log
            WHERE  "requestReceivedAt" < %s
            ORDER  BY "requestReceivedAt"
            """,
            (cutoff,),
        )
        old_logs = [dict(r) for r in cur.fetchall()]
    
    if not old_logs:
        log.info(f"[flush] No old inference_log to archive")
        return
    
    log.info(f"[flush] Found {len(old_logs)} old inference_log entries to archive")
    
    # Convert to parquet
    schema = pa.schema([
        pa.field("request_id", pa.string()),
        pa.field("asset_id", pa.string()),
        pa.field("user_id", pa.string()),
        pa.field("model_version", pa.string()),
        pa.field("is_cold_start", pa.bool_()),
        pa.field("alpha", pa.float64()),
        pa.field("request_received_at", pa.timestamp("us", tz="UTC")),
        pa.field("computed_at", pa.timestamp("us", tz="UTC")),
    ])
    
    table = pa.table(
        {
            "request_id": [log["request_id"] for log in old_logs],
            "asset_id": [log["asset_id"] for log in old_logs],
            "user_id": [log["user_id"] for log in old_logs],
            "model_version": [log["model_version"] for log in old_logs],
            "is_cold_start": [log["is_cold_start"] for log in old_logs],
            "alpha": [float(log["alpha"]) for log in old_logs],
            "request_received_at": [log["request_received_at"] for log in old_logs],
            "computed_at": [log["computed_at"] for log in old_logs],
        },
        schema=schema,
    )
    
    # Upload to MinIO
    key = f"production-sim/inference-log/inference_log_{today}.parquet"
    upload_parquet(client, table, key)
    
    # Delete from Postgres
    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM inference_log WHERE "requestReceivedAt" < %s',
            (cutoff,),
        )
        deleted_count = cur.rowcount
        conn.commit()
    
    log.info(f"[flush] ✓ Archived {len(old_logs)} logs, deleted {deleted_count} from Postgres")


def cleanup_old_models(client, today: str):
    """
    Delete old model artifacts (PTH, embeddings) but keep model_card.json.
    Keeps last KEEP_LAST_N_VERSIONS, deletes older artifacts.
    """
    log.info(f"\n[cleanup] Cleaning up old model artifacts (keeping last {KEEP_LAST_N_VERSIONS} versions)...")
    
    try:
        # List all model versions
        resp = client.list_objects_v2(Bucket=BUCKET, Prefix="models/v", Delimiter="/")
        versions = sorted(
            [cp["Prefix"].rstrip("/").split("/")[-1] for cp in resp.get("CommonPrefixes", [])
             if cp["Prefix"].rstrip("/").split("/")[-1].startswith("v")],
            reverse=True,
        )
        
        if len(versions) <= KEEP_LAST_N_VERSIONS:
            log.info(f"[cleanup] Only {len(versions)} model versions exist, no cleanup needed")
            return
        
        # Delete artifacts from old versions (keep cards)
        versions_to_clean = versions[KEEP_LAST_N_VERSIONS:]
        log.info(f"[cleanup] Found {len(versions_to_clean)} old model versions to clean")
        
        deleted_count = 0
        for version in versions_to_clean:
            prefix = f"models/{version}/"
            
            # List all objects in this version
            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    
                    # Keep model_card.json, delete everything else
                    if key.endswith('model_card.json'):
                        log.info(f"[cleanup]   Keeping {key}")
                        continue
                    
                    # Delete PTH and embeddings
                    if key.endswith('.pth') or key.endswith('.parquet'):
                        client.delete_object(Bucket=BUCKET, Key=key)
                        log.info(f"[cleanup]   Deleted {key}")
                        deleted_count += 1
        
        log.info(f"[cleanup] ✓ Deleted {deleted_count} old model artifacts (kept {len(versions_to_clean)} model_card.json)")
        
    except Exception as e:
        log.warning(f"[cleanup] Model cleanup failed: {e}")


def cleanup_old_datasets(client, today: str):
    """
    Delete old dataset parquets but keep dataset_card.json.
    Keeps last KEEP_LAST_N_VERSIONS, deletes older parquets.
    """
    log.info(f"\n[cleanup] Cleaning up old dataset artifacts (keeping last {KEEP_LAST_N_VERSIONS} versions)...")
    
    try:
        # List all dataset versions
        resp = client.list_objects_v2(Bucket=BUCKET, Prefix="datasets/v", Delimiter="/")
        versions = sorted(
            [cp["Prefix"].rstrip("/").split("/")[-1] for cp in resp.get("CommonPrefixes", [])
             if cp["Prefix"].rstrip("/").split("/")[-1].startswith("v")],
            reverse=True,
        )
        
        if len(versions) <= KEEP_LAST_N_VERSIONS:
            log.info(f"[cleanup] Only {len(versions)} dataset versions exist, no cleanup needed")
            return
        
        # Delete parquets from old versions (keep cards)
        versions_to_clean = versions[KEEP_LAST_N_VERSIONS:]
        log.info(f"[cleanup] Found {len(versions_to_clean)} old dataset versions to clean")
        
        deleted_count = 0
        for version in versions_to_clean:
            prefix = f"datasets/{version}/"
            
            # List all objects in this version
            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    
                    # Keep dataset_card.json, delete everything else
                    if key.endswith('dataset_card.json'):
                        log.info(f"[cleanup]   Keeping {key}")
                        continue
                    
                    # Delete parquets and CSVs
                    if key.endswith('.parquet') or key.endswith('.csv'):
                        client.delete_object(Bucket=BUCKET, Key=key)
                        log.info(f"[cleanup]   Deleted {key}")
                        deleted_count += 1
        
        log.info(f"[cleanup] ✓ Deleted {deleted_count} old dataset artifacts (kept {len(versions_to_clean)} dataset_card.json)")
        
    except Exception as e:
        log.warning(f"[cleanup] Dataset cleanup failed: {e}")


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

    # Burst grouping (adds burst_id and burst_start_time)
    bursts = burst_group(events)

    bursts, sparse_stats = filter_sparse_users(bursts)

    # ── E1.2: Volume gate ─────────────────────────────────────────────────────
    all_user_ids = list({b["user_id"] for b in bursts})
    test_users = e2_get_or_create_test_users(all_user_ids, client)
    non_test_users = [u for u in all_user_ids if u not in test_users]

    skip_reason = None
    if len(bursts) < MIN_EVENTS:
        skip_reason = "insufficient_events"
    elif len(non_test_users) < MIN_USERS:
        skip_reason = "insufficient_users"

    if skip_reason:
        log.warning(f"[E1.2] Skipping: {skip_reason} (bursts={len(bursts)}, non_test_users={len(non_test_users)})")
        dataset_card = {
            "version": f"v{today}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "event_cutoff": (datetime.now(timezone.utc) - timedelta(days=EVENT_WINDOW_DAYS)).isoformat(),
            "skipped": True,
            "skip_reason": skip_reason,
            
            "excluded_rows": {
                "null_or_invalid": schema_stats["null_dropped"] + schema_stats["unknown_type_dropped"],
                "missing_clip_embedding": clip_stats["no_clip_embedding_dropped"],
                "sparse_users": sparse_stats["excluded_sparse_rows"],
                "total_excluded": (
                    schema_stats["null_dropped"] + 
                    schema_stats["unknown_type_dropped"] + 
                    clip_stats["no_clip_embedding_dropped"] + 
                    sparse_stats["excluded_sparse_rows"]
                ),
            },
            
            "quality_warnings": {
                "dominant_event_type": signal_stats["dominant_event_type_warning"],
                "signal_inversion": signal_stats["signal_inversion_warning"],
                "clip_coverage_pct": clip_stats["clip_coverage_pct"],
                "low_clip_coverage_warning": clip_stats["clip_coverage_warning"],
            },
            
            "config": {
                "event_window_days": EVENT_WINDOW_DAYS,
                "burst_window_secs": BURST_WINDOW_SECS,
                "min_user_events": MIN_USER_EVENTS,
                "min_events_threshold": MIN_EVENTS,
                "min_users_threshold": MIN_USERS,
            },
        }
        upload_json(client, dataset_card, f"{prefix}/dataset_card.json")
        conn.close()
        return

    # ── E2: Training Set QA ───────────────────────────────────────────────────
    train, val, test_bursts = e2_split_chronological(bursts, test_users)
    split_stats = e2_split_sanity_checks(train, val, test_bursts)  # Logs warnings if checks fail
    parity_stats = e2_label_parity(train, val)
    label_drift_stats = e2_label_mean_drift(bursts, client, today)
    norm_stats = e2_embedding_norm_drift(bursts, clip, client)
    corr_stats = e2_score_interaction_correlation(bursts, conn)
    rate_stats = e2_interaction_rate_trend(bursts, conn, client, today)

    conn.close()

    # ── Write parquets ────────────────────────────────────────────────────────
    train_table = to_table(train, clip, "train")
    val_table = to_table(val, clip, "val")
    test_table = to_table(test_bursts, clip, "test")

    # Upload train and val (used for retraining)
    upload_parquet(client, train_table, f"{prefix}/train.parquet")
    upload_parquet(client, val_table, f"{prefix}/val.parquet")

    # Write permanent test.parquet (only if it doesn't exist yet)
    try:
        client.head_object(Bucket=BUCKET, Key=TEST_PARQUET_KEY)
        log.info(f"[E2.1] test.parquet already exists, skipping write")
    except Exception:
        if test_bursts:
            upload_parquet(client, test_table, TEST_PARQUET_KEY)
            log.info(f"[E2.1] Wrote permanent test.parquet ({len(test_bursts)} bursts)")

    # Upload manifest (train + val only, no test)
    manifest_table = upload_manifest(
        client,
        [train_table, val_table],  # ← Only train and val
        prefix,
    )

    # ── dataset_card.json ─────────────────────────────────────────────────────
    label_dist: dict[str, int] = defaultdict(int)
    for b in bursts:
        label_dist[b["event_type"]] += 1

    dataset_card = {
        "version": f"v{today}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "event_cutoff": (datetime.now(timezone.utc) - timedelta(days=EVENT_WINDOW_DAYS)).isoformat(),
        "skipped": False,
        "skip_reason": None,
        
        # ── Splits ────────────────────────────────────────────────────────────
        "splits": {
            "train": {
                "users": split_stats["unique_users_train"],
                "bursts": split_stats["unique_bursts_train"],
            },
            "val": {
                "users": split_stats["unique_users_val"],
                "bursts": split_stats["unique_bursts_val"],
            },
            "test": {
                "users": split_stats["unique_users_test"],
                "bursts": split_stats["unique_bursts_test"],
            },
        },
        
        # ── Excluded Rows ─────────────────────────────────────────────────────
        "excluded_rows": {
            "null_or_invalid": schema_stats["null_dropped"] + schema_stats["unknown_type_dropped"],
            "missing_clip_embedding": clip_stats["no_clip_embedding_dropped"],
            "sparse_users": sparse_stats["excluded_sparse_rows"],
            "total_excluded": (
                schema_stats["null_dropped"] + 
                schema_stats["unknown_type_dropped"] + 
                clip_stats["no_clip_embedding_dropped"] + 
                sparse_stats["excluded_sparse_rows"]
            ),
        },
        
        # ── Label Distribution ────────────────────────────────────────────────
        "label_distribution": dict(label_dist),
        
        # ── Split Sanity Checks ───────────────────────────────────────────────
        "split_sanity": {
            "passed": split_stats["all_checks_passed"],
            "temporal_leakage_violations": split_stats["temporal_leakage_violations"],
            "burst_overlap_violations": split_stats["burst_overlap_count"],
            "val_users_without_train": split_stats["val_without_train_users"],
            "warnings": split_stats.get("warnings", []),
        },
        
        # ── Drift & Quality Metrics ───────────────────────────────────────────
        "drift": {
            # North Star Metric
            "score_interaction_spearman_r": corr_stats["score_interaction_spearman_r"],
            "score_interaction_warning": corr_stats["score_interaction_warning"],
            
            # Interaction Rate (Critical)
            "interaction_rate": rate_stats["interaction_rate"],
            "interaction_rate_previous": rate_stats["interaction_rate_previous"],
            "interaction_rate_change_pct": rate_stats["interaction_rate_change_pct"],
            "interaction_rate_decline_warning": rate_stats["interaction_rate_decline_warning"],
            
            # Label Mean Drift
            "label_mean": label_drift_stats["label_mean"],
            "label_mean_previous": label_drift_stats["label_mean_previous"],
            "label_mean_drift": label_drift_stats["label_mean_drift"],
            "label_mean_drift_warning": label_drift_stats["label_mean_drift_warning"],
            
            # Embedding Norm Drift
            "embedding_mean_norm": norm_stats["embedding_mean_norm"],
            "embedding_std_norm": norm_stats["embedding_std_norm"],
            "embedding_norm_drift_warning": norm_stats["embedding_norm_drift_warning"],
        },
        
        # ── Quality Warnings ──────────────────────────────────────────────────
        "quality_warnings": {
            "dominant_event_type": signal_stats["dominant_event_type_warning"],
            "signal_inversion": signal_stats["signal_inversion_warning"],
            "clip_coverage_pct": clip_stats["clip_coverage_pct"],
            "low_clip_coverage_warning": clip_stats["clip_coverage_warning"],
            "label_parity_warning": parity_stats["label_parity_warning"],
        },
        
        # ── Metadata ──────────────────────────────────────────────────────────
        "config": {
            "event_window_days": EVENT_WINDOW_DAYS,
            "burst_window_secs": BURST_WINDOW_SECS,
            "min_user_events": MIN_USER_EVENTS,
            "split_strategy": "chronological_per_user_burst_level",
        },
        
        "schema": {
            "columns": ["user_id", "asset_id", "clip_embedding", "label", "event_type", "split", "burst_id"],
            "clip_dim": CLIP_DIM,
        },
        
        "artifacts": {
            "train_parquet": f"{prefix}/train.parquet",
            "val_parquet": f"{prefix}/val.parquet",
            "test_parquet_permanent": TEST_PARQUET_KEY,
            "manifest_parquet": f"{prefix}/retraining_manifest.parquet",
            "manifest_csv": f"{prefix}/retraining_manifest.csv",
        },
    }

    upload_json(client, dataset_card, f"{prefix}/dataset_card.json")
    log.info(f"Pipeline complete. Dataset version: v{today}")
    
    # Log summary
    log.info(f"\n{'='*60}")
    log.info(f"SUMMARY:")
    log.info(f"  Train: {split_stats['unique_bursts_train']} bursts from {split_stats['unique_users_train']} users")
    log.info(f"  Val:   {split_stats['unique_bursts_val']} bursts from {split_stats['unique_users_val']} users")
    log.info(f"  Test:  {split_stats['unique_bursts_test']} bursts from {split_stats['unique_users_test']} users")
    log.info(f"  ⭐ Score-interaction Spearman-r: {corr_stats['score_interaction_spearman_r']}")
    log.info(f"  Interaction rate: {rate_stats['interaction_rate']}")
    log.info(f"  Mean label: {label_drift_stats['label_mean']}")
    log.info(f"{'='*60}\n")

    # ── Archive & Flush ───────────────────────────────────────────────────────
    # Flush old data from Postgres to MinIO to keep DB lean
    conn = get_conn()
    try:
        flush_interaction_events(conn, client, today)
        flush_inference_log(conn, client, today)
    except Exception as e:
        log.error(f"Flush failed (non-fatal): {e}", exc_info=True)
    finally:
        conn.close()
    
    # ── Cleanup Old Artifacts ─────────────────────────────────────────────────
    # Delete old model/dataset artifacts (keep cards for historical tracking)
    try:
        cleanup_old_models(client, today)
        cleanup_old_datasets(client, today)
    except Exception as e:
        log.error(f"Cleanup failed (non-fatal): {e}", exc_info=True)

    # Optional: trigger Argo training webhook
    if TRIGGER_TRAINING:
        try:
            resp = requests.post(ARGO_WEBHOOK, timeout=10)
            log.info(f"Argo webhook: {resp.status_code}")
        except Exception as e:
            log.warning(f"Argo webhook failed (non-fatal): {e}")


if __name__ == "__main__":
    main()

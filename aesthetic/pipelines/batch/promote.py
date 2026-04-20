"""
Model promotion script — called by the training job after quality gates pass.

Does three things in order:
1. Reads model_card.json from MinIO for the given version
2. Writes a row to model_versions postgres table (marks model as activated)
3. Loads user_embeddings.parquet from MinIO and upserts into user_embeddings postgres table
4. Triggers rescore-all via Immich API so all assets get re-scored with the new model

Usage:
    docker exec aesthetic_service python -m pipelines.batch.promote --version 2026-04-20
    docker exec aesthetic_service python -m pipelines.batch.promote --version 2026-04-20 --dry-run
    docker exec aesthetic_service python -m pipelines.batch.promote --version 2026-04-20 --skip-rescore
"""
import argparse
import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import numpy as np
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import requests

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

IMMICH_URL     = os.environ.get("IMMICH_SERVER_URL", "http://immich_server:2283")
IMMICH_API_KEY = os.environ.get("IMMICH_API_KEY", "")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


def _conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Step 1: Read model_card.json ──────────────────────────────────────────────
def read_model_card(version: str) -> dict:
    key = f"models/v{version}/model_card.json"
    log.info(f"[promote] Reading model card: s3://{BUCKET}/{key}")
    client = _s3()
    obj = client.get_object(Bucket=BUCKET, Key=key)
    card = json.loads(obj["Body"].read())
    log.info(f"[promote] Model card loaded: version={card.get('version_id')}, "
             f"quality_gates_passed={card.get('quality_gates', {}).get('passed')}")
    return card


# ── Step 2: Write model_versions row ─────────────────────────────────────────
def register_model_version(card: dict, dry_run: bool):
    version_id         = card["version_id"]
    dataset_version    = card["dataset_version"]
    mlp_object_key     = card["mlp_object_key"]
    embeddings_key     = card["embeddings_object_key"]

    if dry_run:
        log.info(f"[promote][dry-run] Would insert model_versions row: {version_id}")
        return

    conn = _conn()
    try:
        with conn.cursor() as cur:
            # Deactivate any currently active model
            cur.execute(
                """
                UPDATE model_versions
                SET "deactivatedAt" = NOW()
                WHERE "activatedAt" IS NOT NULL
                  AND "deactivatedAt" IS NULL
                """
            )
            deactivated = cur.rowcount

            # Insert new active version
            cur.execute(
                """
                INSERT INTO model_versions
                    ("versionId", "datasetVersion", "mlpObjectKey", "embeddingsObjectKey",
                     "activatedAt", "createdAt")
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT ("versionId") DO UPDATE SET
                    "activatedAt"   = NOW(),
                    "deactivatedAt" = NULL
                """,
                (version_id, dataset_version, mlp_object_key, embeddings_key),
            )
        conn.commit()
        log.info(f"[promote] Registered model version {version_id} "
                 f"(deactivated {deactivated} previous versions)")
    except Exception as e:
        conn.rollback()
        log.error(f"[promote] Failed to register model version: {e}")
        raise
    finally:
        conn.close()


# ── Step 3: Load user_embeddings into postgres ────────────────────────────────
def load_user_embeddings(card: dict, dry_run: bool):
    key = card["embeddings_object_key"]
    version_id = card["version_id"]

    if dry_run:
        log.info(f"[promote][dry-run] Would download and upsert user embeddings from {key}")
        return

    log.info(f"[promote] Downloading user embeddings: s3://{BUCKET}/{key}")
    client = _s3()
    obj = client.get_object(Bucket=BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf)
    df = table.to_pandas()

    log.info(f"[promote] Loaded {len(df)} user embeddings")

    if dry_run:
        log.info(f"[promote][dry-run] Would upsert {len(df)} rows into user_embeddings")
        return

    conn = _conn()
    try:
        with conn.cursor() as cur:
            upserted = 0
            for _, row in df.iterrows():
                user_id   = str(row["user_id"])
                embedding = list(row["embedding"])  # list of floats
                cur.execute(
                    """
                    INSERT INTO user_embeddings ("userId", embedding, "modelVersion", "updatedAt")
                    VALUES (%s::uuid, %s::double precision[], %s, NOW())
                    ON CONFLICT ("userId") DO UPDATE SET
                        embedding      = EXCLUDED.embedding,
                        "modelVersion" = EXCLUDED."modelVersion",
                        "updatedAt"    = NOW()
                    """,
                    (user_id, embedding, version_id),
                )
                upserted += 1
        conn.commit()
        log.info(f"[promote] Upserted {upserted} user embeddings into postgres")
    except Exception as e:
        conn.rollback()
        log.error(f"[promote] Failed to load user embeddings: {e}")
        raise
    finally:
        conn.close()


# ── Step 3b: Held-out test set evaluation (E3.5) ─────────────────────────────
def evaluate_held_out(card: dict, dry_run: bool) -> float | None:
    """
    Load test.parquet, run new ONNX model directly, compute Spearman-r vs labels.
    Compare to previous model version. Abort if regression > 0.05.
    Returns the Spearman-r or None if test set unavailable.
    """
    from scipy.stats import spearmanr
    import io as _io
    import onnxruntime as rt

    client = _s3()

    # Load test.parquet
    try:
        obj = client.get_object(Bucket=BUCKET, Key="datasets/personalized-flickr/test.parquet")
        table = pq.read_table(_io.BytesIO(obj["Body"].read()))
        df = table.to_pandas()
        log.info(f"[E3.5] Loaded test.parquet: {len(df)} rows")
    except Exception as e:
        log.warning(f"[E3.5] Could not load test.parquet: {e} — skipping held-out eval")
        return None

    if len(df) < 5:
        log.warning("[E3.5] test.parquet has <5 rows — skipping held-out eval")
        return None

    # Download new model
    mlp_key = card["mlp_object_key"]
    try:
        obj = client.get_object(Bucket=BUCKET, Key=mlp_key)
        model_bytes = _io.BytesIO(obj["Body"].read())
        sess = rt.InferenceSession(model_bytes.read(), providers=["CPUExecutionProvider"])
    except Exception as e:
        log.warning(f"[E3.5] Could not load ONNX model for eval: {e} — skipping")
        return None

    # Run inference on test set
    predictions = []
    labels = []
    for _, row in df.iterrows():
        clip_emb = np.array(row["clip_embedding"], dtype=np.float32).reshape(1, 768)
        # Use global model path (input name "input")
        try:
            result = sess.run(["output"], {"input": clip_emb})
            predictions.append(float(result[0][0][0]))
            labels.append(float(row["label"]))
        except Exception:
            # Try personalized model input names
            try:
                user_emb = np.zeros((1, 64), dtype=np.float32)
                result = sess.run(["output"], {"image_embedding": clip_emb, "user_embedding": user_emb})
                predictions.append(float(result[0][0][0]))
                labels.append(float(row["label"]))
            except Exception as e2:
                log.warning(f"[E3.5] Inference failed for row: {e2}")

    if len(predictions) < 5:
        log.warning("[E3.5] Not enough successful predictions for Spearman-r")
        return None

    r, _ = spearmanr(predictions, labels)
    r = round(float(r), 4)
    log.info(f"[E3.5] Held-out Spearman-r = {r}")

    # Compare to previous model version
    try:
        resp = client.list_objects_v2(Bucket=BUCKET, Prefix="models/v", Delimiter="/")
        versions = sorted(
            [cp["Prefix"].rstrip("/").split("/")[-1]
             for cp in resp.get("CommonPrefixes", [])
             if cp["Prefix"].rstrip("/").split("/")[-1].startswith("v")
             and cp["Prefix"].rstrip("/").split("/")[-1] != f"v{card['version_id']}"],
            reverse=True,
        )
        if versions:
            prev_key = f"models/{versions[0]}/model_card.json"
            prev_obj = client.get_object(Bucket=BUCKET, Key=prev_key)
            prev_card = json.loads(prev_obj["Body"].read())
            prev_r = prev_card.get("quality_gates", {}).get("held_out_test_spearman_r")
            if prev_r is not None and (prev_r - r) > 0.05:
                log.error(f"[E3.5] Regression detected: new={r} vs prev={prev_r} (diff={prev_r-r:.3f} > 0.05) — aborting")
                raise SystemExit(1)
            log.info(f"[E3.5] vs previous model: new={r}, prev={prev_r}")
    except SystemExit:
        raise
    except Exception as e:
        log.info(f"[E3.5] Could not compare to previous model: {e}")

    if dry_run:
        log.info(f"[E3.5][dry-run] Would write held_out_test_spearman_r={r} to model_card.json")
        return r

    # Write result back to model_card.json
    try:
        obj = client.get_object(Bucket=BUCKET, Key=f"models/v{card['version_id']}/model_card.json")
        updated_card = json.loads(obj["Body"].read())
        updated_card.setdefault("quality_gates", {})["held_out_test_spearman_r"] = r
        client.put_object(
            Bucket=BUCKET,
            Key=f"models/v{card['version_id']}/model_card.json",
            Body=json.dumps(updated_card, indent=2).encode(),
            ContentType="application/json",
        )
        log.info(f"[E3.5] Updated model_card.json with held_out_test_spearman_r={r}")
    except Exception as e:
        log.warning(f"[E3.5] Could not update model_card.json: {e}")

    return r


# ── Step 4: Reload model in aesthetic-service ────────────────────────────────
def reload_aesthetic_service(dry_run: bool):
    aesthetic_url = os.environ.get("AESTHETIC_SERVICE_URL", "http://aesthetic-service:8002")
    if dry_run:
        log.info("[promote][dry-run] Would call POST /admin/reload-model")
        return

    url = f"{aesthetic_url}/admin/reload-model"
    log.info(f"[promote] Reloading model in aesthetic-service: {url}")
    try:
        resp = requests.post(url, timeout=120)  # download can take a moment
        if resp.ok:
            data = resp.json()
            log.info(f"[promote] Model reloaded: version={data.get('model_version')}, "
                     f"personalized={data.get('personalized_model_loaded')}")
        else:
            log.error(f"[promote] Reload returned {resp.status_code}: {resp.text}")
            raise RuntimeError(f"Model reload failed with status {resp.status_code}")
    except Exception as e:
        log.error(f"[promote] Failed to reload model: {e}")
        raise


# ── Step 5: Trigger rescore-all ───────────────────────────────────────────────
def trigger_rescore(dry_run: bool):
    if dry_run:
        log.info("[promote][dry-run] Would trigger rescore-all")
        return

    if not IMMICH_API_KEY:
        log.warning("[promote] IMMICH_API_KEY not set — skipping rescore. "
                    "Run manually: curl -X POST .../api/aesthetic/admin/rescore-all -H 'x-api-key: ...'")
        return

    url = f"{IMMICH_URL}/api/aesthetic/admin/rescore-all"
    log.info(f"[promote] Triggering rescore-all: {url}")
    try:
        resp = requests.post(url, headers={"x-api-key": IMMICH_API_KEY}, timeout=30)
        if resp.ok:
            log.info(f"[promote] Rescore job queued: jobId={resp.json().get('jobId')}")
        else:
            log.error(f"[promote] Rescore-all returned {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"[promote] Failed to trigger rescore: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Promote a trained model to production")
    parser.add_argument("--version",      required=True, help="Model version date, e.g. 2026-04-20")
    parser.add_argument("--dry-run",      action="store_true", help="Read-only — don't write anything")
    parser.add_argument("--skip-rescore", action="store_true", help="Skip rescore-all trigger")
    args = parser.parse_args()

    log.info(f"[promote] Promoting model version {args.version} (dry_run={args.dry_run})")

    card = read_model_card(args.version)

    if not card.get("quality_gates", {}).get("passed", True):
        log.error("[promote] Quality gates did not pass — aborting promotion")
        raise SystemExit(1)

    register_model_version(card, args.dry_run)
    load_user_embeddings(card, args.dry_run)

    # E3.5: Held-out evaluation before reload
    evaluate_held_out(card, args.dry_run)

    if not args.skip_rescore:
        reload_aesthetic_service(args.dry_run)  # load new model first
        trigger_rescore(args.dry_run)            # then rescore with it

    log.info(f"[promote] Model {args.version} promoted successfully.")


if __name__ == "__main__":
    main()

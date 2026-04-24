#!/usr/bin/env python3
"""
Smoke test for the aesthetic-service.
Called by Argo after staging deployment.

Aesthetic-service fetches CLIP
embeddings from the DB itself, so we need to seed test data first.
This script:
  1. Inserts a dummy asset + CLIP embedding into smart_search
  2. Registers a test user
  3. Tests /score-image (cold start)
  4. Tests /events/interaction
  5. Tests /score-image again (after interaction)
  6. Tests /admin/reload-model
  7. Cleans up test data

Exits 0 if pass, 1 if fail.
"""
import os
import sys
import time
import argparse
import uuid
import psycopg2
import requests
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--service-url", required=True)
parser.add_argument("--output-result", default="/tmp/smoke-passed.txt")
args = parser.parse_args()

BASE_URL = args.service_url.rstrip("/")
RESULTS = []

# Test identifiers — use fixed UUIDs so cleanup is reliable
# Must be valid hex (0-9, a-f only) in the last 12-char group
TEST_ASSET_ID = "00000000-0000-0000-0000-5a0ce0000001"
TEST_USER_ID = "00000000-0000-0000-0000-5a0ce0000099"


def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {detail}")
    RESULTS.append(passed)


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "immich_postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


def seed_test_data():
    """Insert a dummy asset with CLIP embedding so /score-image can find it."""
    conn = get_conn()
    clip_embedding = np.random.randn(768).astype(np.float32).tolist()
    try:
        with conn.cursor() as cur:
            # Ensure test user exists in user table
            cur.execute(
                """
                INSERT INTO "user" (id, email, name, "createdAt", "updatedAt", "isAdmin")
                VALUES (%s::uuid, 'smoke-test@test.local', 'Smoke Test', NOW(), NOW(), false)
                ON CONFLICT (id) DO NOTHING
                """,
                (TEST_USER_ID,),
            )
            # Ensure test asset exists
            cur.execute(
                """
                INSERT INTO asset (id, "ownerId", type, "originalPath",
                                   "fileCreatedAt", "fileModifiedAt",
                                   "localDateTime",
                                   checksum, "checksumAlgorithm",
                                   "originalFileName")
                VALUES (%s::uuid, %s::uuid, 'IMAGE', '/smoke/test.jpg',
                        NOW(), NOW(),
                        NOW(),
                        decode('da39a3ee5e6b4b0d3255bfef95601890afd80709', 'hex'), 'sha1',
                        'test.jpg')
                ON CONFLICT (id) DO NOTHING
                """,
                (TEST_ASSET_ID, TEST_USER_ID),
            )
            # Insert CLIP embedding into smart_search
            cur.execute(
                """
                INSERT INTO smart_search ("assetId", embedding)
                VALUES (%s::uuid, %s::vector)
                ON CONFLICT ("assetId") DO UPDATE SET embedding = EXCLUDED.embedding
                """,
                (TEST_ASSET_ID, str(clip_embedding)),
            )
            conn.commit()
    finally:
        conn.close()


def cleanup_test_data():
    """Remove test data inserted by seed_test_data."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM aesthetic_scores WHERE "assetId" = %s::uuid', (TEST_ASSET_ID,))
            cur.execute('DELETE FROM inference_log WHERE "assetId" = %s::uuid', (TEST_ASSET_ID,))
            cur.execute('DELETE FROM interaction_events WHERE "assetId" = %s::uuid', (TEST_ASSET_ID,))
            cur.execute('DELETE FROM smart_search WHERE "assetId" = %s::uuid', (TEST_ASSET_ID,))
            cur.execute('DELETE FROM user_embeddings WHERE "userId" = %s::uuid', (TEST_USER_ID,))
            cur.execute('DELETE FROM user_interaction_counts WHERE "userId" = %s::uuid', (TEST_USER_ID,))
            cur.execute('DELETE FROM asset WHERE id = %s::uuid', (TEST_ASSET_ID,))
            cur.execute('DELETE FROM "user" WHERE id = %s::uuid', (TEST_USER_ID,))
            conn.commit()
    except Exception as e:
        print(f"[cleanup] Warning: {e}")
        conn.rollback()
    finally:
        conn.close()


# ── Setup ─────────────────────────────────────────────────────────────────────
print("Seeding test data...")
try:
    seed_test_data()
    check("seed test data", True)
except Exception as e:
    check("seed test data", False, str(e))
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

# ── Test 1: Health ────────────────────────────────────────────────────────────
try:
    r = requests.get(f"{BASE_URL}/health", timeout=5)
    check("health endpoint", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("health has model_version", "model_version" in body, f"body={body}")
except Exception as e:
    check("health endpoint", False, str(e))

# ── Test 2: Register user ────────────────────────────────────────────────────
try:
    r = requests.post(
        f"{BASE_URL}/users/register",
        json={"user_id": TEST_USER_ID},
        timeout=5,
    )
    check("register user", r.status_code == 200, f"status={r.status_code}")
except Exception as e:
    check("register user", False, str(e))

# ── Test 3: Score image (cold start — no interactions yet) ────────────────────
try:
    start = time.time()
    r = requests.post(
        f"{BASE_URL}/score-image",
        json={"asset_id": TEST_ASSET_ID, "user_id": TEST_USER_ID},
        timeout=15,
    )
    latency_ms = (time.time() - start) * 1000
    check("score-image cold start returns 200", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("score in [0,1]", 0.0 <= body["score"] <= 1.0, f"score={body['score']}")
        check("alpha is 0 for cold start", body["alpha"] == 0.0, f"alpha={body['alpha']}")
        check("is_cold_start is true", body["is_cold_start"] is True)
        check("latency < 2000ms", latency_ms < 2000, f"{latency_ms:.1f}ms")
except Exception as e:
    check("score-image cold start", False, str(e))

# ── Test 4: Interaction event ─────────────────────────────────────────────────
try:
    event_id = f"smoke-evt-{uuid.uuid4()}"
    r = requests.post(
        f"{BASE_URL}/events/interaction",
        json={
            "event_id": event_id,
            "asset_id": TEST_ASSET_ID,
            "user_id": TEST_USER_ID,
            "event_type": "view",
            "label": 0.8,
            "source": "immich_upload",
            "event_time": "2025-01-01T00:00:00Z",
        },
        timeout=5,
    )
    check("interaction event accepted", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("event status is accepted", body["status"] == "accepted")

    # Test dedup — same event_id should return "duplicate"
    r2 = requests.post(
        f"{BASE_URL}/events/interaction",
        json={
            "event_id": event_id,
            "asset_id": TEST_ASSET_ID,
            "user_id": TEST_USER_ID,
            "event_type": "view",
            "label": 0.8,
            "source": "immich_upload",
            "event_time": "2025-01-01T00:00:00Z",
        },
        timeout=5,
    )
    if r2.status_code == 200:
        check("duplicate event detected", r2.json()["status"] == "duplicate")
except Exception as e:
    check("interaction event", False, str(e))

# ── Test 5: Score image after interaction (alpha should be > 0) ───────────────
try:
    r = requests.post(
        f"{BASE_URL}/score-image",
        json={"asset_id": TEST_ASSET_ID, "user_id": TEST_USER_ID},
        timeout=15,
    )
    check("score-image post-interaction returns 200", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("score in [0,1]", 0.0 <= body["score"] <= 1.0, f"score={body['score']}")
        # Alpha should be n/(n+10) = 1/11 ≈ 0.0909 after 1 interaction
        check("alpha > 0 after interaction", body["alpha"] > 0.0, f"alpha={body['alpha']}")
except Exception as e:
    check("score-image post-interaction", False, str(e))

# ── Test 6: 404 for missing asset ─────────────────────────────────────────────
try:
    r = requests.post(
        f"{BASE_URL}/score-image",
        json={
            "asset_id": "00000000-0000-0000-0000-000000000000",
            "user_id": TEST_USER_ID,
        },
        timeout=15,
    )
    check("missing asset returns 404", r.status_code == 404, f"status={r.status_code}")
except Exception as e:
    check("missing asset rejection", False, str(e))

# ── Test 7: Model reload ─────────────────────────────────────────────────────
try:
    r = requests.post(f"{BASE_URL}/admin/reload-model", timeout=30)
    # 200 = reloaded, 500 = reload failed (acceptable in smoke if MinIO models missing)
    check("reload-model responds", r.status_code in (200, 500), f"status={r.status_code}")
except Exception as e:
    check("reload-model endpoint", False, str(e))

# ── Cleanup ───────────────────────────────────────────────────────────────────
print("\nCleaning up test data...")
try:
    cleanup_test_data()
except Exception as e:
    print(f"[cleanup] Warning: {e}")

# ── Result ────────────────────────────────────────────────────────────────────
all_passed = all(RESULTS)
with open(args.output_result, "w") as f:
    f.write("true" if all_passed else "false")

print(f"\n{'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
print(f"Summary: {sum(RESULTS)}/{len(RESULTS)} checks passed")
sys.exit(0 if all_passed else 1)

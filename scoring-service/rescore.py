#!/usr/bin/env python3
"""
Rescore active users' photos after a new model is promoted.
Calls feature-svc to get feature vectors, then calls scoring service.
Only targets users with interactions in past --active-days days.
"""
import os
import sys
import time
import argparse
import requests
import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument("--scoring-url",
                    default="http://aesthetic-scoring.aesthetic-hub.svc.cluster.local:8000")
parser.add_argument("--feature-svc-url",
                    default="http://feature-svc.aesthetic-hub.svc.cluster.local:8001")
parser.add_argument("--active-days", type=int, default=30)
args = parser.parse_args()

SCORING_URL = args.scoring_url.rstrip("/")
FEATURE_SVC_URL = args.feature_svc_url.rstrip("/")


def get_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"]
    )


print(f"Fetching active users (last {args.active_days} days)...")
conn = get_conn()
try:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT user_id::text
            FROM user_interaction_counts
            WHERE updated_at >= NOW() - INTERVAL '%s days'
            """,
            (args.active_days,)
        )
        active_users = [row[0] for row in cur.fetchall()]
finally:
    conn.close()

print(f"Found {len(active_users)} active users")

total_rescored = 0
total_errors = 0

for user_id in active_users:
    conn = get_conn()
    try:
        # Get all assets for this user
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id::text
                FROM assets a
                WHERE a."ownerId" = %s::uuid
                  AND a."deletedAt" IS NULL
                ORDER BY a."createdAt" DESC
                """,
                (user_id,)
            )
            asset_ids = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    if not asset_ids:
        continue

    print(f"Rescoring {len(asset_ids)} assets for user {user_id}...")

    for asset_id in asset_ids:
        try:
            # Step 1: Get feature vector from feature-svc
            feat_resp = requests.post(
                f"{FEATURE_SVC_URL}/features",
                json={
                    "asset_id": asset_id,
                    "user_id": user_id,
                    "source": "rescore"
                },
                timeout=10
            )
            if feat_resp.status_code != 200:
                total_errors += 1
                continue

            feature_vector = feat_resp.json()

            # Step 2: Call scoring service with feature vector
            score_resp = requests.post(
                f"{SCORING_URL}/score",
                json=feature_vector,
                timeout=10
            )
            if score_resp.status_code != 200:
                total_errors += 1
                continue

            total_rescored += 1

        except Exception as e:
            print(f"  Error on asset {asset_id} for user {user_id}: {e}")
            total_errors += 1

        time.sleep(0.05)  # avoid overwhelming services

print(f"\nRescore complete.")
print(f"  Total rescored: {total_rescored}")
print(f"  Total errors:   {total_errors}")

if total_errors > 0 and total_errors > total_rescored * 0.1:
    print("ERROR: More than 10% of rescore operations failed")
    sys.exit(1)

sys.exit(0)
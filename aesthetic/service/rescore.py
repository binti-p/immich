#!/usr/bin/env python3
"""
Rescore active users' photos after a new model is promoted.
Calls aesthetic-service /score-image which handles the full pipeline
(CLIP lookup → user embedding → alpha → ONNX → DB → Immich callback).
Only targets users with interactions in past --active-days days.
"""
import os
import sys
import time
import argparse
import requests
import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument("--aesthetic-service-url",
                    default="http://aesthetic-service.aesthetic-hub.svc.cluster.local:8000")
parser.add_argument("--active-days", type=int, default=30)
args = parser.parse_args()

SERVICE_URL = args.aesthetic_service_url.rstrip("/")


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
            SELECT DISTINCT "userId"::text
            FROM user_interaction_counts
            WHERE "updatedAt" >= NOW() - INTERVAL '%s days'
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
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text
                FROM asset
                WHERE "ownerId" = %s::uuid
                  AND "deletedAt" IS NULL
                ORDER BY "createdAt" DESC
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
            resp = requests.post(
                f"{SERVICE_URL}/score-image",
                json={
                    "asset_id": asset_id,
                    "user_id": user_id,
                },
                timeout=10
            )
            if resp.status_code != 200:
                print(f"  WARN: /score-image returned {resp.status_code} for asset {asset_id}")
                total_errors += 1
                continue

            total_rescored += 1

        except Exception as e:
            print(f"  Error on asset {asset_id} for user {user_id}: {e}")
            total_errors += 1

        time.sleep(0.05)  # avoid overwhelming the service

print(f"\nRescore complete.")
print(f"  Total rescored: {total_rescored}")
print(f"  Total errors:   {total_errors}")

if total_errors > 0 and total_errors > total_rescored * 0.1:
    print("ERROR: More than 10% of rescore operations failed")
    sys.exit(1)

sys.exit(0)

"""
Shared fixtures for aesthetic-service integration tests.
Requires the stack to be running (docker compose up).
"""
import os
import pytest
import psycopg2
import requests
import numpy as np

SERVICE_URL = os.environ.get("AESTHETIC_SERVICE_URL", "http://localhost:8002")
TEST_USER_ID = "00000000-0000-0000-0000-aaa000000099"
TEST_ASSET_IDS = [
    "00000000-0000-0000-0000-aaa000000001",
    "00000000-0000-0000-0000-aaa000000002",
    "00000000-0000-0000-0000-aaa000000003",
]


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


@pytest.fixture(scope="session", autouse=True)
def seed_and_cleanup():
    """Seed test data before all tests, clean up after."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Create test user
            cur.execute(
                """
                INSERT INTO "user" (id, email, name, "createdAt", "updatedAt", "isAdmin")
                VALUES (%s::uuid, 'test-runner@test.local', 'Test Runner', NOW(), NOW(), false)
                ON CONFLICT (id) DO NOTHING
                """,
                (TEST_USER_ID,),
            )
            # Create test assets + CLIP embeddings
            for i, asset_id in enumerate(TEST_ASSET_IDS):
                clip = np.random.randn(768).astype(np.float32).tolist()
                checksum = bytes([i + 1] * 20)
                cur.execute(
                    """
                    INSERT INTO asset (id, "ownerId", type, "originalPath",
                                       "fileCreatedAt", "fileModifiedAt", checksum,
                                       "originalFileName", "localDateTime", "checksumAlgorithm",
                                       "createdAt", "updatedAt")
                    VALUES (%s::uuid, %s::uuid, 'IMAGE', %s,
                            NOW(), NOW(), %s,
                            'img.jpg', NOW(), 'sha1',
                            NOW(), NOW())
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (asset_id, TEST_USER_ID, f'/test/img_{i}.jpg', checksum),
                )
                cur.execute(
                    """
                    INSERT INTO smart_search ("assetId", embedding)
                    VALUES (%s::uuid, %s::vector)
                    ON CONFLICT ("assetId") DO UPDATE SET embedding = EXCLUDED.embedding
                    """,
                    (asset_id, str(clip)),
                )
            conn.commit()
    finally:
        conn.close()

    # Register test user with aesthetic-service
    requests.post(
        f"{SERVICE_URL}/users/register",
        json={"user_id": TEST_USER_ID},
        timeout=5,
    )

    yield  # run all tests

    # Cleanup
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for asset_id in TEST_ASSET_IDS:
                cur.execute('DELETE FROM aesthetic_scores WHERE "assetId" = %s::uuid', (asset_id,))
                cur.execute('DELETE FROM inference_log WHERE "assetId" = %s::uuid', (asset_id,))
                cur.execute('DELETE FROM interaction_events WHERE "assetId" = %s::uuid', (asset_id,))
                cur.execute('DELETE FROM smart_search WHERE "assetId" = %s::uuid', (asset_id,))
            cur.execute('DELETE FROM user_embeddings WHERE "userId" = %s::uuid', (TEST_USER_ID,))
            cur.execute('DELETE FROM user_interaction_counts WHERE "userId" = %s::uuid', (TEST_USER_ID,))
            for asset_id in TEST_ASSET_IDS:
                cur.execute('DELETE FROM asset WHERE id = %s::uuid', (asset_id,))
            cur.execute('DELETE FROM "user" WHERE id = %s::uuid', (TEST_USER_ID,))
            conn.commit()
    except Exception as e:
        print(f"[cleanup] Warning: {e}")
        conn.rollback()
    finally:
        conn.close()

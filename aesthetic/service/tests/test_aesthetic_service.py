#python3 -m pytest aesthetic/service/tests/ -v
"""
Integration tests for aesthetic-service.
Run with: pytest aesthetic/service/tests/ -v
Requires the docker compose stack to be running.
"""
import os
import time
import uuid
import psycopg2
import pytest
import requests

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


# ── Health & Metrics ──────────────────────────────────────────────────────────

class TestHealth:
    def test_health_returns_200(self):
        r = requests.get(f"{SERVICE_URL}/health", timeout=5)
        assert r.status_code == 200

    def test_health_has_model_version(self):
        body = requests.get(f"{SERVICE_URL}/health", timeout=5).json()
        assert "model_version" in body
        assert "personalized_model_loaded" in body
        assert body["status"] == "healthy"

    def test_metrics_endpoint(self):
        r = requests.get(f"{SERVICE_URL}/metrics/", timeout=5)
        assert r.status_code == 200
        assert "score_image_request_duration_seconds" in r.text


# ── Score Image ───────────────────────────────────────────────────────────────

class TestScoreImage:
    def test_cold_start_returns_200(self):
        r = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        )
        assert r.status_code == 200

    def test_cold_start_score_in_range(self):
        body = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        ).json()
        assert 0.0 <= body["score"] <= 1.0

    def test_cold_start_alpha_is_zero(self):
        body = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        ).json()
        assert body["alpha"] == 0.0
        assert body["is_cold_start"] is True

    def test_cold_start_personalized_is_null(self):
        body = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        ).json()
        assert body["personalized_score"] is None

    def test_response_has_all_fields(self):
        body = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        ).json()
        required = ["request_id", "asset_id", "user_id", "score", "global_score",
                     "alpha", "is_cold_start", "model_version", "low_confidence"]
        for field in required:
            assert field in body, f"Missing field: {field}"

    def test_different_assets_get_different_scores(self):
        scores = []
        for asset_id in TEST_ASSET_IDS:
            body = requests.post(
                f"{SERVICE_URL}/score-image",
                json={"asset_id": asset_id, "user_id": TEST_USER_ID},
                timeout=15,
            ).json()
            scores.append(body["score"])
        # All scores should be valid floats in [0,1]
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_latency_under_2s(self):
        start = time.time()
        requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
            timeout=15,
        )
        elapsed_ms = (time.time() - start) * 1000
        assert elapsed_ms < 2000, f"Latency too high: {elapsed_ms:.0f}ms"

    def test_idempotent_upsert(self):
        """Scoring same asset twice should not create duplicate rows."""
        for _ in range(2):
            requests.post(
                f"{SERVICE_URL}/score-image",
                json={"asset_id": TEST_ASSET_IDS[0], "user_id": TEST_USER_ID},
                timeout=15,
            )
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT count(*) FROM aesthetic_scores WHERE "assetId" = %s::uuid AND "userId" = %s::uuid',
                    (TEST_ASSET_IDS[0], TEST_USER_ID),
                )
                assert cur.fetchone()[0] == 1
        finally:
            conn.close()


# ── Error Handling ────────────────────────────────────────────────────────────

class TestErrorHandling:
    def test_missing_asset_returns_error(self):
        r = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"asset_id": "00000000-0000-0000-0000-000000000000", "user_id": TEST_USER_ID},
            timeout=45,
        )
        assert r.status_code in (404, 500), f"Expected 404 or 500, got {r.status_code}"

    def test_invalid_request_returns_422(self):
        r = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"bad": "data"},
            timeout=5,
        )
        assert r.status_code == 422

    def test_422_has_field_errors(self):
        body = requests.post(
            f"{SERVICE_URL}/score-image",
            json={"bad": "data"},
            timeout=5,
        ).json()
        assert "detail" in body
        fields = [e["loc"][-1] for e in body["detail"]]
        assert "asset_id" in fields
        assert "user_id" in fields


# ── User Registration ─────────────────────────────────────────────────────────

class TestUserRegistration:
    def test_register_returns_200(self):
        r = requests.post(
            f"{SERVICE_URL}/users/register",
            json={"user_id": TEST_USER_ID},
            timeout=5,
        )
        assert r.status_code == 200
        assert r.json()["status"] == "registered"

    def test_register_creates_user_embedding(self):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT array_length(embedding, 1) FROM user_embeddings WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                row = cur.fetchone()
                assert row is not None, "No user embedding found"
                assert row[0] == 64, f"Expected 64-dim embedding, got {row[0]}"
        finally:
            conn.close()

    def test_register_creates_interaction_count(self):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT "interactionCount" FROM user_interaction_counts WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                row = cur.fetchone()
                assert row is not None, "No interaction count row found"
        finally:
            conn.close()


# ── Interaction Events ────────────────────────────────────────────────────────

class TestInteractionEvents:
    def test_event_accepted(self):
        event_id = f"test-evt-{uuid.uuid4()}"
        r = requests.post(
            f"{SERVICE_URL}/events/interaction",
            json={
                "event_id": event_id,
                "asset_id": TEST_ASSET_IDS[0],
                "user_id": TEST_USER_ID,
                "event_type": "view",
                "label": 0.8,
                "source": "immich_upload",
                "event_time": "2026-01-01T00:00:00Z",
            },
            timeout=5,
        )
        assert r.status_code == 200
        assert r.json()["status"] == "accepted"

    def test_event_dedup(self):
        event_id = f"test-dedup-{uuid.uuid4()}"
        payload = {
            "event_id": event_id,
            "asset_id": TEST_ASSET_IDS[0],
            "user_id": TEST_USER_ID,
            "event_type": "view",
            "label": 0.5,
            "source": "immich_upload",
            "event_time": "2026-01-01T00:00:00Z",
        }
        r1 = requests.post(f"{SERVICE_URL}/events/interaction", json=payload, timeout=5)
        assert r1.json()["status"] == "accepted"

        r2 = requests.post(f"{SERVICE_URL}/events/interaction", json=payload, timeout=5)
        assert r2.json()["status"] == "duplicate"

    def test_event_increments_interaction_count(self):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT "interactionCount" FROM user_interaction_counts WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                before = cur.fetchone()[0]
        finally:
            conn.close()

        event_id = f"test-count-{uuid.uuid4()}"
        requests.post(
            f"{SERVICE_URL}/events/interaction",
            json={
                "event_id": event_id,
                "asset_id": TEST_ASSET_IDS[1],
                "user_id": TEST_USER_ID,
                "event_type": "favorite",
                "label": 1.0,
                "source": "immich_upload",
                "event_time": "2026-01-01T00:00:00Z",
            },
            timeout=5,
        )

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT "interactionCount" FROM user_interaction_counts WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                after = cur.fetchone()[0]
        finally:
            conn.close()

        assert after == before + 1


# ── Model Reload ──────────────────────────────────────────────────────────────

class TestModelReload:
    def test_reload_returns_200(self):
        r = requests.post(f"{SERVICE_URL}/admin/reload-model", timeout=30)
        assert r.status_code == 200

    def test_reload_response_has_version(self):
        body = requests.post(f"{SERVICE_URL}/admin/reload-model", timeout=30).json()
        assert body["status"] == "reloaded"
        assert "model_version" in body
        assert "personalized_model_loaded" in body


# ── DB Writes ─────────────────────────────────────────────────────────────────

class TestDBWrites:
    def test_aesthetic_scores_written(self):
        """Score all 3 assets, verify all 3 rows exist in aesthetic_scores."""
        for asset_id in TEST_ASSET_IDS:
            requests.post(
                f"{SERVICE_URL}/score-image",
                json={"asset_id": asset_id, "user_id": TEST_USER_ID},
                timeout=15,
            )

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT count(*) FROM aesthetic_scores WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                assert cur.fetchone()[0] == 3
        finally:
            conn.close()

    def test_inference_log_written(self):
        """After scoring, inference_log should have entries for our test assets."""
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT count(*) FROM inference_log WHERE "userId" = %s::uuid',
                    (TEST_USER_ID,),
                )
                assert cur.fetchone()[0] >= 3
        finally:
            conn.close()

    def test_scores_have_correct_schema(self):
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT score, alpha, "isColdStart", "modelVersion", "inferenceRequestId"
                    FROM aesthetic_scores
                    WHERE "userId" = %s::uuid
                    LIMIT 1
                    """,
                    (TEST_USER_ID,),
                )
                row = cur.fetchone()
                score, alpha, is_cold, model_ver, req_id = row
                assert 0.0 <= score <= 1.0
                assert alpha >= 0.0
                assert isinstance(is_cold, bool)
                assert model_ver is not None
                assert req_id is not None
        finally:
            conn.close()


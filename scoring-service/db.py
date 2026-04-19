import os
import requests
import psycopg2


def get_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"]
    )


def notify_immich(asset_id: str, user_id: str, score: float, model_version: str) -> None:
    """
    POST the score back to Immich so it can update asset.aestheticScore.
    Non-fatal if it fails.
    """
    immich_url = os.environ.get("IMMICH_SERVER_URL")
    if not immich_url:
        return

    try:
        resp = requests.post(
            f"{immich_url}/api/aesthetic/score-callback",
            json={
                "asset_id": asset_id,
                "user_id": user_id,
                "score": score,
                "model_version": model_version,
            },
            timeout=5,
        )
        if not resp.ok:
            print(f"[db] WARNING: Immich score-callback returned {resp.status_code} for asset {asset_id}")
    except Exception as e:
        print(f"[db] WARNING: Failed to notify Immich for asset {asset_id}: {e}")


def upsert_aesthetic_score(
    asset_id: str,
    user_id: str,
    score: float,
    alpha: float,
    model_version: str,
    is_cold_start: bool,
    request_id: str,
    source: str
):
    """
    Upsert into aesthetic_scores on (asset_id, user_id).
    Non-fatal if it fails — scoring still returns result to caller.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aesthetic_scores (
                    asset_id, user_id, score, model_version,
                    is_cold_start, alpha, inference_request_id,
                    source, scored_at
                ) VALUES (
                    %s::uuid, %s::uuid, %s, %s,
                    %s, %s, %s,
                    %s, NOW()
                )
                ON CONFLICT (asset_id, user_id) DO UPDATE SET
                    score                = EXCLUDED.score,
                    model_version        = EXCLUDED.model_version,
                    is_cold_start        = EXCLUDED.is_cold_start,
                    alpha                = EXCLUDED.alpha,
                    inference_request_id = EXCLUDED.inference_request_id,
                    source               = EXCLUDED.source,
                    scored_at            = NOW()
                """,
                (
                    asset_id, user_id, score, model_version,
                    is_cold_start, alpha, request_id,
                    source
                )
            )
            conn.commit()
    except Exception as e:
        print(f"[db] WARNING: upsert_aesthetic_score failed: {e}")
        conn.rollback()
    finally:
        conn.close()

    # Notify Immich to update asset.aestheticScore (non-blocking, non-fatal)
    notify_immich(asset_id, user_id, score, model_version)

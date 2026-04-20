"""
All database access for aesthetic-service.
Uses asyncpg for async FastAPI compatibility.
Absorbed and extended from scoring-service/db.py.
"""
import logging
import os
from typing import Optional

import asyncpg
import httpx

logger = logging.getLogger(__name__)

# Module-level pool — initialised in lifespan
_pool: Optional[asyncpg.Pool] = None


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(
        host=os.environ.get("POSTGRES_HOST", "immich_postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        min_size=2,
        max_size=10,
    )
    logger.info("[db] asyncpg pool created")


async def close_pool():
    if _pool:
        await _pool.close()


# ── Reads ─────────────────────────────────────────────────────────────────────

async def get_clip_embedding(asset_id: str) -> Optional[list]:
    """Read CLIP embedding from smart_search. Returns None if not found."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT embedding FROM smart_search WHERE "assetId" = $1::uuid',
            asset_id,
        )
    if row is None:
        return None
    emb = row["embedding"]
    # pgvector returns a string like '[0.1,0.2,...]' or a list depending on driver
    if isinstance(emb, str):
        import json
        emb = json.loads(emb.replace("(", "[").replace(")", "]"))
    return list(emb)


async def get_user_embedding(user_id: str) -> Optional[list]:
    """Read user embedding from user_embeddings. Returns None if not found (cold start)."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT embedding FROM user_embeddings WHERE "userId" = $1::uuid',
            user_id,
        )
    if row is None:
        return None
    emb = row["embedding"]
    if isinstance(emb, str):
        import json
        emb = json.loads(emb.replace("(", "[").replace(")", "]"))
    return list(emb)


async def get_interaction_count(user_id: str) -> int:
    """Read interaction count for alpha computation."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT "interactionCount" FROM user_interaction_counts WHERE "userId" = $1::uuid',
            user_id,
        )
    return int(row["interactionCount"]) if row else 0


async def event_exists(event_id: str) -> bool:
    """Dedup check — returns True if event_id already in interaction_events."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT 1 FROM interaction_events WHERE "eventId" = $1',
            event_id,
        )
    return row is not None


# ── Writes ────────────────────────────────────────────────────────────────────

async def upsert_user(user_id: str):
    """
    Register a new user:
    - Upserts user_interaction_counts (count=0)
    - Inserts a zero-vector into user_embeddings so personalized model runs immediately
      with alpha=0 (global dominates until interactions accumulate)
    """
    zero_vector = [0.0] * 64  # 64-dim user embedding, all zeros

    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO user_interaction_counts ("userId", "interactionCount", "updatedAt")
                VALUES ($1::uuid, 0, NOW())
                ON CONFLICT ("userId") DO NOTHING
                """,
                user_id,
            )
            await conn.execute(
                """
                INSERT INTO user_embeddings ("userId", embedding, "modelVersion", "updatedAt")
                VALUES ($1::uuid, $2::double precision[], NULL, NOW())
                ON CONFLICT ("userId") DO NOTHING
                """,
                user_id, zero_vector,
            )


async def insert_interaction_event(
    event_id: str,
    asset_id: str,
    user_id: str,
    event_type: str,
    label: float,
    source: str,
    event_time: str,
):
    from datetime import datetime, timezone
    # asyncpg requires datetime objects, not ISO strings
    if isinstance(event_time, str):
        event_time_dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    else:
        event_time_dt = event_time

    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO interaction_events
                    ("eventId", "assetId", "userId", "eventType", label, source, "eventTime", "ingestedAt")
                VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, $7, NOW())
                """,
                event_id, asset_id, user_id, event_type, label, source, event_time_dt,
            )
            await conn.execute(
                """
                INSERT INTO user_interaction_counts ("userId", "interactionCount", "updatedAt")
                VALUES ($1::uuid, 1, NOW())
                ON CONFLICT ("userId") DO UPDATE SET
                    "interactionCount" = user_interaction_counts."interactionCount" + 1,
                    "updatedAt" = NOW()
                """,
                user_id,
            )


async def insert_inference_log(
    request_id: str,
    asset_id: str,
    user_id: str,
    model_version: Optional[str],
    is_cold_start: bool,
    alpha: float,
):
    async with _pool.acquire() as conn:
        # Ensure model_version exists in model_versions table (FK constraint)
        if model_version:
            await conn.execute(
                """
                INSERT INTO model_versions
                    ("versionId", "datasetVersion", "mlpObjectKey", "embeddingsObjectKey",
                     "isColdStart", "activatedAt", "createdAt")
                VALUES ($1, $1, '', '', false, NOW(), NOW())
                ON CONFLICT ("versionId") DO NOTHING
                """,
                model_version,
            )
        await conn.execute(
            """
            INSERT INTO inference_log
                ("requestId", "assetId", "userId", "modelVersion",
                 "isColdStart", alpha, "requestReceivedAt", "computedAt")
            VALUES ($1, $2::uuid, $3::uuid, $4, $5, $6, NOW(), NOW())
            """,
            request_id, asset_id, user_id, model_version, is_cold_start, alpha,
        )


async def upsert_aesthetic_score(
    asset_id: str,
    user_id: str,
    score: float,
    alpha: float,
    model_version: Optional[str],
    is_cold_start: bool,
    request_id: str,
):
    """Absorbed from scoring-service/db.py upsert_aesthetic_score."""
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO aesthetic_scores
                ("assetId", "userId", score, "modelVersion",
                 "isColdStart", alpha, "inferenceRequestId", "scoredAt")
            VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT ("assetId", "userId") DO UPDATE SET
                score                  = EXCLUDED.score,
                "modelVersion"         = EXCLUDED."modelVersion",
                "isColdStart"          = EXCLUDED."isColdStart",
                alpha                  = EXCLUDED.alpha,
                "inferenceRequestId"   = EXCLUDED."inferenceRequestId",
                "scoredAt"             = NOW()
            """,
            asset_id, user_id, score, model_version, is_cold_start, alpha, request_id,
        )


# ── Immich callback ───────────────────────────────────────────────────────────

async def notify_immich(asset_id: str, user_id: str, score: float, model_version: Optional[str]):
    """Absorbed from scoring-service/db.py notify_immich — now async with httpx."""
    immich_url = os.environ.get("IMMICH_SERVER_URL")
    if not immich_url:
        return
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{immich_url}/api/aesthetic/score-callback",
                json={
                    "asset_id": asset_id,
                    "user_id": user_id,
                    "score": score,
                    "model_version": model_version,
                },
            )
            if not resp.is_success:
                logger.warning(
                    f"[db] Immich score-callback returned {resp.status_code} for asset {asset_id}"
                )
    except Exception as e:
        logger.warning(f"[db] Failed to notify Immich for asset {asset_id}: {e}")

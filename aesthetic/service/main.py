"""
aesthetic-service — orchestrates the full scoring pipeline.

Flow for POST /score-image:
  1. Read CLIP from smart_search
  2. Read user_embeddings + interaction_count
  3. Compute alpha = n/(n+10), is_cold_start
  4. Run ONNX scorer (global + personalized blend)
  5. Write inference_log
  6. Upsert aesthetic_scores
  7. Notify Immich score-callback
  8. Buffer to MinIO inference-log parquet
  9. Return score to NestJS
"""
import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, make_asgi_app

import db
import minio_client
from model_loader import download_models
from models import (
    InteractionEventRequest,
    InteractionEventResponse,
    RegisterUserRequest,
    RegisterUserResponse,
    ScoreImageRequest,
    ScoreImageResponse,
)
from scorer import Scorer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Prometheus ────────────────────────────────────────────────────────────────
INTERACTION_EVENTS_TOTAL = Counter(
    "interaction_events_total", "Interaction events received", ["event_type"]
)
SCORE_IMAGE_DURATION = Histogram(
    "score_image_request_duration_seconds",
    "End-to-end score-image latency",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)
COLD_START_TOTAL = Counter("cold_start_total", "Cold-start scoring events")
LOW_CONFIDENCE_TOTAL = Counter("low_confidence_total", "Low-confidence scoring events")  # E3.2

# E3.1 — Score distribution histogram
AESTHETIC_SCORE_HISTOGRAM = Histogram(
    "aesthetic_score",
    "Distribution of aesthetic scores",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# E3.4 — Active model version as unix timestamp gauge
from prometheus_client import Gauge
ACTIVE_MODEL_VERSION_TIMESTAMP = Gauge(
    "aesthetic_active_model_version_timestamp",
    "Unix timestamp of active model version (0 if bootstrap/unknown)",
)

MINIO_FLUSH_TOTAL = minio_client.MINIO_FLUSH_TOTAL
MINIO_FLUSH_ERRORS = minio_client.MINIO_FLUSH_ERRORS

# ── Global scorer instance ────────────────────────────────────────────────────
scorer: Scorer = None
active_model_version: str = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scorer, active_model_version

    # DB pool
    await db.init_pool()

    # Download models from MinIO, load into ONNX Runtime
    global_path, pers_path, version = download_models()
    scorer = Scorer(global_path, pers_path)
    scorer.model_version = version
    active_model_version = version
    
    # Insert model version into database
    await db.upsert_model_version(
        version_id=version,
        mlp_object_key=f"models/{version}/personalized_mlp.onnx",
        embeddings_object_key=f"models/{version}/user_embeddings.parquet",
    )
    
    logger.info(
        f"[startup] Scorer ready — model_version={version}, "
        f"personalized={'yes' if pers_path else 'no (cold-start only)'}"
    )

    # E3.4 — set model version gauge
    _set_model_version_gauge(version)

    yield

    # Shutdown — flush remaining buffers
    await minio_client.flush_all()
    await db.close_pool()


app = FastAPI(title="Aesthetic Service", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


# ── Endpoints ─────────────────────────────────────────────────────────────────

def _set_model_version_gauge(version: str | None):
    """E3.4 — encode version date as unix timestamp, or 0 for bootstrap."""
    if version and version not in ("v0000-00-00", "0000-00-00"):
        try:
            v = version.lstrip("v")
            from datetime import datetime, timezone
            ts = datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
            ACTIVE_MODEL_VERSION_TIMESTAMP.set(ts)
            return
        except Exception:
            pass
    ACTIVE_MODEL_VERSION_TIMESTAMP.set(0)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "model_version": active_model_version,
        "personalized_model_loaded": scorer.personalized_sess is not None if scorer else False,
    }


@app.post("/admin/reload-model")
async def reload_model():
    """
    Re-downloads models from MinIO and reinitializes the ONNX scorer in-place.
    Called by promote.py after a new model is written to MinIO.
    Rescore-all should only be triggered AFTER this returns 200.
    """
    global scorer, active_model_version
    import asyncio

    loop = asyncio.get_event_loop()

    try:
        # Run blocking download in thread pool so we don't block the event loop
        global_path, pers_path, version = await loop.run_in_executor(None, download_models)
        new_scorer = Scorer(global_path, pers_path)
        new_scorer.model_version = version

        # Atomic swap — in-flight requests finish with old scorer, new requests get new one
        scorer = new_scorer
        active_model_version = version

        _set_model_version_gauge(version)  # E3.4

        logger.info(
            f"[reload] Model reloaded — version={version}, "
            f"personalized={'yes' if pers_path else 'no'}"
        )
        return {
            "status": "reloaded",
            "model_version": version,
            "personalized_model_loaded": pers_path is not None,
        }
    except Exception as e:
        logger.error(f"[reload] Model reload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Model reload failed: {e}")


@app.post("/users/register", response_model=RegisterUserResponse)
async def register_user(req: RegisterUserRequest):
    await db.upsert_user(req.user_id)
    return RegisterUserResponse(status="registered", user_id=req.user_id)


@app.post("/events/interaction", response_model=InteractionEventResponse)
async def interaction_event(req: InteractionEventRequest):
    # Dedup
    if await db.event_exists(req.event_id):
        return InteractionEventResponse(status="duplicate", event_id=req.event_id)

    await db.insert_interaction_event(
        event_id=req.event_id,
        asset_id=req.asset_id,
        user_id=req.user_id,
        event_type=req.event_type,
        label=req.label,
        source=req.source,
        event_time=req.event_time,
    )

    INTERACTION_EVENTS_TOTAL.labels(event_type=req.event_type).inc()

    # Buffer for MinIO
    await minio_client.buffer_interaction({
        "event_id": req.event_id,
        "asset_id": req.asset_id,
        "user_id": req.user_id,
        "event_type": req.event_type,
        "label": req.label,
        "source": req.source,
        "event_time": req.event_time,
    })

    return InteractionEventResponse(status="accepted", event_id=req.event_id)


@app.post("/score-image", response_model=ScoreImageResponse)
async def score_image(req: ScoreImageRequest):
    import time
    start = time.time()

    # 1. Read CLIP embedding — retry up to 3 times with backoff (ML indexing is async)
    clip_list = None
    for attempt in range(3):
        clip_list = await db.get_clip_embedding(req.asset_id)
        if clip_list is not None:
            break
        if attempt < 2:
            await asyncio.sleep(5 * (attempt + 1))  # 5s, 10s

    if clip_list is None:
        raise HTTPException(status_code=404, detail="No CLIP embedding found for asset")

    clip_emb = np.array(clip_list, dtype=np.float32)
    if len(clip_emb) != 768:
        raise HTTPException(status_code=422, detail=f"Expected 768-dim CLIP, got {len(clip_emb)}")

    # 2. Read user embedding + interaction count
    user_list = await db.get_user_embedding(req.user_id)
    n = await db.get_interaction_count(req.user_id)

    user_emb = np.array(user_list, dtype=np.float32) if user_list else None
    # is_cold_start = user has no trained embedding (zero vector or missing)
    is_cold_start = user_emb is None or float(np.linalg.norm(user_emb)) < 1e-6

    # 3. Compute alpha
    alpha = n / (n + 10) if n > 0 else 0.0

    # 4. Run ONNX scorer
    final_score, g_score, p_score, effective_alpha, low_confidence = scorer.score(
        clip_emb, user_emb, alpha, is_cold_start
    )

    if is_cold_start:
        COLD_START_TOTAL.inc()
    if low_confidence:
        LOW_CONFIDENCE_TOTAL.inc()  # E3.2

    request_id = str(uuid.uuid4())

    # 5. Write inference_log
    await db.insert_inference_log(
        request_id=request_id,
        asset_id=req.asset_id,
        user_id=req.user_id,
        model_version=active_model_version,
        is_cold_start=is_cold_start,
        alpha=effective_alpha,
    )

    # 6. Upsert aesthetic_scores
    await db.upsert_aesthetic_score(
        asset_id=req.asset_id,
        user_id=req.user_id,
        score=round(final_score, 4),
        alpha=effective_alpha,
        model_version=active_model_version,
        is_cold_start=is_cold_start,
        request_id=request_id,
    )

    # 7. Notify Immich (fire-and-forget, non-fatal)
    await db.notify_immich(req.asset_id, req.user_id, round(final_score, 4), active_model_version)

    # 8. Buffer to MinIO
    await minio_client.buffer_inference({
        "request_id": request_id,
        "asset_id": req.asset_id,
        "user_id": req.user_id,
        "model_version": active_model_version,
        "is_cold_start": is_cold_start,
        "alpha": effective_alpha,
        "final_score": round(final_score, 4),
        "global_score": round(g_score, 4),
        "personalized_score": round(p_score, 4) if p_score is not None else None,
        "low_confidence": low_confidence,
    })

    SCORE_IMAGE_DURATION.observe(time.time() - start)
    AESTHETIC_SCORE_HISTOGRAM.observe(round(final_score, 4))  # E3.1

    return ScoreImageResponse(
        request_id=request_id,
        asset_id=req.asset_id,
        user_id=req.user_id,
        score=round(final_score, 4),
        global_score=round(g_score, 4),
        personalized_score=round(p_score, 4) if p_score is not None else None,
        alpha=effective_alpha,
        is_cold_start=is_cold_start,
        model_version=active_model_version,
        low_confidence=low_confidence,
    )

import os
import time
import numpy as np
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from prometheus_client import Histogram, Counter, make_asgi_app

from models import ScoreRequest, ScoreBatchRequest, ScoreResponse
from triton_client import infer_global, infer_personalized
import db

ENV = os.environ.get("ENVIRONMENT", "production")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "unknown")

# --- Prometheus metrics ---
LATENCY = Histogram(
    "scoring_latency_seconds", "End-to-end scoring latency",
    ["environment"],
    buckets=[.005, .01, .025, .05, .1, .25, .5]
)
ALPHA_HIST = Histogram(
    "scoring_alpha", "Alpha blending value",
    buckets=[0, .1, .2, .3, .5, .7, .9, 1.0]
)
SCORE_HIST = Histogram(
    "scoring_final_score", "Final score distribution",
    buckets=[i/10 for i in range(11)]
)
ERRORS = Counter(
    "scoring_errors_total", "Scoring errors",
    ["reason"]
)
LOW_CONF = Counter(
    "scoring_low_confidence_total", "Low-confidence score flags"
)

LOW_CONF_THRESHOLD = 0.15
DIVERGENCE_THRESHOLD = 0.40


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[startup] Scoring service starting — env={ENV}, model_version={MODEL_VERSION}")
    yield


app = FastAPI(title="Aesthetic Scoring Service", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


def _score(request: ScoreRequest) -> ScoreResponse:
    clip_emb = np.array(request.clip_embedding, dtype=np.float32)

    if len(clip_emb) != 768:
        ERRORS.labels(reason="bad_clip_embedding").inc()
        raise ValueError(f"Expected 768-dim clip_embedding, got {len(clip_emb)}")

    # --- Global model (always called) ---
    try:
        g_score = infer_global(clip_emb)
    except Exception as e:
        ERRORS.labels(reason="triton_global_failed").inc()
        raise HTTPException(status_code=503, detail="Global model unavailable")

    # --- Personalized model ---
    # Only called if: not cold start AND user_embedding is present AND alpha > 0
    p_score = None
    if (
        not request.is_cold_start
        and request.user_embedding is not None
        and request.alpha > 0
    ):
        user_emb = np.array(request.user_embedding, dtype=np.float32)
        if len(user_emb) != 64:
            ERRORS.labels(reason="bad_user_embedding").inc()
        else:
            try:
                p_score = infer_personalized(clip_emb, user_emb)
            except Exception as e:
                # Graceful degradation — fall back to global only
                ERRORS.labels(reason="triton_personalized_failed").inc()

    # --- Blend using pre-computed alpha from feature-svc ---
    alpha = request.alpha
    if p_score is not None:
        final_score = (1 - alpha) * g_score + alpha * p_score
        divergence = abs(g_score - p_score)
    else:
        final_score = g_score
        alpha = 0.0
        divergence = 0.0

    low_confidence = (
        final_score < LOW_CONF_THRESHOLD or
        divergence > DIVERGENCE_THRESHOLD
    )

    # --- Write score to Postgres ---
    db.upsert_aesthetic_score(
        asset_id=request.asset_id,
        user_id=request.user_id,
        score=final_score,
        alpha=alpha,
        model_version=request.model_version or MODEL_VERSION,
        is_cold_start=request.is_cold_start,
        request_id=request.request_id,
        source=request.source or "scoring-service"
    )

    ALPHA_HIST.observe(alpha)
    SCORE_HIST.observe(final_score)
    if low_confidence:
        LOW_CONF.inc()

    return ScoreResponse(
        asset_id=request.asset_id,
        user_id=request.user_id,
        score=round(final_score, 4),
        global_score=round(g_score, 4),
        personalized_score=round(p_score, 4) if p_score is not None else None,
        alpha=round(alpha, 4),
        is_cold_start=request.is_cold_start,
        model_version=request.model_version or MODEL_VERSION,
        low_confidence=low_confidence
    )


@app.post("/score", response_model=ScoreResponse)
def score_single(request: ScoreRequest):
    start = time.time()
    try:
        result = _score(request)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    finally:
        LATENCY.labels(environment=ENV).observe(time.time() - start)
    return result


@app.post("/score/batch", response_model=list[ScoreResponse])
def score_batch(request: ScoreBatchRequest):
    results = []
    for item in request.items:
        start = time.time()
        try:
            results.append(_score(item))
        except HTTPException:
            results.append(ScoreResponse(
                asset_id=item.asset_id,
                user_id=item.user_id,
                score=0.5,
                global_score=0.5,
                alpha=0.0,
                is_cold_start=True,
                model_version=None,
                low_confidence=True
            ))
        finally:
            LATENCY.labels(environment=ENV).observe(time.time() - start)
    return results


@app.get("/health")
def health():
    return {
        "status": "ok",
        "environment": ENV,
        "model_version": MODEL_VERSION
    }
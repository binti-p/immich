from pydantic import BaseModel
from typing import Optional


# ── Inbound from Immich NestJS ────────────────────────────────────────────────

class RegisterUserRequest(BaseModel):
    user_id: str


class InteractionEventRequest(BaseModel):
    event_id: str
    asset_id: str
    user_id: str
    event_type: str
    label: float
    source: str = "immich_upload"
    event_time: str  # ISO-8601


class ScoreImageRequest(BaseModel):
    asset_id: str
    user_id: str


# ── Outbound ──────────────────────────────────────────────────────────────────

class RegisterUserResponse(BaseModel):
    status: str
    user_id: str


class InteractionEventResponse(BaseModel):
    status: str   # "accepted" | "duplicate"
    event_id: str


class ScoreImageResponse(BaseModel):
    request_id: str
    asset_id: str
    user_id: str
    score: float
    global_score: float
    personalized_score: Optional[float] = None
    alpha: float
    is_cold_start: bool
    model_version: Optional[str] = None
    low_confidence: bool

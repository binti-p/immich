"""
Downloads ONNX models from MinIO at startup.
Caches to /tmp so the container doesn't re-download on every request.
"""
import json
import logging
import os
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")
GLOBAL_MODEL_KEY = "models/global/personalized_mlp.onnx"
GLOBAL_LOCAL_PATH = "/tmp/global_mlp.onnx"
PERS_LOCAL_PATH = "/tmp/personalized_mlp.onnx"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def _latest_model_version(s3) -> Optional[str]:
    """Return the latest v{date} prefix that has a model_card.json, or None."""
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="models/v", Delimiter="/")
        prefixes = [
            cp["Prefix"].rstrip("/").split("/")[-1]
            for cp in resp.get("CommonPrefixes", [])
        ]
        versions = sorted([p for p in prefixes if p.startswith("v")], reverse=True)
        for v in versions:
            key = f"models/{v}/model_card.json"
            try:
                s3.head_object(Bucket=BUCKET, Key=key)
                return v
            except ClientError:
                continue
    except Exception as e:
        logger.warning(f"[model_loader] Could not list model versions: {e}")
    return None


def download_models() -> Tuple[str, Optional[str], Optional[str]]:
    """
    Downloads models from MinIO.
    Returns (global_path, personalized_path_or_None, version_id_or_None).
    Always succeeds for global (raises if missing). Personalized is best-effort.
    """
    s3 = _s3_client()

    # --- Global (cold-start) model — mandatory ---
    logger.info(f"[model_loader] Downloading global model: {GLOBAL_MODEL_KEY}")
    s3.download_file(BUCKET, GLOBAL_MODEL_KEY, GLOBAL_LOCAL_PATH)
    logger.info(f"[model_loader] Global model saved to {GLOBAL_LOCAL_PATH}")

    # --- Latest versioned personalized model — optional ---
    version = _latest_model_version(s3)
    if version is None:
        logger.info("[model_loader] No versioned model found — cold-start only mode")
        return GLOBAL_LOCAL_PATH, None, None

    pers_key = f"models/{version}/personalized_mlp.onnx"
    try:
        logger.info(f"[model_loader] Downloading personalized model: {pers_key}")
        s3.download_file(BUCKET, pers_key, PERS_LOCAL_PATH)
        logger.info(f"[model_loader] Personalized model saved to {PERS_LOCAL_PATH}")
        return GLOBAL_LOCAL_PATH, PERS_LOCAL_PATH, version
    except Exception as e:
        logger.warning(f"[model_loader] Could not download personalized model: {e} — cold-start only")
        return GLOBAL_LOCAL_PATH, None, None


def read_model_card(version: str) -> Optional[dict]:
    """Read model_card.json for a given version from MinIO."""
    s3 = _s3_client()
    key = f"models/{version}/model_card.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning(f"[model_loader] Could not read model card {key}: {e}")
        return None

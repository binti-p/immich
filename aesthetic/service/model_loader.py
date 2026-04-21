"""
Downloads ONNX models from MinIO at startup.
Caches to /tmp so the container doesn't re-download on every request.

New bucket structure:
  triton-models bucket:
    {environment}/global_mlp/1/model.onnx
    {environment}/personalized_mlp/1/model.onnx
  
  aesthetic-hub-data bucket:
    models/{version}/best_personalized_model.pth

Environment is controlled by MODEL_STAGE env var (default: production).
"""
import json
import logging
import os
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

BUCKET_DATA = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")
BUCKET_TRITON = os.environ.get("MINIO_TRITON_BUCKET", "triton-models")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "production")
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
    """
    Return the latest v{date} prefix from models/ that has a best_personalized_model.pth.
    Returns version string like "v2024-01-15" or None if no models found.
    """
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET_DATA, Prefix="models/v", Delimiter="/")
        prefixes = [
            cp["Prefix"].rstrip("/").split("/")[-1]
            for cp in resp.get("CommonPrefixes", [])
        ]
        versions = sorted([p for p in prefixes if p.startswith("v")], reverse=True)
        for v in versions:
            key = f"models/{v}/best_personalized_model.pth"
            try:
                s3.head_object(Bucket=BUCKET_DATA, Key=key)
                return v
            except ClientError:
                continue
    except Exception as e:
        logger.warning(f"[model_loader] Could not list model versions: {e}")
    return None


def download_models() -> Tuple[str, Optional[str], Optional[str]]:
    """
    Downloads models from MinIO triton-models bucket.
    Returns (global_path, personalized_path_or_None, version_id_or_None).
    Always succeeds for global (raises if missing). Personalized is best-effort.
    """
    s3 = _s3_client()
    
    # Resolve keys from triton-models bucket
    global_key = f"{MODEL_STAGE}/global_mlp/1/model.onnx"
    pers_key = f"{MODEL_STAGE}/personalized_mlp/1/model.onnx"

    # --- Global (cold-start) model — mandatory ---
    logger.info(f"[model_loader] Downloading global model: s3://{BUCKET_TRITON}/{global_key}")
    s3.download_file(BUCKET_TRITON, global_key, GLOBAL_LOCAL_PATH)
    logger.info(f"[model_loader] Global model saved to {GLOBAL_LOCAL_PATH}")

    # --- Personalized model ---
    try:
        logger.info(f"[model_loader] Downloading personalized model: s3://{BUCKET_TRITON}/{pers_key}")
        s3.download_file(BUCKET_TRITON, pers_key, PERS_LOCAL_PATH)
        logger.info(f"[model_loader] Personalized model saved to {PERS_LOCAL_PATH}")
        
        # Get version from models/ directory (latest PTH version)
        version = _latest_model_version(s3)
        if version is None:
            logger.warning("[model_loader] No versioned PTH models found, using bootstrap")
            version = "v0000-00-00"
        
        return GLOBAL_LOCAL_PATH, PERS_LOCAL_PATH, version
    except Exception as e:
        logger.warning(f"[model_loader] Could not download personalized model: {e} — cold-start only")
        return GLOBAL_LOCAL_PATH, None, "v0000-00-00"


def list_model_versions(s3=None) -> list[str]:
    """
    List all available model versions from models/ directory in aesthetic-hub-data bucket.
    Returns list of version strings like ["v2024-01-15", "v2024-01-14", ...].
    """
    if s3 is None:
        s3 = _s3_client()
    
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET_DATA, Prefix="models/v", Delimiter="/")
        prefixes = [
            cp["Prefix"].rstrip("/").split("/")[-1]
            for cp in resp.get("CommonPrefixes", [])
        ]
        return sorted([p for p in prefixes if p.startswith("v")], reverse=True)
    except Exception as e:
        logger.warning(f"[model_loader] Could not list model versions: {e}")
        return []


def read_model_card(version: str) -> Optional[dict]:
    """Read model_card.json for a given version from MinIO aesthetic-hub-data bucket."""
    s3 = _s3_client()
    key = f"models/{version}/model_card.json"
    try:
        obj = s3.get_object(Bucket=BUCKET_DATA, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning(f"[model_loader] Could not read model card {key}: {e}")
        return None

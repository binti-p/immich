"""
MinIO bucket initialization for aesthetic-hub-data.

Creates bucket structure and populates with:
- Bootstrap stub models (v0000-00-00) from triton-models/
- Directory structure for datasets and production-sim logs

Usage:
    python -m pipelines.batch.bucket_init

Environment variables:
    MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Paths
TRITON_MODELS_DIR = Path("/triton-models")
EMPTY_EMBEDDINGS = Path("/pipelines/batch/empty_embeddings.parquet")

# MinIO config
MINIO_HOST = "minio:9000"
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = "aesthetic-hub-data"


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=False)


def create_bucket(client: Minio):
    """Create bucket if it doesn't exist."""
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"✓ Created bucket: {BUCKET}")
    else:
        log.info(f"✓ Bucket already exists: {BUCKET}")


def upload_models(client: Minio):
    """Upload bootstrap stub models from triton-models directory."""
    log.info("\nUploading models from triton-models...")
    
    # Global cold-start model
    global_model = TRITON_MODELS_DIR / "global_mlp/1/model.onnx"
    if not global_model.exists():
        raise FileNotFoundError(f"Global model not found: {global_model}")
    client.fput_object(BUCKET, "models/global/personalized_mlp.onnx", str(global_model))
    
    # Personalized model v0000-00-00
    pers_model = TRITON_MODELS_DIR / "personalized_mlp/1/model.onnx"
    if not pers_model.exists():
        raise FileNotFoundError(f"Personalized model not found: {pers_model}")
    client.fput_object(BUCKET, "models/v0000-00-00/personalized_mlp.onnx", str(pers_model))
    
    # Empty user embeddings
    if not EMPTY_EMBEDDINGS.exists():
        raise FileNotFoundError(f"Empty embeddings not found: {EMPTY_EMBEDDINGS}")
    client.fput_object(BUCKET, "models/v0000-00-00/user_embeddings.parquet", str(EMPTY_EMBEDDINGS))
    
    # Model card
    model_card = {
        "version_id": "v0000-00-00",
        "dataset_version": "bootstrap",
        "mlp_object_key": "models/v0000-00-00/personalized_mlp.onnx",
        "embeddings_object_key": "models/v0000-00-00/user_embeddings.parquet",
        "quality_gates": {"passed": True},
        "training_details": {"note": "bootstrap stub model"},
        "architecture": {"clip_dim": 768, "user_embedding_dim": 64, "input_dim": 832}
    }
    card_json = json.dumps(model_card, indent=2).encode()
    client.put_object(BUCKET, "models/v0000-00-00/model_card.json", BytesIO(card_json), len(card_json))
    
    log.info("✓ Uploaded models and model card")


def create_directory_structure(client: Minio):
    """Create placeholder files for directory structure."""
    log.info("\nCreating directory structure...")
    
    placeholder = json.dumps({"placeholder": True}).encode()
    directories = [
        "datasets/.placeholder",
        "production-sim/interaction-logs/.placeholder",
        "production-sim/inference-logs/.placeholder"
    ]
    
    for path in directories:
        client.put_object(BUCKET, path, BytesIO(placeholder), len(placeholder))
    
    log.info("✓ Created directory structure")


def main():
    try:
        client = get_minio_client()
        
        # 1. Create bucket
        create_bucket(client)
        
        # 2. Upload models
        upload_models(client)
        
        # 3. Create directory structure
        create_directory_structure(client)
        
        log.info("\n=== Bucket Setup Complete ===")
        return 0
        
    except Exception as e:
        log.error(f"Bucket initialization failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

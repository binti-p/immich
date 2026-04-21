"""
MinIO bucket initialization for aesthetic-hub-data and triton-models.

Creates two buckets:
1. aesthetic-hub-data: Training data, models, logs
2. triton-models: Serving models for canary/staging/production

Bucket structure:

aesthetic-hub-data/
  models/
    v0000-00-00/
      best_personalized_model.pth
      user_embeddings.parquet
      model_card.json
  datasets/
    v0000-00-00/
      personalized-flickr/
        dataset_card.json
  production-sim/
    interactions/
    inference-log/

triton-models/
  production/
    global_mlp/
      1/model.onnx
      config.pbtxt
    personalized_mlp/
      1/model.onnx
      config.pbtxt
  staging/
    (same structure)
  canary/
    (same structure)

Usage:
    python -m pipelines.batch.bucket_init

Environment variables:
    MINIO_ROOT_USER, MINIO_ROOT_PASSWORD

Note: Always creates bootstrap data at v0000-00-00. Real versions come from pipeline runs.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Paths
TRITON_MODELS_DIR = Path("/triton-models")
BOOTSTRAP_PTH = Path("/pipelines/batch/bootstrap_personalized_model.pth")

# MinIO config
MINIO_HOST = "minio:9000"
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_DATA = "aesthetic-hub-data"
BUCKET_TRITON = "triton-models"

# Bootstrap version (always use this for initial setup)
BOOTSTRAP_VERSION = "v0000-00-00"


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=False)


def create_bucket(client: Minio):
    """Create buckets if they don't exist."""
    for bucket in [BUCKET_DATA, BUCKET_TRITON]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info(f"✓ Created bucket: {bucket}")
        else:
            log.info(f"✓ Bucket already exists: {bucket}")


def upload_triton_models(client: Minio):
    """
    Upload Triton model structure to triton-models bucket.
    Each environment (canary/staging/production) gets a copy of the full model structure.
    """
    log.info("\nUploading Triton models to triton-models bucket...")
    
    environments = ["canary", "staging", "production"]
    model_types = ["global_mlp", "personalized_mlp"]
    
    for env in environments:
        for model_type in model_types:
            # Upload model.onnx
            model_file = TRITON_MODELS_DIR / model_type / "1" / "model.onnx"
            if not model_file.exists():
                raise FileNotFoundError(f"Model not found: {model_file}")
            
            minio_path = f"{env}/{model_type}/1/model.onnx"
            client.fput_object(BUCKET_TRITON, minio_path, str(model_file))
            log.info(f"  ✓ Uploaded {minio_path}")
            
            # Upload config.pbtxt
            config_file = TRITON_MODELS_DIR / model_type / "config.pbtxt"
            if not config_file.exists():
                raise FileNotFoundError(f"Config not found: {config_file}")
            
            minio_path = f"{env}/{model_type}/config.pbtxt"
            client.fput_object(BUCKET_TRITON, minio_path, str(config_file))
            log.info(f"  ✓ Uploaded {minio_path}")
    
    log.info("✓ Uploaded all Triton models")


def upload_pytorch_models(client: Minio, version: str):
    """
    Upload initial PyTorch checkpoint to models/v{version}/.
    This will be used by retraining pipeline.
    """
    log.info(f"\nUploading PyTorch models to models/{version}/...")
    
    if not BOOTSTRAP_PTH.exists():
        log.warning(f"Bootstrap PTH not found: {BOOTSTRAP_PTH}, skipping")
        return
    
    minio_path = f"models/{version}/best_personalized_model.pth"
    client.fput_object(BUCKET_DATA, minio_path, str(BOOTSTRAP_PTH))
    log.info(f"  ✓ Uploaded {minio_path}")
    
    log.info("✓ Uploaded PyTorch models")


def create_directory_structure(client: Minio):
    """Create placeholder files for directory structure in aesthetic-hub-data bucket."""
    log.info("\nCreating directory structure...")
    
    placeholder = json.dumps({"placeholder": True}).encode()
    directories = [
        "production-sim/interactions/.placeholder",
        "production-sim/inference-log/.placeholder"
    ]
    
    for path in directories:
        client.put_object(BUCKET_DATA, path, BytesIO(placeholder), len(placeholder))
    
    log.info("✓ Created directory structure")


def create_bootstrap_data(client: Minio, version: str):
    """
    Create bootstrap dataset_card.json, model_card.json, and user_embeddings.parquet
    for the initial model version.
    
    Structure:
      models/v{version}/
        best_personalized_model.pth
        user_embeddings.parquet
        model_card.json
      datasets/v{version}/
        personalized-flickr/
          dataset_card.json
    
    This provides baseline data for drift calculations in the first real pipeline run.
    """
    log.info(f"\nCreating bootstrap data for {version}...")
    
    # ── Bootstrap dataset_card.json ───────────────────────────────────────────
    dataset_prefix = f"datasets/{version}/personalized-flickr"
    
    dataset_card = {
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "event_cutoff": datetime.now(timezone.utc).isoformat(),
        "skipped": False,
        "skip_reason": None,
        
        "splits": {
            "train": {"users": 0, "bursts": 0},
            "val": {"users": 0, "bursts": 0},
            "test": {"users": 0, "bursts": 0},
        },
        
        "excluded_rows": {
            "null_or_invalid": 0,
            "missing_clip_embedding": 0,
            "sparse_users": 0,
            "total_excluded": 0,
        },
        
        "label_distribution": {},
        
        "split_sanity": {
            "passed": True,
            "temporal_leakage_violations": 0,
            "burst_overlap_violations": 0,
            "val_users_without_train": 0,
        },
        
        "drift": {
            "score_interaction_spearman_r": None,
            "score_interaction_warning": False,
            "interaction_rate": 0.0,
            "interaction_rate_previous": None,
            "interaction_rate_change_pct": None,
            "interaction_rate_decline_warning": False,
            "label_mean": 0.0,
            "label_mean_previous": None,
            "label_mean_drift": None,
            "label_mean_drift_warning": False,
            "embedding_mean_norm": 0.0,
            "embedding_std_norm": 0.0,
            "embedding_norm_drift_warning": False,
        },
        
        "quality_warnings": {
            "dominant_event_type": False,
            "signal_inversion": False,
            "clip_coverage_pct": 0.0,
            "low_clip_coverage_warning": False,
            "label_parity_warning": False,
        },
        
        "config": {
            "event_window_days": 30,
            "burst_window_secs": 60,
            "min_user_events": 3,
            "split_strategy": "chronological_per_user_burst_level",
        },
        
        "schema": {
            "columns": ["user_id", "asset_id", "clip_embedding", "label", "event_type", "split", "burst_id"],
            "clip_dim": 768,
        },
        
        "artifacts": {
            "train_parquet": f"{dataset_prefix}/train.parquet",
            "val_parquet": f"{dataset_prefix}/val.parquet",
            "test_parquet_permanent": "datasets/personalized-flickr/test.parquet",
            "manifest_parquet": f"{dataset_prefix}/retraining_manifest.parquet",
            "manifest_csv": f"{dataset_prefix}/retraining_manifest.csv",
        },
    }
    
    dataset_card_key = f"{dataset_prefix}/dataset_card.json"
    client.put_object(
        BUCKET_DATA,
        dataset_card_key,
        BytesIO(json.dumps(dataset_card, indent=2).encode()),
        len(json.dumps(dataset_card, indent=2).encode()),
        content_type="application/json",
    )
    log.info(f"  ✓ Created {dataset_card_key}")
    
    # ── Bootstrap model_card.json ─────────────────────────────────────────────
    model_prefix = f"models/{version}"
    
    model_card = {
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset_version": version,
        "model_type": "personalized_mlp",
        "status": "bootstrap",
        
        "training": {
            "epochs": 0,
            "batch_size": 0,
            "learning_rate": 0.0,
            "train_loss": None,
            "val_loss": None,
        },
        
        "quality_gates": {
            "passed": True,
            "val_spearman_r": None,
            "test_spearman_r": None,
        },
        
        "artifacts": {
            "pth_key": f"{model_prefix}/best_personalized_model.pth",
            "onnx_key": None,
            "embeddings_key": f"{model_prefix}/user_embeddings.parquet",
        },
    }
    
    model_card_key = f"{model_prefix}/model_card.json"
    client.put_object(
        BUCKET_DATA,
        model_card_key,
        BytesIO(json.dumps(model_card, indent=2).encode()),
        len(json.dumps(model_card, indent=2).encode()),
        content_type="application/json",
    )
    log.info(f"  ✓ Created {model_card_key}")
    
    # ── Bootstrap user_embeddings.parquet ─────────────────────────────────────
    # Empty parquet with correct schema
    schema = pa.schema([
        pa.field("user_id", pa.string()),
        pa.field("embedding", pa.list_(pa.float32(), 128)),  # MLP hidden dim
        pa.field("model_version", pa.string()),
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),
    ])
    
    empty_table = pa.table(
        {
            "user_id": [],
            "embedding": [],
            "model_version": [],
            "updated_at": [],
        },
        schema=schema,
    )
    
    buf = BytesIO()
    pq.write_table(empty_table, buf)
    buf.seek(0)
    
    embeddings_key = f"{model_prefix}/user_embeddings.parquet"
    client.put_object(BUCKET_DATA, embeddings_key, buf, buf.getbuffer().nbytes)
    log.info(f"  ✓ Created {embeddings_key}")
    
    log.info("✓ Bootstrap data created")


def main():
    try:
        client = get_minio_client()
        
        # 1. Create buckets
        create_bucket(client)
        
        # 2. Upload Triton models to triton-models bucket
        upload_triton_models(client)
        
        # 3. Upload PyTorch models to aesthetic-hub-data bucket
        upload_pytorch_models(client, BOOTSTRAP_VERSION)
        
        # 4. Create directory structure in aesthetic-hub-data bucket
        create_directory_structure(client)
        
        # 5. Create bootstrap data at v0000-00-00
        create_bootstrap_data(client, BOOTSTRAP_VERSION)
        
        log.info("\n=== Bucket Setup Complete ===")
        log.info("Buckets:")
        log.info(f"  {BUCKET_DATA}/")
        log.info(f"    models/{BOOTSTRAP_VERSION}/")
        log.info(f"      best_personalized_model.pth")
        log.info(f"      user_embeddings.parquet")
        log.info(f"      model_card.json")
        log.info(f"    datasets/{BOOTSTRAP_VERSION}/")
        log.info(f"      personalized-flickr/")
        log.info(f"        dataset_card.json")
        log.info(f"    production-sim/")
        log.info(f"      interactions/")
        log.info(f"      inference-log/")
        log.info(f"  {BUCKET_TRITON}/")
        log.info(f"    production/")
        log.info(f"    staging/")
        log.info(f"    canary/")
        return 0
        
    except Exception as e:
        log.error(f"Bucket initialization failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
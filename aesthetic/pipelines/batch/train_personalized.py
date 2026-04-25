from __future__ import annotations

import argparse
import io
import json
import logging
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import mlflow
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from torch.utils.data import DataLoader

try:
    from .training_common import (
        ManifestEmbeddingDataset,
        PersonalizedMLP,
        collate_personalized,
        ensure_dirs,
        evaluate_personalized,
        flatten_config,
        load_config,
        set_seed,
        train_one_epoch_personalized,
    )
except ImportError:
    from training_common import (
        ManifestEmbeddingDataset,
        PersonalizedMLP,
        collate_personalized,
        ensure_dirs,
        evaluate_personalized,
        flatten_config,
        load_config,
        set_seed,
        train_one_epoch_personalized,
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")


PG_HOST = os.environ.get("POSTGRES_HOST", "immich_postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "immich")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")

MINIO_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://immich_minio:9000")
MINIO_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET = os.environ.get("MINIO_BUCKET", "aesthetic-hub-data")

DEFAULT_CONFIG_PATH = Path(__file__).with_name("train_personalized.yaml")


def supports_mlflow_system_metrics() -> bool:
    try:
        import psutil  # noqa: F401
    except ImportError:
        return False
    return True


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


def load_manifest(client, dataset_version: str) -> pd.DataFrame:
    """
    Load retraining manifest (train + val splits only).
    Test split is loaded separately from persistent test set.
    """
    key = f"datasets/v{dataset_version}/personalized-flickr/retraining_manifest.parquet"
    obj = client.get_object(Bucket=BUCKET, Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    return table.to_pandas()


def load_persistent_test_manifest(client) -> pd.DataFrame | None:
    """
    Load permanent held-out test set (10% of users, created once on first pipeline run).
    This test set is reused across all training runs for consistent evaluation.
    """
    key = "datasets/personalized-flickr/test.parquet"
    try:
        obj = client.get_object(Bucket=BUCKET, Key=key)
    except Exception:
        return None
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    df = table.to_pandas()
    if "split" not in df.columns:
        df["split"] = "test"
    return df


def read_dataset_card(client, dataset_version: str) -> dict:
    key = f"datasets/v{dataset_version}/personalized-flickr/dataset_card.json"
    obj = client.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def active_model_version(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "versionId"
            FROM model_versions
            WHERE "activatedAt" IS NOT NULL
              AND "deactivatedAt" IS NULL
            ORDER BY "activatedAt" DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return row["versionId"] if row else None


def read_model_card(client, version_id: str | None) -> dict | None:
    if not version_id:
        log.info("No active model version found while reading model card")
        return None
    key = f"models/{version_id}/model_card.json"
    try:
        obj = client.get_object(Bucket=BUCKET, Key=key)
    except Exception as error:
        log.warning("Failed to read model card for %s at s3://%s/%s: %s", version_id, BUCKET, key, error)
        return None
    card = json.loads(obj["Body"].read())
    log.info(
        "Loaded active model card for %s with keys: checkpoint_object_key=%s mlp_object_key=%s",
        version_id,
        card.get("checkpoint_object_key"),
        card.get("mlp_object_key"),
    )
    return card


def dataset_for_split(manifest: pd.DataFrame, split: str) -> pd.DataFrame:
    return manifest[manifest["split"] == split].reset_index(drop=True)


def build_user_index(train_df: pd.DataFrame) -> dict[str, int]:
    return {user_id: idx for idx, user_id in enumerate(sorted(train_df["user_id"].unique()), start=1)}


def download_checkpoint_bundle(client, checkpoint_key: str, local_path: Path) -> dict | None:
    log.info("Attempting to download warm-start checkpoint from s3://%s/%s", BUCKET, checkpoint_key)
    try:
        obj = client.get_object(Bucket=BUCKET, Key=checkpoint_key)
    except Exception as error:
        log.warning("Could not fetch checkpoint %s from MinIO: %s", checkpoint_key, error)
        return None

    local_path.write_bytes(obj["Body"].read())
    bundle = torch.load(local_path, map_location="cpu", weights_only=False)
    state_dict = bundle.get("model_state_dict")
    log.info(
        "Downloaded checkpoint to %s (has_model_state_dict=%s, num_users=%s, user_ids=%s)",
        local_path,
        isinstance(state_dict, dict),
        bundle.get("num_users"),
        len(bundle.get("user_ids", [])) if isinstance(bundle.get("user_ids"), list) else None,
    )
    return bundle


def load_existing_user_embeddings(conn, user_ids: list[str], expected_dim: int) -> dict[str, np.ndarray]:
    if not user_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT "userId"::text AS user_id, embedding
            FROM user_embeddings
            WHERE "userId" = ANY(%s::uuid[])
            """,
            (user_ids,),
        )
        rows = cur.fetchall()

    embeddings: dict[str, np.ndarray] = {}
    for row in rows:
        embedding = row["embedding"]
        if embedding is None:
            continue
        vector = np.asarray(embedding, dtype=np.float32)
        if vector.shape != (expected_dim,):
            log.warning(
                "Skipping warm-start embedding for %s due to dim mismatch: expected=%s actual=%s",
                row["user_id"],
                expected_dim,
                vector.shape,
            )
            continue
        embeddings[row["user_id"]] = vector

    log.info(
        "Fetched %s/%s existing user embeddings from postgres.user_embeddings (expected_dim=%s)",
        len(embeddings),
        len(user_ids),
        expected_dim,
    )
    return embeddings


def warm_start_user_embeddings(model: PersonalizedMLP, user2idx: dict[str, int], existing: dict[str, np.ndarray]) -> int:
    initialized = 0
    with torch.no_grad():
        for user_id, vector in existing.items():
            idx = user2idx.get(user_id)
            if idx is None:
                continue
            model.user_embedding.weight[idx].copy_(torch.tensor(vector, dtype=torch.float32))
            initialized += 1
    return initialized


def warm_start_model_weights(model: PersonalizedMLP, checkpoint_bundle: dict | None) -> bool:
    if not checkpoint_bundle:
        log.info("Skipping model weight warm start because no checkpoint bundle was loaded")
        return False

    state_dict = checkpoint_bundle.get("model_state_dict")
    if not isinstance(state_dict, dict):
        log.warning("Checkpoint bundle does not contain model_state_dict; skipping weight warm start")
        return False

    filtered_state_dict = {
        key: value
        for key, value in state_dict.items()
        if key != "user_embedding.weight"
    }
    missing, unexpected = model.load_state_dict(filtered_state_dict, strict=False)
    unexpected = [key for key in unexpected if key != "user_embedding.weight"]
    if unexpected:
        log.warning("Unexpected keys while warm-starting model weights: %s", unexpected)
    if missing and any(key != "user_embedding.weight" for key in missing):
        log.warning("Missing keys while warm-starting model weights: %s", missing)
    log.info(
        "Applied warm-start model weights (loaded_keys=%s, missing_keys=%s, unexpected_keys=%s)",
        len(filtered_state_dict),
        missing,
        unexpected,
    )
    return True


def make_loader(df: pd.DataFrame, user2idx: dict[str, int], batch_size: int, shuffle: bool) -> DataLoader:
    dataset = ManifestEmbeddingDataset(df, user2idx)
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=0,
        pin_memory=False,
        collate_fn=collate_personalized,
    )


def log_artifact_if_exists(path: Path):
    if path.exists():
        mlflow.log_artifact(str(path))


def log_metrics_safe(metrics: dict, step: int | None = None):
    filtered = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float)) and not math.isnan(float(value)) and math.isfinite(float(value)):
            filtered[key] = float(value)
    if filtered:
        mlflow.log_metrics(filtered, step=step)


def upload_file(client, local_path: Path, key: str, content_type: str | None = None):
    extra = {"ContentType": content_type} if content_type else {}
    with local_path.open("rb") as file:
        client.upload_fileobj(file, BUCKET, key, ExtraArgs=extra)
    log.info("Uploaded s3://%s/%s", BUCKET, key)


def upload_json(client, payload: dict, key: str):
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("Uploaded s3://%s/%s", BUCKET, key)


def export_user_embeddings(model: PersonalizedMLP, user_ids: list[str], output_path: Path) -> None:
    embedding_weights = model.user_embedding.weight.detach().cpu().numpy().astype(np.float32)[1:]
    table = pa.table(
        {
            "user_id": user_ids,
            "embedding": [row.tolist() for row in embedding_weights],
        },
        schema=pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("embedding", pa.list_(pa.float32(), embedding_weights.shape[1])),
            ]
        ),
    )
    pq.write_table(table, output_path)


def gate_candidate(
    current_card: dict | None,
    candidate_metrics: dict[str, dict],
    thresholds: dict,
) -> dict:
    """
    Quality gate checks for model promotion.
    
    Required checks:
    1. Minimum samples in val split (test is optional)
    2. Val SRCC gain vs baseline (if baseline exists)
    3. Val MAE/MSE regression limits
    """
    checks: dict[str, dict] = {}
    passed = True

    # Check 1: Minimum samples in val (test is optional now)
    min_eval_samples = int(thresholds.get("min_eval_samples", 5))
    
    # Val is required
    if "val" in candidate_metrics:
        val_samples = int(candidate_metrics["val"]["samples"])
        val_passed = val_samples >= min_eval_samples
        checks["val_min_samples"] = {
            "passed": val_passed,
            "candidate": val_samples,
            "minimum": min_eval_samples,
        }
        passed &= val_passed
    else:
        checks["val_min_samples"] = {
            "passed": False,
            "candidate": 0,
            "minimum": min_eval_samples,
        }
        passed = False
    
    # Test is optional (may not exist on first run)
    if "test" in candidate_metrics:
        test_samples = int(candidate_metrics["test"]["samples"])
        test_passed = test_samples >= min_eval_samples
        checks["test_min_samples"] = {
            "passed": test_passed,
            "candidate": test_samples,
            "minimum": min_eval_samples,
        }
        passed &= test_passed

    # Check 2-3: Baseline comparison (only if baseline exists)
    previous_metrics = (current_card or {}).get("offline_metrics", {})
    baseline_version = (current_card or {}).get("version_id")

    baseline_splits_present = "val" in previous_metrics
    baseline_values = []
    if baseline_splits_present:
        baseline_values = [
            previous_metrics["val"].get("srcc"),
            previous_metrics["val"].get("mae"),
            previous_metrics["val"].get("mse"),
        ]
        baseline_splits_present = all(
            value is not None and math.isfinite(float(value))
            for value in baseline_values
        )
    if baseline_splits_present:
        srcc_gain = float(candidate_metrics["val"]["srcc"] - previous_metrics["val"]["srcc"])
        val_mae_delta = float(candidate_metrics["val"]["mae"] - previous_metrics["val"]["mae"])
        val_mse_delta = float(candidate_metrics["val"]["mse"] - previous_metrics["val"]["mse"])
        checks["val_srcc_gain"] = {
            "passed": srcc_gain >= thresholds["min_val_srcc_gain"],
            "candidate": srcc_gain,
            "minimum": thresholds["min_val_srcc_gain"],
        }
        checks["val_mae_regression"] = {
            "passed": val_mae_delta <= thresholds["max_val_mae_regression"],
            "candidate": val_mae_delta,
            "maximum": thresholds["max_val_mae_regression"],
        }
        checks["val_mse_regression"] = {
            "passed": val_mse_delta <= thresholds["max_val_mse_regression"],
            "candidate": val_mse_delta,
            "maximum": thresholds["max_val_mse_regression"],
        }
        passed &= all(check["passed"] for name, check in checks.items() if name.endswith(("gain", "regression")))
    else:
        checks["baseline_available"] = {
            "passed": True,
            "candidate": None,
            "message": "No active personalized model with offline metrics was found; baseline comparison skipped.",
        }

    return {
        "passed": passed,
        "checks": checks,
        "thresholds": thresholds,
        "baseline_version": baseline_version,
    }


def main():
    parser = argparse.ArgumentParser(description="Train the Flickr personalized model from Immich interaction data")
    parser.add_argument("--dataset-version", required=True, help="Dataset version date, e.g. 2026-04-20")
    parser.add_argument("--model-version", help="Model version date. Defaults to current UTC date.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Training config YAML")
    args = parser.parse_args()

    config = load_config(args.config)
    set_seed(int(config["seed"]))

    dataset_version = args.dataset_version
    model_version = args.model_version or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    version_id = f"v{model_version}"

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    client = s3()
    dataset_card = read_dataset_card(client, dataset_version)
    
    # Load manifest (train + val only)
    manifest = load_manifest(client, dataset_version)
    manifest["clip_embedding"] = manifest["clip_embedding"].apply(lambda value: np.asarray(value, dtype=np.float32))
    
    # Load persistent test set (10% held-out users, created once)
    persistent_test_manifest = load_persistent_test_manifest(client)
    if persistent_test_manifest is not None:
        persistent_test_manifest["clip_embedding"] = persistent_test_manifest["clip_embedding"].apply(
            lambda value: np.asarray(value, dtype=np.float32)
        )

    # Split manifest into train and val
    train_df = dataset_for_split(manifest, "train")
    val_df = dataset_for_split(manifest, "val")
    
    # Use persistent test set (always use this for consistent evaluation across runs)
    test_df = persistent_test_manifest if persistent_test_manifest is not None else pd.DataFrame()
    
    if train_df.empty:
        raise RuntimeError("Training split is empty; run data prep first or lower the sparse-user threshold.")
    
    if test_df.empty:
        log.warning("No test set available - persistent test set not found. Evaluation will only use val split.")
    
    # Build user index from train set (all users in train due to chronological split)
    user2idx = build_user_index(train_df)
    user_ids = sorted(user2idx, key=user2idx.get)
    conn = get_conn()
    try:
        current_version = active_model_version(conn)
        log.info("Active model version for warm start: %s", current_version)
        existing_user_embeddings = load_existing_user_embeddings(
            conn,
            user_ids,
            int(config["model"]["user_emb_dim"]),
        )
    finally:
        conn.close()
    current_card = read_model_card(client, current_version)
    checkpoint_bundle = None
    checkpoint_source = None
    with tempfile.TemporaryDirectory(prefix=f"flickr-personalized-bootstrap-{model_version}-") as bootstrap_dir:
        bootstrap_path = Path(bootstrap_dir) / "bootstrap_model.pth"
        if current_card and current_card.get("checkpoint_object_key"):
            checkpoint_source = current_card["checkpoint_object_key"]
            checkpoint_bundle = download_checkpoint_bundle(client, checkpoint_source, bootstrap_path)
        else:
            log.info(
                "No checkpoint_object_key available for active version %s; current_card_present=%s",
                current_version,
                current_card is not None,
            )

    train_loader = make_loader(train_df, user2idx, int(config["training"]["batch_size"]), True)
    val_loader = make_loader(val_df, user2idx, int(config["training"]["batch_size"]), False)
    
    # Only create test loader if test set exists
    test_loader = None
    if not test_df.empty:
        test_loader = make_loader(test_df, user2idx, int(config["training"]["batch_size"]), False)
    device = config["training"]["device"]
    model = PersonalizedMLP(
        num_users=len(user2idx) + 1,
        input_dim=int(config["model"]["input_dim"]),
        user_emb_dim=int(config["model"]["user_emb_dim"]),
    ).to(device)
    warm_started_model = warm_start_model_weights(model, checkpoint_bundle)
    warm_started_users = warm_start_user_embeddings(model, user2idx, existing_user_embeddings)
    log.info(
        "Warm-started model_weights=%s from %s; user_embeddings=%s/%s from user_embeddings table (active_version=%s)",
        warm_started_model,
        checkpoint_source,
        warm_started_users,
        len(user2idx),
        current_version,
    )

    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=float(config["training"]["lr"]),
        weight_decay=float(config["training"].get("weight_decay", 0.0)),
    )

    run_name = f"{config['mlflow']['run_name']}-{model_version}"
    with tempfile.TemporaryDirectory(prefix=f"flickr-personalized-{model_version}-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        output_root = tmp_path / "outputs"
        checkpoint_dir = output_root / "checkpoints"
        prediction_dir = output_root / "predictions"
        export_dir = output_root / "export"
        ensure_dirs(output_root, checkpoint_dir, prediction_dir, export_dir)

        best_ckpt = checkpoint_dir / "best_personalized_model.pth"
        last_ckpt = checkpoint_dir / "last_personalized_model.pth"

        history: list[dict] = []
        best_val_srcc = -1e9

        try:
            mlflow.end_run()
        except Exception:
            pass

        log_system_metrics = supports_mlflow_system_metrics()
        if not log_system_metrics:
            log.warning("psutil is not installed; disabling MLflow system metrics logging for this run")

        with mlflow.start_run(run_name=run_name, log_system_metrics=log_system_metrics) as run:
            mlflow.set_tags(
                {
                    "model_type": "personalized",
                    "dataset": "IMMICH_INTERACTIONS",
                    "task": "personalized_aesthetic_prediction",
                    "framework": "pytorch",
                    "dataset_version": dataset_version,
                    "candidate_model_version": version_id,
                }
            )
            mlflow.log_params(flatten_config(config))
            mlflow.log_params(
                {
                    "dataset_version": dataset_version,
                    "model_version": version_id,
                    "num_train_rows": len(train_df),
                    "num_val_rows": len(val_df),
                    "num_test_rows": len(test_df),
                    "num_train_users": train_df["user_id"].nunique(),
                    "num_val_users": val_df["user_id"].nunique(),
                    "num_test_users": test_df["user_id"].nunique(),
                    "warm_started_model": int(warm_started_model),
                    "warm_started_train_users": warm_started_users,
                }
            )

            for epoch in range(1, int(config["training"]["epochs"]) + 1):
                start = time.time()
                train_mse = train_one_epoch_personalized(model, train_loader, optimizer, device)
                val_metrics, _ = evaluate_personalized(model, val_loader, device)
                epoch_time = time.time() - start

                row = {
                    "epoch": epoch,
                    "train_mse": train_mse,
                    "val_mse": val_metrics["mse"],
                    "val_mae": val_metrics["mae"],
                    "val_plcc": val_metrics["plcc"],
                    "val_srcc": val_metrics["srcc"],
                    "epoch_time_sec": epoch_time,
                }
                history.append(row)
                log_metrics_safe(row, step=epoch)

                checkpoint_payload = {
                    "epoch": epoch,
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "history": history,
                    "model_config": {
                        "input_dim": int(config["model"]["input_dim"]),
                        "user_emb_dim": int(config["model"]["user_emb_dim"]),
                        "architecture": "legacy_personalized_mlp",
                        "layer_dims": [832, 512, 128, 32, 1],
                    },
                    "num_users": len(user2idx) + 1,
                    "user_ids": user_ids,
                }
                torch.save(checkpoint_payload, last_ckpt)

                if val_metrics["srcc"] > best_val_srcc:
                    best_val_srcc = val_metrics["srcc"]
                    torch.save(checkpoint_payload, best_ckpt)
                    mlflow.log_metric("best_val_srcc", best_val_srcc, step=epoch)

            best_bundle = torch.load(best_ckpt, map_location=device, weights_only=False)
            model.load_state_dict(best_bundle["model_state_dict"])

            split_metrics: dict[str, dict] = {}
            prediction_paths: dict[str, Path] = {}

            # Evaluate on val (always available)
            val_metrics, val_predictions = evaluate_personalized(model, val_loader, device)
            split_metrics["val"] = val_metrics
            val_prediction_path = prediction_dir / "val_predictions.csv"
            val_predictions.to_csv(val_prediction_path, index=False)
            prediction_paths["val"] = val_prediction_path
            log_metrics_safe({f"val_{metric}": value for metric, value in val_metrics.items() if metric != "samples"})
            mlflow.log_metric("val_samples", val_metrics["samples"])
            
            # Evaluate on test (if available)
            if test_loader is not None:
                test_metrics, test_predictions = evaluate_personalized(model, test_loader, device)
                split_metrics["test"] = test_metrics
                test_prediction_path = prediction_dir / "test_predictions.csv"
                test_predictions.to_csv(test_prediction_path, index=False)
                prediction_paths["test"] = test_prediction_path
                log_metrics_safe({f"test_{metric}": value for metric, value in test_metrics.items() if metric != "samples"})
                mlflow.log_metric("test_samples", test_metrics["samples"])
            else:
                log.warning("Skipping test evaluation - no test set available")

            history_path = output_root / "history.csv"
            metrics_path = output_root / "metrics.csv"
            user_embeddings_path = export_dir / "user_embeddings.parquet"
            pd.DataFrame(history).to_csv(history_path, index=False)
            pd.DataFrame([{"split": split, **metrics} for split, metrics in split_metrics.items()]).to_csv(metrics_path, index=False)
            export_user_embeddings(model, user_ids, user_embeddings_path)

            thresholds = config["quality_gates"]
            gate_result = gate_candidate(current_card, split_metrics, thresholds)
            mlflow.set_tag("quality_gates_passed", str(gate_result["passed"]).lower())

            checkpoint_key = f"models/{version_id}/best_personalized_model.pth"
            embeddings_key = f"models/{version_id}/user_embeddings.parquet"

            upload_file(client, best_ckpt, checkpoint_key)
            upload_file(client, user_embeddings_path, embeddings_key)

            model_card = {
                "version_id": version_id,
                "dataset_version": dataset_version,
                "mlp_object_key": None,
                "checkpoint_object_key": checkpoint_key,
                "embeddings_object_key": embeddings_key,
                "quality_gates": gate_result,
                "offline_metrics": split_metrics,
                "training_details": {
                    "mlflow_run_id": run.info.run_id,
                    "run_name": run_name,
                    "trained_at": datetime.now(timezone.utc).isoformat(),
                    "epochs": int(config["training"]["epochs"]),
                    "batch_size": int(config["training"]["batch_size"]),
                    "learning_rate": float(config["training"]["lr"]),
                    "weight_decay": float(config["training"].get("weight_decay", 0.0)),
                    "dataset_card_key": f"datasets/v{dataset_version}/personalized-flickr/dataset_card.json",
                    "warm_started_model": warm_started_model,
                    "warm_start_checkpoint_key": checkpoint_source,
                    "warm_started_train_users": warm_started_users,
                    "warm_start_source": "postgres.user_embeddings",
                    "warm_start_model_version": current_version,
                },
                "architecture": {
                    "name": "legacy_personalized_mlp",
                    "clip_dim": int(config["model"]["input_dim"]),
                    "user_embedding_dim": int(config["model"]["user_emb_dim"]),
                    "input_dim": int(config["model"]["input_dim"]) + int(config["model"]["user_emb_dim"]),
                    "layer_dims": [832, 512, 128, 32, 1],
                },
                "dataset_summary": {
                    "train_rows": len(train_df),
                    "val_rows": len(val_df),
                    "test_rows": len(test_df),
                    "train_users": train_df["user_id"].nunique(),
                    "source_event_window_days": dataset_card.get("event_window_days"),
                },
            }
            upload_json(client, model_card, f"models/{version_id}/model_card.json")

            log_artifact_if_exists(best_ckpt)
            log_artifact_if_exists(last_ckpt)
            log_artifact_if_exists(history_path)
            log_artifact_if_exists(metrics_path)
            log_artifact_if_exists(user_embeddings_path)
            for prediction_path in prediction_paths.values():
                log_artifact_if_exists(prediction_path)

            log.info(
                "Training complete for %s. Gates passed=%s",
                version_id,
                gate_result["passed"],
            )


if __name__ == "__main__":
    main()

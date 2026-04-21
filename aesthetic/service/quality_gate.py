#!/usr/bin/env python3
"""
Serving-side quality gate for the personalized ONNX model.
Called by Argo after export-onnx step.
Checks:
  - Model loads without error
  - No NaN/Inf outputs on test embeddings
  - Spearman-r on Flickr-AES test split meets threshold
  - MSE meets threshold
  - P95 batch latency meets threshold
Exits 0 (pass) or 1 (fail).
"""
import sys
import time
import argparse
import yaml
import boto3
import numpy as np
import onnxruntime as ort
from botocore.client import Config
from scipy.stats import spearmanr
import os

parser = argparse.ArgumentParser()
parser.add_argument("--criteria-file", default="/app/promotion-criteria.yaml")
parser.add_argument("--minio-endpoint", required=True)
parser.add_argument("--minio-bucket", default="triton-models")
parser.add_argument("--model-key", default="staging/personalized_mlp/1/model.onnx")
parser.add_argument("--test-data-key", default="aesthetic-hub-data/flickr_test_embeddings.npz")
parser.add_argument("--output-result", default="/tmp/quality-gate-passed.txt")
args = parser.parse_args()

# --- Load criteria ---
with open(args.criteria_file) as f:
    criteria = yaml.safe_load(f)["quality_gate"]

MIN_SPEARMAN_R = criteria["min_spearman_r"]
MAX_MSE = criteria["max_mse"]
MAX_P95_LATENCY_MS = criteria["max_p95_latency_ms"]
MIN_EVAL_SAMPLES = criteria["min_eval_samples"]
ALLOW_NAN = criteria["allow_nan_outputs"]

RESULTS = []

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {detail}")
    RESULTS.append((name, passed))

# --- Download model from MinIO ---
print("Downloading model from MinIO...")
s3 = boto3.client(
    "s3",
    endpoint_url=args.minio_endpoint,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

try:
    s3.download_file(args.minio_bucket, args.model_key, "/tmp/model.onnx")
    check("model download from MinIO", True, args.model_key)
except Exception as e:
    check("model download from MinIO", False, str(e))
    print("FATAL: cannot download model, aborting")
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

# --- Load ONNX model ---
try:
    sess = ort.InferenceSession(
        "/tmp/model.onnx",
        providers=["CPUExecutionProvider"]
    )
    input_names = [i.name for i in sess.get_inputs()]
    check("model loads without error", True, f"inputs={input_names}")
except Exception as e:
    check("model loads without error", False, str(e))
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

# --- Download test data ---
# This is a .npz file with keys: embeddings (N,768), user_embeddings (N,64), scores (N,)
# Binti's data pipeline should have put this in MinIO during data prep
print("Downloading test data...")
try:
    s3.download_file(
        "aesthetic-hub-data",
        "flickr_test_embeddings.npz",
        "/tmp/test_data.npz"
    )
    data = np.load("/tmp/test_data.npz")
    embeddings = data["embeddings"].astype(np.float32)             # (N, 768)
    user_embeddings = data["user_embeddings"].astype(np.float32)   # (N, 64)
    gt_scores = data["scores"].astype(np.float32)                  # (N,) ground truth, 0-1 scale
    check("test data loaded", True, f"N={len(embeddings)}")
except Exception as e:
    check("test data loaded", False, str(e))
    print("FATAL: cannot load test data, aborting")
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

check(
    "sufficient eval samples",
    len(embeddings) >= MIN_EVAL_SAMPLES,
    f"{len(embeddings)} samples (need {MIN_EVAL_SAMPLES})"
)

# --- Run inference on test set ---
print("Running inference on test set...")
BATCH_SIZE = 64
predictions = []

try:
    for i in range(0, len(embeddings), BATCH_SIZE):
        batch_emb = embeddings[i:i+BATCH_SIZE]
        batch_user = user_embeddings[i:i+BATCH_SIZE]
        outputs = sess.run(
            ["output"],
            {
                "image_embedding": batch_emb,
                "user_embedding": batch_user
            }
        )
        predictions.extend(outputs[0].flatten().tolist())
except Exception as e:
    check("inference runs without error", False, str(e))
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

predictions = np.array(predictions, dtype=np.float32)

# --- Check NaN/Inf ---
has_nan = bool(np.any(np.isnan(predictions)))
has_inf = bool(np.any(np.isinf(predictions)))
check("no NaN outputs", not has_nan, f"NaN count={np.sum(np.isnan(predictions))}")
check("no Inf outputs", not has_inf, f"Inf count={np.sum(np.isinf(predictions))}")

# --- Spearman-r ---
srcc, _ = spearmanr(gt_scores[:len(predictions)], predictions)
check(
    f"Spearman-r >= {MIN_SPEARMAN_R}",
    srcc >= MIN_SPEARMAN_R,
    f"SRCC={srcc:.4f}"
)

# --- MSE ---
mse = float(np.mean((gt_scores[:len(predictions)] - predictions) ** 2))
check(
    f"MSE <= {MAX_MSE}",
    mse <= MAX_MSE,
    f"MSE={mse:.6f}"
)

# --- P95 Latency on a batch of 64 ---
print("Benchmarking P95 latency...")
latencies = []
bench_emb = embeddings[:64]
bench_user = user_embeddings[:64]

for _ in range(100):   # 100 runs of batch=64
    start = time.perf_counter()
    sess.run(
        ["output"],
        {"image_embedding": bench_emb, "user_embedding": bench_user}
    )
    latencies.append((time.perf_counter() - start) * 1000)

p95_ms = float(np.percentile(latencies, 95))
check(
    f"P95 latency <= {MAX_P95_LATENCY_MS}ms",
    p95_ms <= MAX_P95_LATENCY_MS,
    f"P95={p95_ms:.2f}ms"
)

# --- Final decision ---
all_passed = all(passed for _, passed in RESULTS)

with open(args.output_result, "w") as f:
    f.write("true" if all_passed else "false")

print(f"\n{'ALL QUALITY GATES PASSED' if all_passed else 'QUALITY GATE FAILED'}")
print(f"Summary: {sum(p for _, p in RESULTS)}/{len(RESULTS)} checks passed")
sys.exit(0 if all_passed else 1)
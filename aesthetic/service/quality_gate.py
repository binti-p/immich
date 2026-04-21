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
parser.add_argument("--data-bucket", default="aesthetic-hub-data")
parser.add_argument("--model-key", default="staging/personalized_mlp/1/model.onnx")
parser.add_argument("--test-data-key", default="datasets/personalized-flickr/test.parquet")
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

# --- Download test data (image embeddings + ground truth from MinIO) ---
print("Downloading test data from MinIO...")
try:
    s3.download_file(
        args.data_bucket,
        args.test_data_key,
        "/tmp/test_data.parquet"
    )
    import pyarrow.parquet as pq
    table = pq.read_table("/tmp/test_data.parquet")
    df = table.to_pandas()

    embeddings = np.stack(df["embedding"].values).astype(np.float32)  # (N, 768)
    gt_scores = df["score"].values.astype(np.float32)                 # (N,)
    check("test image data loaded", True, f"N={len(embeddings)} images")
except Exception as e:
    check("test image data loaded", False, str(e))
    print("FATAL: cannot load test data, aborting")
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

# --- Load user embeddings from Postgres ---
print("Loading user embeddings from Postgres...")
try:
    import psycopg2
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "immich-postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "immich"),
        password=os.environ.get("POSTGRES_PASSWORD", "immich"),
    )
    with conn.cursor() as cur:
        cur.execute('SELECT "userId"::text, embedding FROM user_embeddings')
        rows = cur.fetchall()
    conn.close()

    # Filter out zero-vector embeddings (cold-start users)
    user_embs = {}
    for user_id, emb in rows:
        arr = np.array(emb, dtype=np.float32)
        if np.linalg.norm(arr) > 1e-6:
            user_embs[user_id] = arr

    check("user embeddings loaded", True, f"{len(user_embs)} users with trained embeddings (of {len(rows)} total)")

    if len(user_embs) == 0:
        print("WARNING: No trained user embeddings found — skipping personalized evaluation, using global only")
        # Fall back: create a dummy zero embedding so inference still runs (global-only mode)
        user_emb_list = [np.zeros(64, dtype=np.float32)]
    else:
        user_emb_list = list(user_embs.values())
except Exception as e:
    check("user embeddings loaded", False, str(e))
    print("FATAL: cannot load user embeddings, aborting")
    with open(args.output_result, "w") as f:
        f.write("false")
    sys.exit(1)

# --- Build cross-test pairs: each user × each image ---
# To keep evaluation tractable, sample up to 5 users if many exist
sampled_users = user_emb_list[:5] if len(user_emb_list) > 5 else user_emb_list
print(f"Cross-testing {len(sampled_users)} users × {len(embeddings)} images = {len(sampled_users) * len(embeddings)} pairs")

# Tile: repeat each image embedding for each user, repeat each user embedding for each image
all_image_embs = np.tile(embeddings, (len(sampled_users), 1))                          # (U*N, 768)
all_user_embs = np.repeat(np.stack(sampled_users), len(embeddings), axis=0)            # (U*N, 64)
all_gt_scores = np.tile(gt_scores, len(sampled_users))                                 # (U*N,)

user_embeddings = all_user_embs  # for compatibility with downstream code

check(
    "sufficient eval samples",
    len(all_image_embs) >= MIN_EVAL_SAMPLES,
    f"{len(all_image_embs)} pairs (need {MIN_EVAL_SAMPLES})"
)

# --- Run inference on cross-test set ---
print(f"Running inference on {len(all_image_embs)} pairs...")
BATCH_SIZE = 64
predictions = []

try:
    for i in range(0, len(all_image_embs), BATCH_SIZE):
        batch_emb = all_image_embs[i:i+BATCH_SIZE]
        batch_user = all_user_embs[i:i+BATCH_SIZE]
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
srcc, _ = spearmanr(all_gt_scores[:len(predictions)], predictions)
check(
    f"Spearman-r >= {MIN_SPEARMAN_R}",
    srcc >= MIN_SPEARMAN_R,
    f"SRCC={srcc:.4f}"
)

# --- MSE ---
mse = float(np.mean((all_gt_scores[:len(predictions)] - predictions) ** 2))
check(
    f"MSE <= {MAX_MSE}",
    mse <= MAX_MSE,
    f"MSE={mse:.6f}"
)

# --- P95 Latency on a batch of 64 ---
print("Benchmarking P95 latency...")
latencies = []
bench_emb = all_image_embs[:64]
bench_user = all_user_embs[:64]

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
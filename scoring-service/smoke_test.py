#!/usr/bin/env python3
"""
Smoke test for the aesthetic scoring service.
Called by Argo after staging deployment.
Exits 0 if pass, 1 if fail.
"""
import sys
import time
import argparse
import requests
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--scoring-url", required=True)
parser.add_argument("--output-result", default="/tmp/smoke-passed.txt")
args = parser.parse_args()

BASE_URL = args.scoring_url.rstrip("/")
RESULTS = []


def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {detail}")
    RESULTS.append(passed)


def make_request(asset_id="smoke-asset-001", cold_start=False):
    """Build a valid ScoreRequest matching feature-svc output format."""
    return {
        "request_id": f"smoke-{asset_id}-{int(time.time())}",
        "asset_id": "00000000-0000-0000-0000-000000000001",
        "user_id": "00000000-0000-0000-0000-000000000099",
        "clip_embedding": np.random.randn(768).astype(np.float32).tolist(),
        "user_embedding": None if cold_start else np.random.randn(64).astype(np.float32).tolist(),
        "alpha": 0.0 if cold_start else 0.42,
        "is_cold_start": cold_start,
        "model_version": "smoke-test",
        "source": "smoke_test"
    }


# --- Test 1: Health ---
try:
    r = requests.get(f"{BASE_URL}/health", timeout=5)
    check("health endpoint", r.status_code == 200, f"status={r.status_code}")
except Exception as e:
    check("health endpoint", False, str(e))

# --- Test 2: Cold start (global model only, alpha=0) ---
try:
    start = time.time()
    r = requests.post(f"{BASE_URL}/score", json=make_request(cold_start=True), timeout=10)
    latency_ms = (time.time() - start) * 1000
    check("cold start returns 200", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("score in [0,1]", 0.0 <= body["score"] <= 1.0, f"score={body['score']}")
        check("alpha is 0 for cold start", body["alpha"] == 0.0, f"alpha={body['alpha']}")
        check("personalized_score is null for cold start",
              body["personalized_score"] is None)
        check("latency < 500ms", latency_ms < 500, f"{latency_ms:.1f}ms")
except Exception as e:
    check("cold start score", False, str(e))

# --- Test 3: Warm user (personalized model called) ---
try:
    r = requests.post(f"{BASE_URL}/score", json=make_request(cold_start=False), timeout=10)
    check("warm user returns 200", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        body = r.json()
        check("score in [0,1]", 0.0 <= body["score"] <= 1.0, f"score={body['score']}")
        check("alpha > 0 for warm user", body["alpha"] > 0.0, f"alpha={body['alpha']}")
except Exception as e:
    check("warm user score", False, str(e))

# --- Test 4: Batch endpoint ---
batch_payload = {
    "items": [make_request(cold_start=(i % 2 == 0)) for i in range(5)]
}
try:
    r = requests.post(f"{BASE_URL}/score/batch", json=batch_payload, timeout=15)
    check("batch returns 200", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        items = r.json()
        check("batch returns 5 items", len(items) == 5, f"got {len(items)}")
        check("all scores in [0,1]", all(0.0 <= i["score"] <= 1.0 for i in items))
except Exception as e:
    check("batch endpoint", False, str(e))

# --- Test 5: Bad embedding rejected ---
bad = make_request()
bad["clip_embedding"] = [0.1] * 512  # wrong dim
try:
    r = requests.post(f"{BASE_URL}/score", json=bad, timeout=5)
    check("bad embedding returns 422", r.status_code == 422, f"status={r.status_code}")
except Exception as e:
    check("bad embedding rejection", False, str(e))

# --- Result ---
all_passed = all(RESULTS)
with open(args.output_result, "w") as f:
    f.write("true" if all_passed else "false")

print(f"\n{'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
sys.exit(0 if all_passed else 1)
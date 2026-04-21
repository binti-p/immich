#!/usr/bin/env python3
"""
Canary observation monitor.
Queries Prometheus for canary scoring service metrics.
After observation window, decides: promote to production or rollback.
Exits 0 = promote, 1 = rollback.
"""
import sys
import time
import argparse
import requests

parser = argparse.ArgumentParser()
parser.add_argument("--prometheus-url", default="http://prometheus.monitoring.svc.cluster.local:9090")
parser.add_argument("--observation-minutes", type=int, default=30)
parser.add_argument("--max-error-rate", type=float, default=0.02)      # 2%
parser.add_argument("--max-p95-latency-ms", type=float, default=200.0)
parser.add_argument("--min-requests", type=int, default=10)             # need at least this many requests to make a decision
parser.add_argument("--output-decision", default="/tmp/canary-decision.txt")
args = parser.parse_args()

PROM = args.prometheus_url.rstrip("/")

def query(promql: str) -> float | None:
    try:
        r = requests.get(f"{PROM}/api/v1/query",
                        params={"query": promql}, timeout=10)
        data = r.json()
        results = data["data"]["result"]
        if not results:
            return None
        return float(results[0]["value"][1])
    except Exception as e:
        print(f"Prometheus query failed: {promql} — {e}")
        return None

print(f"Waiting {args.observation_minutes} minutes for canary traffic to accumulate...")
time.sleep(args.observation_minutes * 60)

print("Observation window complete. Querying Prometheus...")

# These queries target the canary namespace specifically via the ENVIRONMENT label
# which your scoring service sets as an env var
window = f"{args.observation_minutes}m"

error_rate = query(
    f'sum(rate(scoring_errors_total{{environment="canary"}}[{window}])) '
    f'/ sum(rate(scoring_latency_seconds_count{{environment="canary"}}[{window}]))'
)

p95_latency_ms = query(
    f'histogram_quantile(0.95, '
    f'sum(rate(scoring_latency_seconds_bucket{{environment="canary"}}[{window}])) by (le)) * 1000'
)

total_requests = query(
    f'sum(increase(scoring_latency_seconds_count{{environment="canary"}}[{window}]))'
)

low_conf_rate = query(
    f'sum(rate(scoring_low_confidence_total{{environment="canary"}}[{window}])) '
    f'/ sum(rate(scoring_latency_seconds_count{{environment="canary"}}[{window}]))'
)

print(f"Results:")
print(f"  Total requests:      {total_requests}")
print(f"  Error rate:          {error_rate}")
print(f"  P95 latency (ms):    {p95_latency_ms}")
print(f"  Low confidence rate: {low_conf_rate}")

# Decision logic
reasons = []

if total_requests is None or total_requests < args.min_requests:
    reasons.append(f"Insufficient traffic: {total_requests} requests (need {args.min_requests})")

if error_rate is not None and error_rate > args.max_error_rate:
    reasons.append(f"Error rate too high: {error_rate:.2%} > {args.max_error_rate:.2%}")

if p95_latency_ms is not None and p95_latency_ms > args.max_p95_latency_ms:
    reasons.append(f"P95 latency too high: {p95_latency_ms:.1f}ms > {args.max_p95_latency_ms:.1f}ms")

if low_conf_rate is not None and low_conf_rate > 0.3:
    reasons.append(f"Low confidence rate too high: {low_conf_rate:.2%} — possible model quality issue")

decision = "rollback" if reasons else "promote"

with open(args.output_decision, "w") as f:
    f.write(decision)

if reasons:
    print(f"\nDECISION: ROLLBACK")
    for r in reasons:
        print(f"  REASON: {r}")
    sys.exit(1)
else:
    print(f"\nDECISION: PROMOTE TO PRODUCTION")
    sys.exit(0)
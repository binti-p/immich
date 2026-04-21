from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import Iterable

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def read_user_ids(worker_api_keys_csv: Path) -> list[str]:
    with worker_api_keys_csv.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        required = {"user_id"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Worker API keys CSV is missing required columns: {sorted(missing)}")

        user_ids = []
        seen = set()
        for row in reader:
            user_id = row.get("user_id", "").strip()
            if not user_id or user_id in seen:
                continue
            seen.add(user_id)
            user_ids.append(user_id)
        return user_ids


def trigger_rescore(server_url: str, admin_api_key: str, user_id: str | None = None) -> dict:
    url = server_url.rstrip("/") + "/api/aesthetic/admin/rescore-all"
    params = {"userId": user_id} if user_id else None
    response = requests.post(
        url,
        params=params,
        headers={"x-api-key": admin_api_key, "Accept": "application/json"},
        timeout=60,
    )
    if response.status_code != 202:
        raise RuntimeError(
            f"Rescore request failed for {user_id or 'all users'}: {response.status_code} {response.text}"
        )
    return response.json()


def iter_targets(args: argparse.Namespace) -> Iterable[str | None]:
    if args.worker_api_keys_csv:
        yield from read_user_ids(Path(args.worker_api_keys_csv))
        return
    yield None


def main():
    parser = argparse.ArgumentParser(
        description="Trigger Immich aesthetic rescoring for all users or one job per user from worker_api_keys.csv"
    )
    parser.add_argument("--server-url", default="http://localhost:2283", help="Immich base URL")
    parser.add_argument("--admin-api-key", required=True, help="Admin API key for the Immich API")
    parser.add_argument(
        "--worker-api-keys-csv",
        default=None,
        help="Optional worker_api_keys.csv to trigger one rescore job per distinct user_id instead of a single global job",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print intended requests without sending them")
    args = parser.parse_args()

    queued = 0
    for user_id in iter_targets(args):
        target_label = user_id or "all users"
        if args.dry_run:
            log.info("Would trigger rescore for %s", target_label)
            queued += 1
            continue

        result = trigger_rescore(args.server_url, args.admin_api_key, user_id)
        log.info("Queued rescore for %s: jobId=%s", target_label, result.get("jobId"))
        queued += 1

    log.info("Finished. queued_jobs=%s dry_run=%s", queued, args.dry_run)


if __name__ == "__main__":
    main()

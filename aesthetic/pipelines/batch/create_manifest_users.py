from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def read_rows(input_csv: Path):
    with input_csv.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        required = {"worker_id", "email", "password"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Input CSV is missing required columns: {sorted(missing)}")
        for row in reader:
            if not row.get("worker_id") or not row.get("email") or not row.get("password"):
                continue
            yield row


def create_user(session: requests.Session, server_url: str, row: dict, default_storage_label: str | None):
    payload = {
        "email": row["email"].strip(),
        "password": row["password"],
        "name": row["worker_id"].strip(),
        "shouldChangePassword": False,
        "notify": False,
        "isAdmin": False,
    }
    if default_storage_label:
        payload["storageLabel"] = default_storage_label

    response = session.post(server_url.rstrip("/") + "/api/admin/users", json=payload, timeout=60)
    return response


def main():
    parser = argparse.ArgumentParser(description="Create Immich users from a CSV using the admin API")
    parser.add_argument("--input-csv", required=True, help="CSV with worker_id,email,password")
    parser.add_argument("--server-url", default="http://localhost:2283", help="Immich base URL")
    parser.add_argument("--admin-api-key", required=True, help="Admin API key with user creation permission")
    parser.add_argument("--storage-label", default=None, help="Optional storage label for all created users")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": args.admin_api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )

    created = 0
    skipped = 0
    failed = 0

    for row in read_rows(Path(args.input_csv)):
        email = row["email"].strip()
        response = create_user(session, args.server_url, row, args.storage_label)
        if response.status_code in (200, 201):
            created += 1
            body = response.json()
            log.info("Created user %s (%s)", email, body.get("id"))
            continue

        # Immich returns 400 if the email already exists.
        if response.status_code == 400 and "Email already in use" in response.text:
            skipped += 1
            log.info("Skipped existing user %s", email)
            continue

        failed += 1
        log.error("Failed to create %s: %s %s", email, response.status_code, response.text)

    log.info("Finished user creation. created=%s skipped=%s failed=%s", created, skipped, failed)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

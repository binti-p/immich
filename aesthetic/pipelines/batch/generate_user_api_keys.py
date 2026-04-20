from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def login(session: requests.Session, server_url: str, email: str, password: str) -> dict:
    response = session.post(
        server_url.rstrip("/") + "/api/auth/login",
        json={"email": email, "password": password},
        timeout=60,
    )
    if response.status_code != 201:
        raise RuntimeError(f"Login failed for {email}: {response.status_code} {response.text}")
    return response.json()


def create_api_key(session: requests.Session, server_url: str, access_token: str, key_name: str) -> dict:
    response = session.post(
        server_url.rstrip("/") + "/api/api-keys",
        headers={"Authorization": f"Bearer {access_token}"},
        json={"name": key_name, "permissions": ["all"]},
        timeout=60,
    )
    if response.status_code != 201:
        raise RuntimeError(f"API key creation failed: {response.status_code} {response.text}")
    return response.json()


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


def main():
    parser = argparse.ArgumentParser(description="Generate Immich API keys from a worker/email/password CSV")
    parser.add_argument("--input-csv", required=True, help="CSV with worker_id,email,password")
    parser.add_argument("--output-csv", required=True, help="Where to write worker_id,email,api_key,user_id")
    parser.add_argument("--server-url", default="http://localhost:2283", help="Immich base URL")
    parser.add_argument("--key-name", default="seed-uploader", help="API key name to create for each user")
    args = parser.parse_args()

    output_rows: list[dict] = []

    for row in read_rows(Path(args.input_csv)):
        email = row["email"].strip()
        worker_id = row["worker_id"].strip()
        password = row["password"]
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})

        log.info("Logging in %s for worker %s", email, worker_id)
        login_payload = login(session, args.server_url, email, password)
        key_payload = create_api_key(session, args.server_url, login_payload["accessToken"], args.key_name)

        output_rows.append(
            {
                "worker_id": worker_id,
                "email": email,
                "user_id": login_payload["userId"],
                "api_key": key_payload["secret"],
                "api_key_id": key_payload["apiKey"]["id"],
            }
        )
        log.info("Created API key for %s", email)

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["worker_id", "email", "user_id", "api_key", "api_key_id"])
        writer.writeheader()
        writer.writerows(output_rows)

    log.info("Wrote %s API keys to %s", len(output_rows), output_path)


if __name__ == "__main__":
    main()

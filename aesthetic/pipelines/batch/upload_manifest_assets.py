from __future__ import annotations

import argparse
import csv
import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def iter_production_new_user_rows(csv_path: Path) -> Iterator[dict[str, str]]:
    seen = set()
    with csv_path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row.get("split") != "production_new_user":
                continue
            image_path = row["image_path"]
            if image_path in seen:
                continue
            seen.add(image_path)
            yield row


def read_worker_api_keys(csv_path: Path) -> dict[str, dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        required = {"worker_id", "api_key"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Worker API keys CSV is missing required columns: {sorted(missing)}")

        mapping: dict[str, dict[str, str]] = {}
        for row in reader:
            worker_id = row.get("worker_id", "").strip()
            api_key = row.get("api_key", "").strip()
            if not worker_id or not api_key:
                continue
            mapping[worker_id] = row

    if not mapping:
        raise RuntimeError(f"No worker API keys found in {csv_path}")

    return mapping


def guess_file_timestamps(path: Path) -> tuple[str, str]:
    stat = path.stat()
    created = (
        datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_mtime), tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    modified = (
        datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    return created, modified


def get_or_create_session(
    sessions: dict[str, requests.Session], server_url: str, worker_id: str, api_key: str
) -> requests.Session:
    session = sessions.get(worker_id)
    if session:
        return session

    session = requests.Session()
    session.headers.update({"x-api-key": api_key, "Accept": "application/json"})
    sessions[worker_id] = session
    log.info("Initialized upload session for worker %s", worker_id)
    return session


def upload_one(session: requests.Session, server_url: str, image_path: Path, filename: str) -> dict:
    file_created_at, file_modified_at = guess_file_timestamps(image_path)
    with image_path.open("rb") as handle:
        response = session.post(
            server_url.rstrip("/") + "/api/assets",
            data={
                "fileCreatedAt": file_created_at,
                "fileModifiedAt": file_modified_at,
                "filename": filename,
            },
            files={"assetData": (filename, handle, "application/octet-stream")},
            timeout=120,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Upload failed for {image_path} with {response.status_code}: {response.text}")
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Upload manifest images for split=production_new_user to Immich")
    parser.add_argument("--manifest-csv", required=True, help="Path to the manifest CSV")
    parser.add_argument("--images-root", required=True, help="Base directory containing the images referenced by image_path")
    parser.add_argument("--server-url", default="http://localhost:2283", help="Immich base URL")
    parser.add_argument(
        "--worker-api-keys-csv",
        required=True,
        help="CSV with worker_id and api_key columns, typically worker_api_keys.csv",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of images to upload")
    parser.add_argument("--dry-run", action="store_true", help="Only print the files that would be uploaded")
    args = parser.parse_args()

    manifest_csv = Path(args.manifest_csv)
    images_root = Path(args.images_root)
    worker_api_keys = read_worker_api_keys(Path(args.worker_api_keys_csv))
    sessions: dict[str, requests.Session] = {}

    uploaded = 0
    skipped = 0
    missing_workers = 0

    for row in iter_production_new_user_rows(manifest_csv):
        worker_id = row.get("worker_id", "").strip()
        worker_entry = worker_api_keys.get(worker_id)
        if not worker_entry:
            log.warning("Missing API key mapping for worker %s", worker_id or "<empty>")
            missing_workers += 1
            continue

        relative_path = Path(row["image_path"])
        image_path = images_root / relative_path.name
        if not image_path.exists():
            image_path = images_root / relative_path

        if not image_path.exists():
            log.warning("Missing file for %s", row["image_path"])
            skipped += 1
            continue

        if args.dry_run:
            log.info("Would upload %s for worker %s", image_path, worker_id)
        else:
            session = get_or_create_session(sessions, args.server_url, worker_id, worker_entry["api_key"])
            result = upload_one(session, args.server_url, image_path, row["image_name"])
            log.info(
                "Uploaded %s for worker %s -> asset %s (%s)",
                image_path.name,
                worker_id,
                result.get("id"),
                result.get("status"),
            )
        uploaded += 1

        if args.limit and uploaded >= args.limit:
            break

    log.info(
        "Finished. uploaded=%s skipped=%s missing_workers=%s dry_run=%s",
        uploaded,
        skipped,
        missing_workers,
        args.dry_run,
    )


if __name__ == "__main__":
    main()

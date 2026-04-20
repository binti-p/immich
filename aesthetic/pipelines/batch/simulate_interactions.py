from __future__ import annotations

import argparse
import csv
import logging
import os
import random
from collections import Counter
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


PG_HOST = os.environ.get("POSTGRES_HOST", "immich_postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "immich")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "postgres")

DEFAULT_SERVER_URL = os.environ.get("IMMICH_SERVER_URL", "http://immich_server:2283")
DEFAULT_API_KEY = os.environ.get("IMMICH_API_KEY", "")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


class ImmichInteractionSeeder:
    def __init__(self, server_url: str, api_key: str, user_id: str):
        self.base_url = server_url.rstrip("/") + "/api"
        self.user_id = user_id
        self.session = requests.Session()
        self.session.headers.update(
            {
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.created_album_ids: list[str] = []
        self.created_shared_link_ids: list[str] = []
        self.counts: Counter[str] = Counter()

    def request(self, method: str, path: str, expected_status: tuple[int, ...] = (200,), **kwargs):
        response = self.session.request(method, self.base_url + path, timeout=60, **kwargs)
        if response.status_code not in expected_status:
            raise RuntimeError(f"{method} {path} failed with {response.status_code}: {response.text}")
        if response.content and "application/json" in response.headers.get("content-type", ""):
            return response.json()
        return response

    def create_album(self, album_name: str) -> str:
        payload = {"albumName": album_name}
        data = self.request("POST", "/albums", expected_status=(200, 201), json=payload)
        album_id = data["id"]
        self.created_album_ids.append(album_id)
        return album_id

    def add_assets_to_album(self, album_id: str, asset_ids: list[str]) -> None:
        self.request("PUT", f"/albums/{album_id}/assets", expected_status=(200, 201), json={"ids": asset_ids})
        self.counts["album_add"] += len(asset_ids)

    def create_shared_link(self, asset_ids: list[str]) -> str:
        payload = {"type": "INDIVIDUAL", "assetIds": asset_ids, "allowDownload": True, "showMetadata": True}
        data = self.request("POST", "/shared-links", expected_status=(200, 201), json=payload)
        shared_link_id = data["id"]
        self.created_shared_link_ids.append(shared_link_id)
        self.counts["share"] += len(asset_ids)
        return shared_link_id

    def bulk_update_assets(self, asset_ids: list[str], **payload) -> None:
        body = {"ids": asset_ids, **payload}
        self.request("PUT", "/assets", expected_status=(204,), json=body)

    def favorite_assets(self, asset_ids: list[str]) -> None:
        self.bulk_update_assets(asset_ids, isFavorite=True)
        self.counts["favorite"] += len(asset_ids)

    def unfavorite_assets(self, asset_ids: list[str]) -> None:
        self.bulk_update_assets(asset_ids, isFavorite=False)
        self.counts["unfavorite"] += len(asset_ids)

    def archive_assets(self, asset_ids: list[str]) -> None:
        self.bulk_update_assets(asset_ids, visibility="archive")
        self.counts["archive"] += len(asset_ids)

    def restore_assets(self, asset_ids: list[str]) -> None:
        self.bulk_update_assets(asset_ids, visibility="timeline")

    def download_asset(self, asset_id: str) -> None:
        response = self.request("GET", f"/assets/{asset_id}/original", expected_status=(200, 206), stream=True)
        try:
            for _ in response.iter_content(chunk_size=8192):
                break
        finally:
            response.close()
        self.counts["download"] += 1

    def cleanup(self) -> None:
        for shared_link_id in self.created_shared_link_ids:
            try:
                self.request("DELETE", f"/shared-links/{shared_link_id}", expected_status=(204,))
            except Exception as error:
                log.warning("Failed to delete temporary shared link %s: %s", shared_link_id, error)
        for album_id in self.created_album_ids:
            try:
                self.request("DELETE", f"/albums/{album_id}", expected_status=(204,))
            except Exception as error:
                log.warning("Failed to delete temporary album %s: %s", album_id, error)


def read_user_mappings(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        required = {"user_id", "api_key"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Worker API keys CSV is missing required columns: {sorted(missing)}")

        mappings: list[dict[str, str]] = []
        seen = set()
        for row in reader:
            user_id = row.get("user_id", "").strip()
            api_key = row.get("api_key", "").strip()
            if not user_id or not api_key or user_id in seen:
                continue
            seen.add(user_id)
            mappings.append(
                {
                    "worker_id": row.get("worker_id", "").strip(),
                    "user_id": user_id,
                    "api_key": api_key,
                }
            )
        return mappings


def load_candidate_assets(user_id: str, limit: int) -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text
                FROM asset
                WHERE "ownerId" = %s::uuid
                  AND "deletedAt" IS NULL
                  AND visibility = 'timeline'
                ORDER BY "fileCreatedAt" DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
            rows = cur.fetchall()
    return [row["id"] for row in rows]


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def run_for_user(
    user_id: str,
    api_key: str,
    server_url: str,
    asset_limit: int,
    cycles: int,
    batch_size: int,
    seed: int,
    keep_artifacts: bool,
    label: str,
) -> Counter[str]:
    asset_ids = load_candidate_assets(user_id, asset_limit)
    if len(asset_ids) < max(3, batch_size):
        raise RuntimeError(f"Need at least {max(3, batch_size)} eligible assets for {label}, found {len(asset_ids)}")

    random.shuffle(asset_ids)
    batches = chunked(asset_ids, batch_size)
    seeder = ImmichInteractionSeeder(server_url, api_key, user_id)
    album_id = seeder.create_album(f"Aesthetic Retraining Seed {seed} {label}")

    try:
        for cycle in range(cycles):
            batch = batches[cycle % len(batches)]
            favorite_batch = batch[: max(1, len(batch) // 2)]
            unfavorite_batch = favorite_batch[: max(1, len(favorite_batch) // 2)]
            archive_batch = batch[max(1, len(batch) // 2) : max(2, len(batch) // 2 + max(1, len(batch) // 4))]
            share_batch = batch[-max(1, len(batch) // 3) :]
            download_batch = random.sample(batch, k=min(3, len(batch)))

            log.info("[%s] Cycle %s using %s assets", label, cycle + 1, len(batch))
            seeder.favorite_assets(favorite_batch)
            if unfavorite_batch:
                seeder.unfavorite_assets(unfavorite_batch)
            if archive_batch:
                seeder.archive_assets(archive_batch)
                seeder.restore_assets(archive_batch)
            seeder.add_assets_to_album(album_id, batch)
            seeder.create_shared_link(share_batch)
            for asset_id in download_batch:
                seeder.download_asset(asset_id)

        log.info("[%s] Generated interaction counts: %s", label, dict(seeder.counts))
        return seeder.counts
    finally:
        if not keep_artifacts:
            seeder.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Generate realistic Immich interaction events through the public API")
    parser.add_argument("--user-id", help="Owner user ID whose assets should be used")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="Immich API key for the same user")
    parser.add_argument(
        "--worker-api-keys-csv",
        default=None,
        help="Optional CSV with worker_id,user_id,api_key to seed multiple users in one run",
    )
    parser.add_argument("--server-url", default=DEFAULT_SERVER_URL, help="Immich server base URL")
    parser.add_argument("--asset-limit", type=int, default=60, help="Maximum number of candidate assets to sample")
    parser.add_argument("--cycles", type=int, default=3, help="How many interaction cycles to run")
    parser.add_argument("--batch-size", type=int, default=10, help="Assets per album/share/favorite operation")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatability")
    parser.add_argument("--keep-artifacts", action="store_true", help="Keep the temporary album and shared links")
    args = parser.parse_args()

    random.seed(args.seed)

    if args.worker_api_keys_csv:
        aggregate_counts: Counter[str] = Counter()
        user_mappings = read_user_mappings(Path(args.worker_api_keys_csv))
        if not user_mappings:
            raise RuntimeError("No users found in worker API keys CSV")

        successful_users = 0
        skipped_users = 0
        for index, mapping in enumerate(user_mappings):
            label = mapping["worker_id"] or mapping["user_id"]
            try:
                counts = run_for_user(
                    user_id=mapping["user_id"],
                    api_key=mapping["api_key"],
                    server_url=args.server_url,
                    asset_limit=args.asset_limit,
                    cycles=args.cycles,
                    batch_size=args.batch_size,
                    seed=args.seed + index,
                    keep_artifacts=args.keep_artifacts,
                    label=label,
                )
                aggregate_counts.update(counts)
                successful_users += 1
            except RuntimeError as error:
                skipped_users += 1
                log.warning("[%s] Skipping user: %s", label, error)

        log.info(
            "Finished multi-user seeding. successful_users=%s skipped_users=%s counts=%s",
            successful_users,
            skipped_users,
            dict(aggregate_counts),
        )
        return

    if not args.user_id:
        raise RuntimeError("Pass --user-id for single-user mode or --worker-api-keys-csv for multi-user mode.")
    if not args.api_key:
        raise RuntimeError("An API key is required. Pass --api-key or set IMMICH_API_KEY.")

    counts = run_for_user(
        user_id=args.user_id,
        api_key=args.api_key,
        server_url=args.server_url,
        asset_limit=args.asset_limit,
        cycles=args.cycles,
        batch_size=args.batch_size,
        seed=args.seed,
        keep_artifacts=args.keep_artifacts,
        label=args.user_id,
    )
    log.info("Finished single-user seeding. counts=%s", dict(counts))


if __name__ == "__main__":
    main()

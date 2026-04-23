#!/usr/bin/env python3
"""
Backfill aesthetic scoring for assets that already have CLIP embeddings.

Finds smart_search rows whose owner asset is missing an aesthetic_scores row or
any inference_log row, then calls aesthetic-service /score-image.
"""
import argparse
import asyncio
import logging
import os
from dataclasses import dataclass

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackfillCandidate:
    asset_id: str
    user_id: str
    missing_score: bool
    missing_inference_log: bool


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.environ.get("POSTGRES_HOST", "immich-postgres"),
        port=env_int("POSTGRES_PORT", 5432),
        database=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "immich"),
        password=os.environ.get("POSTGRES_PASSWORD", "immich"),
        min_size=1,
        max_size=4,
    )


async def find_candidates(pool: asyncpg.Pool, limit: int) -> list[BackfillCandidate]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH logged AS (
                SELECT DISTINCT "assetId", "userId"
                FROM inference_log
            )
            SELECT
                ss."assetId"::text AS asset_id,
                a."ownerId"::text AS user_id,
                (scores."assetId" IS NULL) AS missing_score,
                (logged."assetId" IS NULL) AS missing_inference_log
            FROM smart_search ss
            JOIN asset a ON a.id = ss."assetId"
            LEFT JOIN aesthetic_scores scores
                ON scores."assetId" = ss."assetId"
               AND scores."userId" = a."ownerId"
            LEFT JOIN logged
                ON logged."assetId" = ss."assetId"
               AND logged."userId" = a."ownerId"
            WHERE a."deletedAt" IS NULL
              AND (
                    scores."assetId" IS NULL
                 OR logged."assetId" IS NULL
              )
            ORDER BY a."createdAt" DESC
            LIMIT $1
            """,
            limit,
        )

    return [
        BackfillCandidate(
            asset_id=row["asset_id"],
            user_id=row["user_id"],
            missing_score=row["missing_score"],
            missing_inference_log=row["missing_inference_log"],
        )
        for row in rows
    ]


async def score_candidate(
    client: httpx.AsyncClient,
    service_url: str,
    candidate: BackfillCandidate,
    dry_run: bool,
) -> bool:
    reason = []
    if candidate.missing_score:
        reason.append("missing_score")
    if candidate.missing_inference_log:
        reason.append("missing_inference_log")
    reason_text = ",".join(reason) or "unknown"

    if dry_run:
        log.info(
            "[dry-run] Would score asset=%s user=%s reason=%s",
            candidate.asset_id,
            candidate.user_id,
            reason_text,
        )
        return True

    try:
        response = await client.post(
            f"{service_url.rstrip('/')}/score-image",
            json={"asset_id": candidate.asset_id, "user_id": candidate.user_id},
        )
    except Exception as exc:
        log.warning("Scoring request failed asset=%s error=%s", candidate.asset_id, exc)
        return False

    if response.is_success:
        log.info("Scored asset=%s user=%s reason=%s", candidate.asset_id, candidate.user_id, reason_text)
        return True

    log.warning(
        "Scoring returned HTTP %s asset=%s body=%s",
        response.status_code,
        candidate.asset_id,
        response.text[:500],
    )
    return False


async def run(args: argparse.Namespace) -> int:
    service_url = os.environ.get(
        "AESTHETIC_SERVICE_URL",
        "http://aesthetic-service.aesthetic-hub.svc.cluster.local:8000",
    )

    pool = await create_pool()
    try:
        candidates = await find_candidates(pool, args.limit)
    finally:
        await pool.close()

    if not candidates:
        log.info("No backfill candidates found")
        return 0

    log.info("Found %d backfill candidates", len(candidates))

    success_count = 0
    failure_count = 0
    timeout = httpx.Timeout(args.timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for candidate in candidates:
            ok = await score_candidate(client, service_url, candidate, args.dry_run)
            if ok:
                success_count += 1
            else:
                failure_count += 1
            if args.sleep_seconds > 0:
                await asyncio.sleep(args.sleep_seconds)

    log.info(
        "Backfill complete candidates=%d successful=%d failed=%d dry_run=%s",
        len(candidates),
        success_count,
        failure_count,
        args.dry_run,
    )
    return 1 if failure_count and args.fail_on_error else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing aesthetic scoring rows")
    parser.add_argument("--limit", type=int, default=env_int("BACKFILL_LIMIT", 200))
    parser.add_argument("--sleep-seconds", type=float, default=float(os.environ.get("BACKFILL_SLEEP_SECONDS", "0.05")))
    parser.add_argument("--timeout-seconds", type=float, default=float(os.environ.get("BACKFILL_TIMEOUT_SECONDS", "120")))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fail-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    return asyncio.run(run(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

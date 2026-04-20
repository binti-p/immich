#!/usr/bin/env python3
"""
Retry failed aesthetic scoring attempts.

This script queries the inference_log table for failed attempts and retries them
by calling the aesthetic service's score-image endpoint.

Usage:
    python retry_failed_scoring.py [--status STATUS] [--limit LIMIT] [--dry-run]

Options:
    --status STATUS     Only retry specific status (default: failed_clip_missing)
    --limit LIMIT       Maximum number of assets to retry (default: 100)
    --dry-run          Show what would be retried without actually retrying
"""
import argparse
import asyncio
import logging
import os
from typing import List, Tuple

import asyncpg
import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_failed_attempts(
    pool: asyncpg.Pool,
    status: str,
    limit: int
) -> List[Tuple[str, str, str]]:
    """
    Query failed scoring attempts from inference_log.
    
    Returns list of (asset_id, user_id, error_message) tuples.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON ("assetId", "userId")
                "assetId", "userId", "errorMessage"
            FROM inference_log
            WHERE status = $1
            ORDER BY "assetId", "userId", "requestReceivedAt" DESC
            LIMIT $2
            """,
            status,
            limit,
        )
    
    return [(row['assetId'], row['userId'], row['errorMessage']) for row in rows]


async def retry_scoring(
    asset_id: str,
    user_id: str,
    service_url: str,
    dry_run: bool = False
) -> bool:
    """
    Retry scoring for a single asset.
    
    Returns True if successful, False otherwise.
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would retry scoring for asset {asset_id}, user {user_id}")
        return True
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{service_url}/score-image",
                json={
                    "asset_id": asset_id,
                    "user_id": user_id,
                }
            )
            
            if response.is_success:
                logger.info(f"✓ Successfully scored asset {asset_id}")
                return True
            else:
                body = await response.aread()
                logger.warning(
                    f"✗ Scoring failed for asset {asset_id}: "
                    f"HTTP {response.status_code} - {body.decode()}"
                )
                return False
                
    except Exception as e:
        logger.error(f"✗ Error scoring asset {asset_id}: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(
        description="Retry failed aesthetic scoring attempts"
    )
    parser.add_argument(
        '--status',
        default='failed_clip_missing',
        help='Status to retry (default: failed_clip_missing)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of assets to retry (default: 100)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be retried without actually retrying'
    )
    args = parser.parse_args()
    
    # Database connection
    pool = await asyncpg.create_pool(
        host=os.environ.get("POSTGRES_HOST", "immich_postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "immich"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        min_size=2,
        max_size=10,
    )
    
    service_url = os.environ.get(
        "AESTHETIC_SERVICE_URL",
        "http://aesthetic_service:8000"
    )
    
    logger.info(f"Querying failed attempts with status={args.status}, limit={args.limit}")
    
    failed_attempts = await get_failed_attempts(pool, args.status, args.limit)
    
    if not failed_attempts:
        logger.info("No failed attempts found")
        await pool.close()
        return
    
    logger.info(f"Found {len(failed_attempts)} failed attempts to retry")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual retries will be performed")
    
    success_count = 0
    fail_count = 0
    
    for asset_id, user_id, error_msg in failed_attempts:
        logger.info(f"Retrying asset {asset_id} (previous error: {error_msg})")
        
        success = await retry_scoring(asset_id, user_id, service_url, args.dry_run)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
        
        # Rate limit to avoid overwhelming the service
        await asyncio.sleep(0.5)
    
    logger.info(
        f"\n=== Retry Summary ===\n"
        f"Total attempts: {len(failed_attempts)}\n"
        f"Successful: {success_count}\n"
        f"Failed: {fail_count}\n"
    )
    
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Bulk Scrape Pending URLs

Purpose:
    Scrapes all URLs in scrape_registry that have no scraped_text.
    Runs continuously, picking up new URLs as they're added by
    the discovery sweep. Exits when no new URLs appear for
    a configurable idle period.

    Designed to run in parallel with discovery — trails by a few
    minutes, so by the time discovery finishes, scraping is nearly done.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)

Usage:
    # Run alongside discovery sweep
    python scripts/bulk_scrape_pending.py 2>&1 | tee logs/bulk_scrape.log

    # Limit to recent URLs only
    python scripts/bulk_scrape_pending.py --since "2026-03-31 17:15:00"

    # Dry run
    python scripts/bulk_scrape_pending.py --dry-run

Notes:
    - HTTP-only scraping (ScrapeAgent with max_depth=0)
    - Updates scrape_registry.scraped_text in place
    - Loops until no pending URLs remain for --idle-timeout seconds
    - Safe to interrupt — each URL is independent
    - Runs ~200-400 URLs/hr depending on response times
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


def get_pending_urls(since: str | None = None, limit: int = 500) -> list[dict]:
    """Get URLs with no scraped text."""
    schema = settings.utility_schema
    since_filter = ""
    params = {"src": "serper", "limit": limit}
    if since:
        since_filter = "AND sr.created_at >= :since"
        params["since"] = since

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id, sr.pwsid, sr.url, sr.content_type, sr.discovery_rank
            FROM {schema}.scrape_registry sr
            WHERE sr.url_source = :src
              AND (sr.scraped_text IS NULL OR LENGTH(sr.scraped_text) < 100)
              AND sr.status != 'dead'
              {since_filter}
            ORDER BY sr.discovery_rank ASC, sr.id ASC
            LIMIT :limit
        """), params).fetchall()

    return [dict(r._mapping) for r in rows]


def scrape_one(registry_id: int, url: str, pwsid: str) -> bool:
    """Scrape a single URL via ScrapeAgent.

    ScrapeAgent.run(registry_id=...) handles fetch, text extraction,
    and scrape_registry update (scraped_text, content_type, status).

    Returns True if text was retrieved, False otherwise.
    """
    from utility_api.agents.scrape import ScrapeAgent

    try:
        agent = ScrapeAgent()
        result = agent.run(registry_id=registry_id, max_depth=0)
        return result.get("succeeded", 0) > 0
    except Exception as e:
        logger.debug(f"  {pwsid} scrape failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Scrape all pending URLs in scrape_registry"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--since", default=None,
                        help="Only scrape URLs created after this timestamp")
    parser.add_argument("--idle-timeout", type=int, default=600,
                        help="Exit after N seconds with no new URLs (default: 600)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Bulk Scrape — Pending URLs")
    logger.info(f"Since: {args.since or 'all'}")
    logger.info("=" * 60)

    total_scraped = 0
    total_failed = 0
    total_processed = 0
    started = datetime.now(timezone.utc)
    last_work_at = time.time()

    while True:
        batch = get_pending_urls(since=args.since, limit=500)

        if not batch:
            idle_seconds = time.time() - last_work_at
            if idle_seconds > args.idle_timeout:
                logger.info(f"\nNo pending URLs for {args.idle_timeout}s. Exiting.")
                break
            logger.info(f"  No pending URLs. Waiting 30s (idle {idle_seconds:.0f}/{args.idle_timeout}s)...")
            time.sleep(30)
            continue

        last_work_at = time.time()

        if args.dry_run:
            logger.info(f"DRY RUN — {len(batch)} pending URLs")
            for u in batch[:10]:
                logger.info(f"  {u['pwsid']} rank={u['discovery_rank']} {u['url'][:60]}")
            return

        for i, u in enumerate(batch):
            success = scrape_one(u["id"], u["url"], u["pwsid"])
            total_processed += 1
            if success:
                total_scraped += 1
            else:
                total_failed += 1

            if total_processed % 100 == 0:
                elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                rate = total_processed / elapsed * 3600 if elapsed > 0 else 0
                logger.info(
                    f"  Progress: {total_processed:,} processed, "
                    f"{total_scraped:,} scraped, {total_failed:,} failed, "
                    f"{rate:.0f}/hr"
                )

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Bulk Scrape Complete")
    logger.info(f"  Processed: {total_processed:,}")
    logger.info(f"  Scraped:   {total_scraped:,} ({total_scraped/max(total_processed,1)*100:.0f}%)")
    logger.info(f"  Failed:    {total_failed:,}")
    logger.info(f"  Time:      {elapsed/3600:.1f} hours")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()

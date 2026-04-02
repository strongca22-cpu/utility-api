#!/usr/bin/env python3
"""
Parallel Bulk Scraper

Purpose:
    Runs N parallel workers to scrape pending URLs from scrape_registry.
    Each worker is pre-assigned a partition of URLs via modulo on registry ID
    (sr.id % N = worker_id), guaranteeing zero overlap between workers.

    Replaces the single-threaded bulk_scrape_pending.py for large scrape jobs.

Author: AI-Generated
Created: 2026-04-01
Modified: 2026-04-01

Dependencies:
    - utility_api.agents.scrape (ScrapeAgent)
    - sqlalchemy
    - loguru
    - multiprocessing

Usage:
    # 10 workers, rank 1 only
    python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --since "2026-03-31 17:15:00"

    # 8 workers, all ranks
    python scripts/bulk_scrape_parallel.py --workers 8

    # Dry run
    python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --dry-run

Notes:
    - Workers partition by sr.id % N = worker_id (zero overlap)
    - Each worker logs to logs/scrape_worker_{id}.log
    - Summary printed when all workers finish
    - Safe to Ctrl+C — workers terminate gracefully
"""

import argparse
import multiprocessing
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from loguru import logger


def worker_main(
    worker_id: int,
    total_workers: int,
    since: str | None,
    rank: int | None,
    rank_min: int | None,
    idle_timeout: int,
    any_source: bool = False,
) -> dict:
    """Single worker process — scrapes its partition of pending URLs.

    Parameters
    ----------
    worker_id : int
        This worker's ID (0 to total_workers-1).
    total_workers : int
        Total number of workers (for modulo partitioning).
    since, rank, rank_min : filters passed through to query.
    idle_timeout : int
        Exit after this many seconds with no work.

    Returns
    -------
    dict
        {worker_id, processed, scraped, failed, elapsed_s}
    """
    # Configure per-worker logging
    logger.remove()
    log_path = PROJECT_ROOT / "logs" / f"scrape_worker_{worker_id}.log"
    logger.add(log_path, format="{time:HH:mm:ss} | {message}", rotation="50 MB")
    logger.add(
        sys.stderr,
        format=f"[W{worker_id}] {{time:HH:mm:ss}} | {{message}}",
        level="INFO",
    )

    # Import DB inside worker (each process needs its own engine)
    from sqlalchemy import text
    from utility_api.config import settings
    from utility_api.db import engine as _engine

    # Create a fresh engine for this worker process
    from sqlalchemy import create_engine
    engine = create_engine(str(settings.database_url), pool_size=2)
    schema = settings.utility_schema

    def get_batch(limit: int = 100) -> list[dict]:
        """Get pending URLs for this worker's partition."""
        params = {
            "limit": limit,
            "total": total_workers,
            "worker": worker_id,
        }

        source_filter = ""
        if not any_source:
            source_filter = "AND sr.url_source = :src"
            params["src"] = "serper"

        since_filter = ""
        if since:
            since_filter = "AND sr.created_at >= :since"
            params["since"] = since

        rank_filter = ""
        if rank is not None:
            rank_filter = "AND sr.discovery_rank = :rank"
            params["rank"] = rank
        elif rank_min is not None:
            rank_filter = "AND sr.discovery_rank >= :rank_min"
            params["rank_min"] = rank_min

        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url, sr.content_type, sr.discovery_rank
                FROM {schema}.scrape_registry sr
                WHERE (sr.scraped_text IS NULL OR LENGTH(sr.scraped_text) < 100)
                  AND sr.status != 'dead'
                  {source_filter}
                  AND MOD(sr.id, :total) = :worker
                  {since_filter}
                  {rank_filter}
                ORDER BY sr.id ASC
                LIMIT :limit
            """), params).fetchall()

        return [dict(r._mapping) for r in rows]

    def scrape_one(registry_id: int, url: str, pwsid: str) -> bool:
        """Scrape a single URL."""
        from utility_api.agents.scrape import ScrapeAgent
        try:
            agent = ScrapeAgent()
            result = agent.run(registry_id=registry_id, max_depth=0)
            return result.get("succeeded", 0) > 0
        except Exception as e:
            logger.debug(f"  {pwsid} scrape failed: {e}")
            return False

    # Main loop
    total_scraped = 0
    total_failed = 0
    total_processed = 0
    started = time.time()
    last_work_at = time.time()

    logger.info(f"Worker {worker_id}/{total_workers} starting (partition: id%{total_workers}={worker_id})")

    while True:
        batch = get_batch(limit=100)

        if not batch:
            idle_seconds = time.time() - last_work_at
            if idle_seconds > idle_timeout:
                logger.info(f"No pending URLs for {idle_timeout}s. Exiting.")
                break
            time.sleep(10)
            continue

        last_work_at = time.time()

        for u in batch:
            success = scrape_one(u["id"], u["url"], u["pwsid"])
            total_processed += 1
            if success:
                total_scraped += 1
            else:
                total_failed += 1

            if total_processed % 50 == 0:
                elapsed = time.time() - started
                rate = total_processed / elapsed * 3600 if elapsed > 0 else 0
                logger.info(
                    f"Progress: {total_processed:,} done, "
                    f"{total_scraped:,} scraped, {total_failed:,} failed, "
                    f"{rate:.0f}/hr"
                )

    elapsed = time.time() - started
    logger.info(
        f"Worker {worker_id} done: {total_processed:,} processed, "
        f"{total_scraped:,} scraped, {total_failed:,} failed, "
        f"{elapsed/60:.1f} min"
    )

    engine.dispose()

    return {
        "worker_id": worker_id,
        "processed": total_processed,
        "scraped": total_scraped,
        "failed": total_failed,
        "elapsed_s": int(elapsed),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Parallel bulk scraper — N workers with modulo partitioning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --since "2026-03-31 17:15:00"
  python scripts/bulk_scrape_parallel.py --workers 8 --rank 1 --dry-run
""",
    )
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of parallel workers (default: 10)")
    parser.add_argument("--since", default=None,
                        help="Only scrape URLs created after this timestamp")
    parser.add_argument("--rank", type=int, default=None,
                        help="Scrape only this exact discovery_rank")
    parser.add_argument("--rank-min", type=int, default=None,
                        help="Scrape ranks >= this value")
    parser.add_argument("--idle-timeout", type=int, default=120,
                        help="Per-worker idle timeout in seconds (default: 120)")
    parser.add_argument("--any-source", action="store_true",
                        help="Scrape all url_sources, not just serper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show partition sizes, no scraping")

    args = parser.parse_args()

    print("=" * 60)
    print(f"Parallel Bulk Scraper — {args.workers} workers")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    if args.dry_run:
        # Show how many URLs each worker would get
        from sqlalchemy import text
        from utility_api.config import settings
        from utility_api.db import engine

        schema = settings.utility_schema
        params = {}

        source_filter = ""
        if not args.any_source:
            source_filter = "AND sr.url_source = :src"
            params["src"] = "serper"

        since_filter = ""
        if args.since:
            since_filter = "AND sr.created_at >= :since"
            params["since"] = args.since

        rank_filter = ""
        if args.rank is not None:
            rank_filter = "AND sr.discovery_rank = :rank"
            params["rank"] = args.rank
        elif args.rank_min is not None:
            rank_filter = "AND sr.discovery_rank >= :rank_min"
            params["rank_min"] = args.rank_min

        with engine.connect() as conn:
            for w in range(args.workers):
                p = {**params, "total": args.workers, "worker": w}
                r = conn.execute(text(f"""
                    SELECT count(*) as cnt
                    FROM {schema}.scrape_registry sr
                    WHERE (sr.scraped_text IS NULL OR LENGTH(sr.scraped_text) < 100)
                      AND sr.status != 'dead'
                      {source_filter}
                      AND MOD(sr.id, :total) = :worker
                      {since_filter}
                      {rank_filter}
                """), p).fetchone()
                print(f"  Worker {w}: {r.cnt:,} pending URLs")

        total = sum(1 for _ in range(args.workers))  # just for format
        print(f"\n[DRY RUN] Would launch {args.workers} workers. Exiting.")
        return

    # Launch workers
    started = time.time()
    pool = multiprocessing.Pool(processes=args.workers)

    try:
        results = pool.starmap(
            worker_main,
            [
                (w, args.workers, args.since, args.rank, args.rank_min, args.idle_timeout, args.any_source)
                for w in range(args.workers)
            ],
        )
    except KeyboardInterrupt:
        print("\nCtrl+C — terminating workers...")
        pool.terminate()
        pool.join()
        return
    else:
        pool.close()
        pool.join()

    elapsed = time.time() - started

    # Summary
    print("\n" + "=" * 60)
    print("PARALLEL SCRAPE COMPLETE")
    print("=" * 60)

    total_processed = 0
    total_scraped = 0
    total_failed = 0

    for r in results:
        print(
            f"  Worker {r['worker_id']}: "
            f"{r['processed']:,} processed, "
            f"{r['scraped']:,} scraped, "
            f"{r['failed']:,} failed, "
            f"{r['elapsed_s']//60}m"
        )
        total_processed += r["processed"]
        total_scraped += r["scraped"]
        total_failed += r["failed"]

    print(f"\n  TOTAL: {total_processed:,} processed, "
          f"{total_scraped:,} scraped, {total_failed:,} failed")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Throughput: {total_processed/elapsed*3600:.0f}/hr "
          f"(vs ~2,000/hr single-threaded)")
    print("=" * 60)


if __name__ == "__main__":
    main()

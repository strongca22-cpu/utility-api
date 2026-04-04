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
Modified: 2026-04-03

Dependencies:
    - utility_api.agents.scrape (ScrapeAgent)
    - sqlalchemy
    - loguru
    - multiprocessing

Usage:
    # 10 workers, rank 1 only (single node)
    python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --since "2026-03-31 17:15:00"

    # 8 workers, all ranks
    python scripts/bulk_scrape_parallel.py --workers 8

    # Multi-node: desktop (IDs 0-19) + VPS (IDs 20-23) = 24 total
    python scripts/bulk_scrape_parallel.py --workers 24 --worker-start 0 --worker-end 19
    python scripts/bulk_scrape_parallel.py --workers 24 --worker-start 20 --worker-end 23

    # Dry run
    python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --dry-run

Notes:
    - Workers partition by sr.id % N = worker_id (zero overlap)
    - --worker-start/--worker-end allow multi-node setups (each node spawns a subset)
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
        else:
            # Exclude domain_guess/domain_guesser — speculative URL guesses,
            # not search results. These have very low yield and waste scrape time.
            source_filter = "AND sr.url_source NOT IN ('domain_guess', 'domain_guesser')"

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
  # Single node (backward compatible)
  python scripts/bulk_scrape_parallel.py --workers 10 --rank 1 --since "2026-03-31 17:15:00"

  # Multi-node: desktop runs IDs 0-19, VPS runs IDs 20-23, 24 total
  python scripts/bulk_scrape_parallel.py --workers 24 --worker-start 0 --worker-end 19
  python scripts/bulk_scrape_parallel.py --workers 24 --worker-start 20 --worker-end 23

  # Dry run
  python scripts/bulk_scrape_parallel.py --workers 24 --worker-start 20 --worker-end 23 --dry-run
""",
    )
    parser.add_argument("--workers", type=int, default=10,
                        help="Total number of workers across all nodes (default: 10)")
    parser.add_argument("--worker-start", type=int, default=None,
                        help="First worker ID to spawn on this node (default: 0)")
    parser.add_argument("--worker-end", type=int, default=None,
                        help="Last worker ID to spawn on this node (default: --workers-1)")
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

    # Multi-node partitioning: default to spawning all workers locally
    if args.worker_start is None:
        args.worker_start = 0
    if args.worker_end is None:
        args.worker_end = args.workers - 1
    if args.worker_start < 0 or args.worker_end >= args.workers:
        parser.error(f"worker-start/end must be in [0, {args.workers - 1}]")
    if args.worker_start > args.worker_end:
        parser.error("worker-start must be <= worker-end")
    local_count = args.worker_end - args.worker_start + 1

    print("=" * 60)
    print(f"Parallel Bulk Scraper — {local_count} local workers "
          f"(IDs {args.worker_start}-{args.worker_end} of {args.workers} total)")
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
        else:
            source_filter = "AND sr.url_source NOT IN ('domain_guess', 'domain_guesser')"

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
            total_pending = 0
            for w in range(args.worker_start, args.worker_end + 1):
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
                total_pending += r.cnt

        print(f"\n  Total pending for this node: {total_pending:,}")
        print(f"\n[DRY RUN] Would launch {local_count} workers "
              f"(IDs {args.worker_start}-{args.worker_end} of {args.workers} global). Exiting.")
        return

    # Launch workers
    started = time.time()
    pool = multiprocessing.Pool(processes=local_count)

    try:
        results = pool.starmap(
            worker_main,
            [
                (w, args.workers, args.since, args.rank, args.rank_min, args.idle_timeout, args.any_source)
                for w in range(args.worker_start, args.worker_end + 1)
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

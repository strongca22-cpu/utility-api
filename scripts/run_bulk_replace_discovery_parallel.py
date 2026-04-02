#!/usr/bin/env python3
"""
Parallel Bulk Replace Discovery

Purpose:
    Run Serper discovery for bulk-only PWSIDs using multiple workers.
    Each worker gets a modulo-partitioned slice of the target list.
    ~4x throughput vs serial discovery.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - utility_api.agents.discovery (DiscoveryAgent)
    - multiprocessing

Usage:
    python scripts/run_bulk_replace_discovery_parallel.py --workers 4 --min-pop 3000 --dry-run
    python scripts/run_bulk_replace_discovery_parallel.py --workers 4 --min-pop 3000
"""

import argparse
import multiprocessing
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def get_remaining_targets(min_pop: int = 3000) -> list[dict]:
    """Get bulk-only PWSIDs not yet Serper-searched."""
    from sqlalchemy import text
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT c.pwsid, c.state_code, c.population_served, c.pws_name
            FROM {schema}.cws_boundaries c
            WHERE c.population_served >= :min_pop
              AND EXISTS (
                  SELECT 1 FROM {schema}.rate_schedules rs WHERE rs.pwsid = c.pwsid
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.rate_schedules rs
                  WHERE rs.pwsid = c.pwsid AND rs.source_key = 'scraped_llm'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.scrape_registry sr
                  WHERE sr.pwsid = c.pwsid AND sr.url_source = 'serper'
              )
            ORDER BY c.population_served DESC
        """), {"min_pop": min_pop}).fetchall()

    return [dict(r._mapping) for r in rows]


def discovery_worker(worker_id: int, targets: list[dict]) -> dict:
    """Run discovery for a partition of targets."""
    # Per-worker logging
    logger.remove()
    log_path = PROJECT_ROOT / "logs" / f"discovery_worker_{worker_id}.log"
    logger.add(log_path, format="{time:HH:mm:ss} | {message}", rotation="50 MB")
    logger.add(sys.stderr, format=f"[D{worker_id}] {{time:HH:mm:ss}} | {{message}}", level="INFO")

    from utility_api.agents.discovery import DiscoveryAgent

    agent = DiscoveryAgent()
    searched = 0
    urls_found = 0
    errors = 0
    started = time.time()

    logger.info(f"Worker {worker_id}: {len(targets)} PWSIDs to discover")

    for i, target in enumerate(targets):
        pwsid = target["pwsid"]
        try:
            result = agent.run(pwsid=pwsid, diagnostic=False)
            searched += 1
            urls_found += result.get("urls_written", 0)
        except Exception as e:
            errors += 1
            logger.debug(f"  {pwsid}: error: {e}")

        if (i + 1) % 50 == 0:
            elapsed = time.time() - started
            rate = searched / elapsed * 3600 if elapsed > 0 else 0
            logger.info(
                f"Progress: {i+1}/{len(targets)}, "
                f"{urls_found} URLs, {errors} errors, {rate:.0f}/hr"
            )

    elapsed = time.time() - started
    logger.info(
        f"Worker {worker_id} done: {searched} searched, "
        f"{urls_found} URLs, {errors} errors, {elapsed/60:.1f} min"
    )

    return {
        "worker_id": worker_id,
        "searched": searched,
        "urls_found": urls_found,
        "errors": errors,
        "elapsed_s": int(elapsed),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Parallel Serper discovery for bulk-only PWSIDs")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel workers (default: 4)")
    parser.add_argument("--min-pop", type=int, default=3000,
                        help="Minimum population (default: 3000)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = get_remaining_targets(min_pop=args.min_pop)

    print("=" * 60)
    print(f"Parallel Bulk Replace Discovery — {args.workers} workers")
    print(f"Targets: {len(targets):,} PWSIDs (pop >= {args.min_pop:,})")
    print(f"Serper queries: ~{len(targets) * 4:,}")
    print("=" * 60)

    if not targets:
        print("No targets remaining.")
        return

    if args.dry_run:
        for w in range(args.workers):
            partition = [t for i, t in enumerate(targets) if i % args.workers == w]
            print(f"  Worker {w}: {len(partition)} PWSIDs")
        print(f"\n[DRY RUN] Exiting.")
        return

    # Partition targets by modulo
    partitions = [[] for _ in range(args.workers)]
    for i, t in enumerate(targets):
        partitions[i % args.workers].append(t)

    started = time.time()
    pool = multiprocessing.Pool(processes=args.workers)

    try:
        results = pool.starmap(
            discovery_worker,
            [(w, partitions[w]) for w in range(args.workers)],
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
    total_searched = sum(r["searched"] for r in results)
    total_urls = sum(r["urls_found"] for r in results)
    total_errors = sum(r["errors"] for r in results)

    print("\n" + "=" * 60)
    print("PARALLEL DISCOVERY COMPLETE")
    print("=" * 60)
    for r in results:
        print(f"  Worker {r['worker_id']}: {r['searched']} searched, "
              f"{r['urls_found']} URLs, {r['errors']} errors, {r['elapsed_s']//60}m")
    print(f"\n  TOTAL: {total_searched:,} searched, {total_urls:,} URLs, "
          f"{total_errors:,} errors")
    print(f"  Time: {elapsed/60:.1f} min ({total_searched/elapsed*3600:.0f}/hr)")
    print("=" * 60)


if __name__ == "__main__":
    main()

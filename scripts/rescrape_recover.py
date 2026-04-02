#!/usr/bin/env python3
"""
Re-scrape Recovery: Reset and Re-scrape Bug-Affected Rows

Purpose:
    Takes candidate IDs from rescrape_diagnose.py, snapshots current state,
    resets scraped_text to NULL, re-scrapes with parallel workers using
    the fixed Playwright/form code, and reports before/after results.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - utility_api.agents.scrape (ScrapeAgent)
    - multiprocessing
    - sqlalchemy

Usage:
    python scripts/rescrape_recover.py --candidates candidates.json --workers 20
    python scripts/rescrape_recover.py --state NY --min-pop 3000 --workers 5
    python scripts/rescrape_recover.py --state NY --min-pop 3000 --dry-run
"""

import argparse
import json
import multiprocessing
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def load_candidates(candidates_path: str | None, state: str | None, min_pop: int) -> list[dict]:
    """Load candidate IDs from file or run diagnostics inline."""
    if candidates_path:
        with open(candidates_path) as f:
            return json.load(f)

    # Run inline diagnostics
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from rescrape_diagnose import find_candidates
    candidates = find_candidates(state=state, min_pop=min_pop)
    return [{"id": c["id"], "pwsid": c["pwsid"], "priority": c["priority"]} for c in candidates]


def snapshot_candidates(candidate_ids: list[int]) -> list[dict]:
    """Save pre-rescrape state for comparison."""
    from sqlalchemy import text
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema
    snapshots = []

    with engine.connect() as conn:
        for batch_start in range(0, len(candidate_ids), 500):
            batch = candidate_ids[batch_start:batch_start + 500]
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url,
                       sr.last_content_length,
                       LENGTH(sr.scraped_text) AS text_length
                FROM {schema}.scrape_registry sr
                WHERE sr.id = ANY(:ids)
            """), {"ids": batch}).fetchall()
            for r in rows:
                snapshots.append({
                    "id": r.id,
                    "pwsid": r.pwsid,
                    "url": r.url,
                    "old_content_length": r.last_content_length,
                    "old_text_length": r.text_length or 0,
                })

    return snapshots


def reset_candidates(candidate_ids: list[int], timestamp: str) -> int:
    """Set scraped_text = NULL on candidates so ScrapeAgent re-fetches."""
    from sqlalchemy import text
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema
    total_reset = 0

    with engine.connect() as conn:
        for batch_start in range(0, len(candidate_ids), 500):
            batch = candidate_ids[batch_start:batch_start + 500]
            affected = conn.execute(text(f"""
                UPDATE {schema}.scrape_registry
                SET scraped_text = NULL,
                    notes = COALESCE(notes, '') || ' rescrape_reset={timestamp}'
                WHERE id = ANY(:ids)
            """), {"ids": batch}).rowcount
            total_reset += affected
        conn.commit()

    return total_reset


def worker_main(worker_id: int, candidate_ids: list[int], total_workers: int) -> dict:
    """Single worker — re-scrapes its partition of candidate IDs."""
    logger.remove()
    log_path = PROJECT_ROOT / "logs" / f"rescrape_worker_{worker_id}.log"
    logger.add(log_path, format="{time:HH:mm:ss} | {message}", rotation="50 MB")
    logger.add(sys.stderr, format=f"[R{worker_id}] {{time:HH:mm:ss}} | {{message}}", level="INFO")

    my_ids = [cid for i, cid in enumerate(candidate_ids) if i % total_workers == worker_id]
    logger.info(f"Worker {worker_id}: {len(my_ids)} URLs to re-scrape")

    from utility_api.agents.scrape import ScrapeAgent

    succeeded = 0
    failed = 0
    started = time.time()

    for i, registry_id in enumerate(my_ids):
        try:
            agent = ScrapeAgent()
            result = agent.run(registry_id=registry_id, max_depth=0)
            if result.get("succeeded", 0) > 0:
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            logger.debug(f"  id={registry_id} error: {e}")
            failed += 1

        if (i + 1) % 25 == 0:
            elapsed = time.time() - started
            rate = (i + 1) / elapsed * 3600 if elapsed > 0 else 0
            logger.info(f"Progress: {i+1}/{len(my_ids)}, {succeeded} ok, {failed} fail, {rate:.0f}/hr")

    elapsed = time.time() - started
    logger.info(f"Worker {worker_id} done: {succeeded}/{len(my_ids)} in {elapsed/60:.1f}m")

    return {"worker_id": worker_id, "total": len(my_ids), "succeeded": succeeded, "failed": failed}


def compare_results(candidate_ids: list[int], snapshots: list[dict]) -> dict:
    """Compare pre/post re-scrape and categorize outcomes."""
    from sqlalchemy import text
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema
    snap_map = {s["id"]: s for s in snapshots}

    recovered = []
    improved = []
    unchanged = []
    degraded = []

    with engine.connect() as conn:
        for batch_start in range(0, len(candidate_ids), 500):
            batch = candidate_ids[batch_start:batch_start + 500]
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url,
                       sr.last_content_length AS new_length,
                       LENGTH(sr.scraped_text) AS new_text_length
                FROM {schema}.scrape_registry sr
                WHERE sr.id = ANY(:ids)
            """), {"ids": batch}).fetchall()

            for r in rows:
                old = snap_map.get(r.id, {})
                old_len = old.get("old_text_length", 0) or 0
                new_len = r.new_text_length or 0

                entry = {
                    "id": r.id, "pwsid": r.pwsid, "url": r.url,
                    "old_length": old_len, "new_length": new_len,
                }

                if old_len < 200 and new_len >= 500:
                    recovered.append(entry)
                elif new_len > old_len * 2 and new_len < 500:
                    improved.append(entry)
                elif new_len < old_len and old_len > 0:
                    degraded.append(entry)
                else:
                    unchanged.append(entry)

    return {
        "recovered": recovered,
        "improved": improved,
        "unchanged": unchanged,
        "degraded": degraded,
    }


def print_results(results: dict, snapshots: list[dict]) -> None:
    """Print before/after comparison."""
    logger.info(f"\n{'='*60}")
    logger.info(f"RE-SCRAPE RESULTS")
    logger.info(f"{'='*60}")
    logger.info(f"  Recovered (thin→substantive): {len(results['recovered']):,}")
    logger.info(f"  Improved (still thin):        {len(results['improved']):,}")
    logger.info(f"  Unchanged:                    {len(results['unchanged']):,}")
    logger.info(f"  Degraded (worse):             {len(results['degraded']):,}")

    if results["recovered"]:
        logger.info(f"\n  Recovered URLs (top 10):")
        for r in sorted(results["recovered"], key=lambda x: x["new_length"], reverse=True)[:10]:
            logger.info(f"    {r['pwsid']} {r['old_length']:>5}→{r['new_length']:>6}ch  {r['url'][:60]}")

    if results["degraded"]:
        logger.info(f"\n  WARNING — Degraded URLs:")
        for d in results["degraded"][:5]:
            logger.info(f"    {d['pwsid']} {d['old_length']:>5}→{d['new_length']:>6}ch  {d['url'][:60]}")


def main():
    parser = argparse.ArgumentParser(
        description="Re-scrape bug-affected rows with fixed Playwright/form code")
    parser.add_argument("--candidates", default=None, help="JSON file of candidate IDs")
    parser.add_argument("--state", default=None, help="Auto-diagnose for state")
    parser.add_argument("--min-pop", type=int, default=0, help="Min population (with --state)")
    parser.add_argument("--workers", type=int, default=20, help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates, no re-scrape")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Re-scrape Recovery")
    logger.info("=" * 60)

    # Load candidates
    candidates = load_candidates(args.candidates, args.state, args.min_pop)
    candidate_ids = [c["id"] for c in candidates]

    if not candidate_ids:
        logger.warning("No candidates found.")
        return

    logger.info(f"Candidates: {len(candidate_ids):,} URLs")

    if args.dry_run:
        logger.info(f"[DRY RUN] Would reset and re-scrape {len(candidate_ids):,} URLs.")
        return

    # Phase 1: Snapshot
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    logger.info(f"\nPhase 1: Snapshotting {len(candidate_ids):,} rows...")
    snapshots = snapshot_candidates(candidate_ids)
    snap_path = PROJECT_ROOT / "logs" / f"rescrape_snapshot_{timestamp}.json"
    with open(snap_path, "w") as f:
        json.dump(snapshots, f)
    logger.info(f"  Snapshot saved to {snap_path}")

    # Phase 2: Reset
    logger.info(f"\nPhase 2: Resetting scraped_text to NULL...")
    reset_count = reset_candidates(candidate_ids, timestamp)
    logger.info(f"  Reset {reset_count:,} rows")

    # Phase 3: Parallel re-scrape
    logger.info(f"\nPhase 3: Re-scraping with {args.workers} workers...")
    pool = multiprocessing.Pool(processes=args.workers)
    try:
        results = pool.starmap(
            worker_main,
            [(w, candidate_ids, args.workers) for w in range(args.workers)],
        )
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
        return
    else:
        pool.close()
        pool.join()

    total_ok = sum(r["succeeded"] for r in results)
    total_fail = sum(r["failed"] for r in results)
    logger.info(f"  Workers done: {total_ok} succeeded, {total_fail} failed")

    # Phase 4: Compare and report
    logger.info(f"\nPhase 4: Comparing results...")
    comparison = compare_results(candidate_ids, snapshots)
    print_results(comparison, snapshots)

    # Save detailed results
    results_path = PROJECT_ROOT / "logs" / f"rescrape_results_{timestamp}.json"
    with open(results_path, "w") as f:
        json.dump(comparison, f, default=str)
    logger.info(f"\n  Detailed results saved to {results_path}")


if __name__ == "__main__":
    main()

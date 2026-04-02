#!/usr/bin/env python3
"""
Re-scrape NY Test: Pilot re-scrape on NY gap PWSIDs

Purpose:
    Thin wrapper combining diagnose + recover for the NY pilot.
    Tests the Playwright/form bug fixes on NY gap PWSIDs >= 3k pop.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Usage:
    python scripts/rescrape_ny_test.py --dry-run     # Show candidates
    python scripts/rescrape_ny_test.py --yes          # Execute re-scrape
"""

import argparse
import json
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from utility_api.config import settings
from utility_api.db import engine


def find_ny_candidates() -> list[dict]:
    """Find NY gap PWSIDs with thin/broken HTML content.

    Uses a broader filter (< 500 chars) than the general diagnostic
    to maximize recovery on this small test set.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH ny_gaps AS (
                SELECT c.pwsid, c.pws_name, c.population_served
                FROM {schema}.cws_boundaries c
                WHERE c.state_code = 'NY'
                  AND c.population_served >= 3000
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.rate_best_estimate rbe WHERE rbe.pwsid = c.pwsid
                  )
            )
            SELECT sr.id, sr.pwsid, ng.pws_name, ng.population_served,
                   sr.url, sr.last_content_length, sr.url_source,
                   sr.content_type, sr.notes
            FROM {schema}.scrape_registry sr
            JOIN ny_gaps ng ON ng.pwsid = sr.pwsid
            WHERE sr.status != 'dead'
              AND COALESCE(sr.content_type, 'html') != 'pdf'
              AND (
                  sr.scraped_text IS NULL
                  OR LENGTH(sr.scraped_text) < 500
                  OR sr.notes LIKE '%%playwright_reason=thin_still_thin%%'
                  OR sr.notes LIKE '%%playwright_reason=error%%'
                  OR (sr.url LIKE '%%.aspx%%' AND COALESCE(sr.last_content_length, 0) < 100)
              )
            ORDER BY ng.population_served DESC, sr.last_content_length ASC
        """)).fetchall()

    return [dict(r._mapping) for r in rows]


def main():
    parser = argparse.ArgumentParser(
        description="NY pilot: re-scrape gap PWSIDs with Playwright/form bug fixes")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates only")
    parser.add_argument("--yes", action="store_true", help="Execute without confirmation")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("NY Re-scrape Pilot Test")
    logger.info("=" * 60)

    candidates = find_ny_candidates()
    unique_pwsids = set(c["pwsid"] for c in candidates)
    total_pop = sum(
        c["population_served"] for c in candidates
        if c["pwsid"] in {c2["pwsid"] for c2 in candidates}
    )

    logger.info(f"\nCandidates: {len(candidates):,} URLs across {len(unique_pwsids):,} NY gap PWSIDs")

    # Show top candidates
    seen = set()
    logger.info(f"\nTop candidates by population:")
    for c in candidates:
        if c["pwsid"] in seen:
            continue
        seen.add(c["pwsid"])
        logger.info(
            f"  {c['pwsid']} {c['population_served']:>10,} "
            f"{(c['pws_name'] or '')[:30]:30s} "
            f"{c['last_content_length'] or 0:>5}ch {c['url_source']:>10} "
            f"{c['url'][:55]}"
        )
        if len(seen) >= 20:
            if len(unique_pwsids) > 20:
                logger.info(f"  ... +{len(unique_pwsids) - 20} more PWSIDs")
            break

    if args.dry_run:
        logger.info(f"\n[DRY RUN] Would re-scrape {len(candidates):,} URLs. Use --yes to execute.")
        return

    if not args.yes:
        logger.info(f"\nUse --yes to execute, or --dry-run to preview.")
        return

    # Execute via rescrape_recover
    from rescrape_recover import (
        snapshot_candidates,
        reset_candidates,
        worker_main,
        compare_results,
        print_results,
    )
    import multiprocessing
    from datetime import datetime, timezone

    candidate_ids = [c["id"] for c in candidates]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # Snapshot
    logger.info(f"\nSnapshotting {len(candidate_ids):,} rows...")
    snapshots = snapshot_candidates(candidate_ids)
    snap_path = PROJECT_ROOT / "logs" / f"rescrape_ny_snapshot_{timestamp}.json"
    with open(snap_path, "w") as f:
        json.dump(snapshots, f)

    # Reset
    logger.info(f"Resetting scraped_text to NULL...")
    reset_count = reset_candidates(candidate_ids, timestamp)
    logger.info(f"  Reset {reset_count:,} rows")

    # Re-scrape with 5 workers (small job)
    workers = 5
    logger.info(f"Re-scraping with {workers} workers...")
    pool = multiprocessing.Pool(processes=workers)
    try:
        results = pool.starmap(
            worker_main,
            [(w, candidate_ids, workers) for w in range(workers)],
        )
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
        return
    else:
        pool.close()
        pool.join()

    # Compare
    comparison = compare_results(candidate_ids, snapshots)
    print_results(comparison, snapshots)

    results_path = PROJECT_ROOT / "logs" / f"rescrape_ny_results_{timestamp}.json"
    with open(results_path, "w") as f:
        json.dump(comparison, f, default=str)
    logger.info(f"\nResults saved to {results_path}")


if __name__ == "__main__":
    main()

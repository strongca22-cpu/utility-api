#!/usr/bin/env python3
"""
Cascade Parse — Process All Unparsed PWSIDs

Purpose:
    Runs process_pwsid() (full cascade: rank 1→2→3→4→5 + deep crawl) on
    all PWSIDs that have Serper URLs but no scraped_llm rate. This is the
    workhorse that converts discovered URLs into rate data.

    Feeds from:
    - Discovery sweep results (new URLs from Scenario B + gap sweep)
    - Truncation batch failures (609 PWSIDs that failed even at 45k)
    - Original Scenario A cascade failures (rank 2/3 not yet tried)

    After processing, rebuilds best_estimate and exports dashboard data.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)

Usage:
    # Full run (all unparsed PWSIDs with Serper URLs)
    python scripts/run_cascade_parse.py 2>&1 | tee logs/cascade_parse.log

    # Dry run
    python scripts/run_cascade_parse.py --dry-run

    # Limit to N PWSIDs (for testing)
    python scripts/run_cascade_parse.py --limit 50

    # Min population filter
    python scripts/run_cascade_parse.py --min-pop 3000

Notes:
    - Uses process_pwsid() which tries rank 1→5 + reactive deep crawl
    - LLM parse costs: ~$0.004/attempt (direct API, not batch)
    - With 5 ranks and up to 3 parse attempts, worst case ~$0.012/PWSID
    - Rebuilds best_estimate per-state at the end (batched)
    - Safe to interrupt — each PWSID is independent
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
from utility_api.ops.best_estimate import run_best_estimate
from utility_api.pipeline.process import process_pwsid


def get_parse_candidates(min_pop: int = 0, limit: int | None = None) -> list[dict]:
    """Get PWSIDs with Serper URLs but no scraped_llm rate.

    Parameters
    ----------
    min_pop : int
        Minimum population filter.
    limit : int, optional
        Max PWSIDs to return.

    Returns
    -------
    list[dict]
        Sorted by population descending.
    """
    schema = settings.utility_schema
    limit_clause = f"LIMIT {limit}" if limit else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT c.pwsid, c.state_code, c.population_served, c.pws_name
            FROM {schema}.cws_boundaries c
            JOIN {schema}.scrape_registry sr ON sr.pwsid = c.pwsid
            WHERE sr.url_source = :src
              AND c.population_served >= :min_pop
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = c.pwsid AND rs.source_key = :llm
              )
            ORDER BY c.population_served DESC
            {limit_clause}
        """), {"src": "serper", "llm": "scraped_llm", "min_pop": min_pop}).fetchall()

    return [dict(r._mapping) for r in rows]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run cascade parse on all PWSIDs with Serper URLs but no scraped_llm rate"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview candidates, no API calls")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max PWSIDs to process")
    parser.add_argument("--min-pop", type=int, default=0,
                        help="Minimum population filter (default: all)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Cascade Parse — All Unparsed PWSIDs")
    logger.info(f"Started: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    candidates = get_parse_candidates(min_pop=args.min_pop, limit=args.limit)
    logger.info(f"Candidates: {len(candidates)} PWSIDs")

    if not candidates:
        logger.info("No candidates to process.")
        return

    total_pop = sum(c.get("population_served", 0) or 0 for c in candidates)
    states = {}
    for c in candidates:
        st = c.get("state_code", "??")
        states[st] = states.get(st, 0) + 1

    logger.info(f"Population: {total_pop:,.0f}")
    logger.info(f"States: {len(states)}")
    logger.info(f"Est. cost: ${len(candidates) * 0.008:.2f} (avg ~$0.008/PWSID cascade)")

    if args.dry_run:
        logger.info(f"\nDRY RUN — top 15 states:")
        for st, cnt in sorted(states.items(), key=lambda x: -x[1])[:15]:
            logger.info(f"  {st}: {cnt}")
        logger.info(f"\nTop 10 by population:")
        for c in candidates[:10]:
            logger.info(f"  {c['pwsid']}  {c.get('state_code', '??')}  "
                        f"pop={c.get('population_served', 0):>10,}  "
                        f"{(c.get('pws_name') or '')[:35]}")
        return

    # Process each PWSID through cascade pipeline
    succeeded = 0
    failed = 0
    errors = 0
    affected_states = set()
    started = datetime.now(timezone.utc)

    for i, cand in enumerate(candidates):
        pwsid = cand["pwsid"]

        if (i + 1) % 50 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            rate = (i + 1) / elapsed * 3600 if elapsed > 0 else 0
            remaining = len(candidates) - i - 1
            eta_hrs = remaining / rate if rate > 0 else 0
            logger.info(
                f"\n--- Progress: {i+1}/{len(candidates)} "
                f"({succeeded} succeeded, {failed} failed, {errors} errors, "
                f"{rate:.0f}/hr, ETA {eta_hrs:.1f}h) ---"
            )

        try:
            result = process_pwsid(pwsid, skip_best_estimate=True)
            if result.get("parse_success"):
                succeeded += 1
                st = cand.get("state_code", pwsid[:2])
                affected_states.add(st)
            else:
                failed += 1
        except Exception as e:
            errors += 1
            if errors <= 10:
                logger.error(f"  {pwsid}: cascade error: {e}")

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Cascade Parse Complete")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Processed: {len(candidates)}")
    logger.info(f"  Succeeded: {succeeded} ({succeeded/max(len(candidates),1)*100:.1f}%)")
    logger.info(f"  Failed:    {failed}")
    logger.info(f"  Errors:    {errors}")
    logger.info(f"  Time:      {elapsed/3600:.1f} hours")

    # Rebuild best estimate for affected states
    if affected_states:
        logger.info(f"\nRebuilding best estimate for {len(affected_states)} states...")
        run_best_estimate()
        logger.info("Best estimate rebuilt.")

    # Export dashboard data
    if succeeded > 0:
        logger.info("\nExporting dashboard data...")
        try:
            import subprocess
            subprocess.run(
                [sys.executable, "scripts/export_dashboard_data.py"],
                cwd=str(PROJECT_ROOT),
                check=True,
                capture_output=True,
            )
            logger.info("Dashboard data exported.")
        except Exception as e:
            logger.warning(f"Dashboard export failed: {e}")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"DONE — {succeeded} new rates, {failed} failures")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()

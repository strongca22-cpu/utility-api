#!/usr/bin/env python3
"""
Discovery Sweep — Unsearched Gap PWSIDs + Scenario B

Purpose:
    Runs Serper-based DiscoveryAgent for all PWSIDs that have never been
    searched. Two tiers processed in priority order:

    1. Gap PWSIDs (pop >= 3k, no rate_schedules, never searched): ~3,231
    2. Scenario B (pop 1k-3k, no rate_schedules, never searched): ~5,005

    Total: ~8,236 PWSIDs, ~33k Serper queries.

    Discovery only — does NOT parse. URLs are written to scrape_registry
    with rank tagging. Parse batch submitted separately after truncation
    batch completes.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)
    - Serper API key in .env

Usage:
    # Full sweep
    python scripts/run_discovery_sweep.py 2>&1 | tee logs/discovery_sweep.log

    # Dry run
    python scripts/run_discovery_sweep.py --dry-run

    # Gap only (skip Scenario B)
    python scripts/run_discovery_sweep.py --gap-only

Notes:
    - Discovery only — no parsing, no LLM calls
    - URLs written to scrape_registry with url_source='serper', discovery_rank=1-5
    - Serper cost: ~$33 at $0.001/query (paid mode)
    - Run in tmux: long-running (~4-6 hours at 0.2s/query delay)
    - Safe to interrupt and resume — already-searched PWSIDs are skipped
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

from utility_api.agents.discovery import DiscoveryAgent
from utility_api.config import settings
from utility_api.db import engine

QUERIES_PER_PWSID = 4


def get_unsearched_pwsids(min_pop: int = 1000, max_pop: int | None = None) -> list[dict]:
    """Get PWSIDs that have never been searched by Serper.

    Parameters
    ----------
    min_pop : int
        Minimum population threshold.
    max_pop : int, optional
        Maximum population (exclusive). None = no upper limit.

    Returns
    -------
    list[dict]
        Sorted by population descending.
    """
    schema = settings.utility_schema
    pop_clause = "AND c.population_served >= :min_pop"
    params = {"min_pop": min_pop}
    if max_pop:
        pop_clause += " AND c.population_served < :max_pop"
        params["max_pop"] = max_pop

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT c.pwsid, c.state_code, c.population_served, c.pws_name
            FROM {schema}.cws_boundaries c
            WHERE 1=1
              {pop_clause}
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = c.pwsid
              )
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = c.pwsid AND sr.url_source = 'serper'
              )
            ORDER BY c.population_served DESC
        """), params).fetchall()

    return [dict(r._mapping) for r in rows]


def run_discovery(
    targets: list[dict],
    label: str = "",
    dry_run: bool = False,
) -> dict:
    """Run DiscoveryAgent for each target PWSID.

    Parameters
    ----------
    targets : list[dict]
        PWSIDs to search.
    label : str
        Label for logging (e.g., "Gap >=3k", "Scenario B").
    dry_run : bool
        Preview only.

    Returns
    -------
    dict
        Summary stats.
    """
    est_queries = len(targets) * QUERIES_PER_PWSID

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Discovery: {label} ({len(targets)} PWSIDs, ~{est_queries:,} queries)")
    logger.info(f"{'=' * 60}")

    if dry_run:
        # State breakdown
        states = {}
        for t in targets:
            st = t.get("state_code", "??")
            states[st] = states.get(st, 0) + 1
        logger.info(f"\nDRY RUN — top 15 states:")
        for st, cnt in sorted(states.items(), key=lambda x: -x[1])[:15]:
            logger.info(f"  {st}: {cnt}")
        logger.info(f"\nTop 10 by population:")
        for t in targets[:10]:
            logger.info(f"  {t['pwsid']}  {t.get('state_code', '??')}  "
                        f"pop={t.get('population_served', 0):>10,}  "
                        f"{(t.get('pws_name') or '')[:35]}")
        return {"searched": 0, "urls_found": 0, "dry_run": True}

    agent = DiscoveryAgent()
    searched = 0
    urls_found = 0
    errors = 0
    started = datetime.now(timezone.utc)

    for i, target in enumerate(targets):
        pwsid = target["pwsid"]

        if (i + 1) % 100 == 0:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            rate = searched / elapsed * 3600 if elapsed > 0 else 0
            remaining = len(targets) - i - 1
            eta_hrs = remaining / rate if rate > 0 else 0
            logger.info(
                f"\n--- [{label}] Progress: {i+1}/{len(targets)} "
                f"({urls_found} URLs, {errors} errors, "
                f"{rate:.0f}/hr, ETA {eta_hrs:.1f}h) ---"
            )

        try:
            result = agent.run(pwsid=pwsid, search_delay=0.2)
            searched += 1
            urls_found += result.get("urls_written", 0)
        except Exception as e:
            errors += 1
            if errors <= 10:
                logger.error(f"  {pwsid}: discovery error: {e}")

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Discovery Complete: {label}")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Searched: {searched}")
    logger.info(f"  URLs found: {urls_found}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Time: {elapsed/3600:.1f} hours")

    return {
        "searched": searched,
        "urls_found": urls_found,
        "errors": errors,
        "elapsed_seconds": elapsed,
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Serper discovery for all unsearched PWSIDs"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview targets, no API calls")
    parser.add_argument("--gap-only", action="store_true",
                        help="Only search gap PWSIDs (pop >= 3k), skip Scenario B")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Discovery Sweep — Unsearched Gap + Scenario B")
    logger.info(f"Started: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # Tier 1: Gap PWSIDs (pop >= 3k)
    gap_targets = get_unsearched_pwsids(min_pop=3000)
    logger.info(f"\nTier 1 — Gap (pop >= 3k): {len(gap_targets)} PWSIDs")

    gap_result = run_discovery(gap_targets, label="Gap >=3k", dry_run=args.dry_run)

    # Tier 2: Scenario B (pop 1k-3k)
    if not args.gap_only:
        scenb_targets = get_unsearched_pwsids(min_pop=1000, max_pop=3000)
        logger.info(f"\nTier 2 — Scenario B (pop 1k-3k): {len(scenb_targets)} PWSIDs")

        scenb_result = run_discovery(scenb_targets, label="Scenario B 1k-3k", dry_run=args.dry_run)
    else:
        scenb_result = {"searched": 0, "urls_found": 0}
        logger.info("\n--gap-only: Skipping Scenario B")

    # Summary
    total_searched = gap_result.get("searched", 0) + scenb_result.get("searched", 0)
    total_urls = gap_result.get("urls_found", 0) + scenb_result.get("urls_found", 0)

    logger.info(f"\n{'=' * 60}")
    logger.info(f"SWEEP COMPLETE")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Gap >=3k searched:      {gap_result.get('searched', 0):>6}")
    logger.info(f"  Gap >=3k URLs found:    {gap_result.get('urls_found', 0):>6}")
    logger.info(f"  Scenario B searched:    {scenb_result.get('searched', 0):>6}")
    logger.info(f"  Scenario B URLs found:  {scenb_result.get('urls_found', 0):>6}")
    logger.info(f"  Total searched:         {total_searched:>6}")
    logger.info(f"  Total URLs found:       {total_urls:>6}")
    logger.info(f"\nNext steps:")
    logger.info(f"  URLs are in scrape_registry. Submit parse batch after truncation batch completes.")
    logger.info(f"  Combine with cascade failures for one large batch submission.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Reprocess Failed Serper PWSIDs Through Cascade Pipeline

Purpose:
    Runs the 8 PWSIDs that failed initial Serper parsing through the
    new cascade pipeline (deep crawl + re-score + cascade parse).
    Uses existing Serper URLs — no new API queries consumed.

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Dependencies:
    - utility_api (installed in dev mode)
    - PostgreSQL with utility schema (migration 020)

Usage:
    python scripts/reprocess_failed_serper.py
    python scripts/reprocess_failed_serper.py --pwsid CO0116001
    python scripts/reprocess_failed_serper.py --dry-run

Notes:
    - Does NOT consume Serper queries — uses existing URLs
    - Deep crawls each URL proactively (up to 15 fetches per URL)
    - Cascade parse: try top 3 re-scored candidates
    - All results logged to discovery_diagnostics table
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.pipeline.process import process_pwsid


def get_failed_serper_pwsids(specific_pwsid: str | None = None) -> list[dict]:
    """Find PWSIDs where Serper found URLs but no parse succeeded."""
    schema = settings.utility_schema

    if specific_pwsid:
        where_clause = "AND sr.pwsid = :pwsid"
        params = {"pwsid": specific_pwsid}
    else:
        where_clause = ""
        params = {}

    query = f"""
        SELECT DISTINCT sr.pwsid, s.pws_name, s.state_code,
               s.population_served_count as pop,
               count(*) FILTER (WHERE sr.url_source = 'serper') as serper_urls,
               count(*) FILTER (
                   WHERE sr.url_source = 'serper'
                   AND sr.last_parse_result = 'success'
               ) as serper_successes
        FROM {schema}.scrape_registry sr
        JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
        WHERE sr.url_source = 'serper'
          {where_clause}
        GROUP BY sr.pwsid, s.pws_name, s.state_code, s.population_served_count
        HAVING count(*) FILTER (
            WHERE sr.url_source = 'serper'
            AND sr.last_parse_result = 'success'
        ) = 0
        ORDER BY s.population_served_count DESC NULLS LAST
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    return [
        {
            "pwsid": r.pwsid,
            "pws_name": r.pws_name,
            "state_code": r.state_code,
            "population": r.pop,
            "serper_urls": r.serper_urls,
        }
        for r in rows
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Reprocess failed Serper PWSIDs through cascade pipeline"
    )
    parser.add_argument("--pwsid", type=str, help="Specific PWSID to reprocess")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    targets = get_failed_serper_pwsids(args.pwsid)

    if not targets:
        logger.info("No failed Serper PWSIDs found.")
        return

    logger.info(f"Found {len(targets)} PWSIDs to reprocess:")
    for t in targets:
        pop_str = f"{t['population']:>10,}" if t["population"] else "       N/A"
        logger.info(
            f"  {t['pwsid']:12s} {t['pws_name'][:35]:35s} "
            f"{pop_str} {t['state_code']:>4s} ({t['serper_urls']} URLs)"
        )

    if args.dry_run:
        logger.info("Dry run — no processing.")
        return

    # Process with batched BestEstimate
    stats = {"processed": 0, "succeeded": 0, "failed": 0}
    states_with_new_data: set[str] = set()

    for t in targets:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {t['pwsid']} | {t['pws_name']}")
        logger.info(f"{'='*60}")

        result = process_pwsid(
            pwsid=t["pwsid"],
            skip_best_estimate=True,
        )

        stats["processed"] += 1
        if result["parse_success"]:
            stats["succeeded"] += 1
            states_with_new_data.add(t["state_code"])
            logger.info(
                f"  ✓ SUCCESS — rank #{result['winning_rank']} "
                f"source={result['winning_source']} "
                f"({result['total_candidates']} candidates, "
                f"{result['deep_crawl_children']} from deep crawl)"
            )
        else:
            stats["failed"] += 1
            logger.info(
                f"  ✗ FAILED — {result['parse_attempts']} attempts, "
                f"{result['total_candidates']} candidates, "
                f"{result['deep_crawl_children']} from deep crawl"
            )

    # BestEstimate for affected states
    if states_with_new_data:
        from utility_api.agents.best_estimate import BestEstimateAgent
        for st in sorted(states_with_new_data):
            logger.info(f"\nRebuilding best_estimate for {st}...")
            BestEstimateAgent().run(state=st)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Cascade Reprocess Complete")
    logger.info(f"{'='*60}")
    logger.info(f"  Processed:  {stats['processed']}")
    logger.info(f"  Succeeded:  {stats['succeeded']}")
    logger.info(f"  Failed:     {stats['failed']}")
    if stats["processed"] > 0:
        logger.info(f"  Success rate: {100*stats['succeeded']/stats['processed']:.0f}%")

    # Point to diagnostics
    logger.info(f"\nDiagnostics query:")
    logger.info(f"  SELECT pwsid, starting_urls, deep_crawl_children,")
    logger.info(f"         total_candidates, parse_attempts, parse_success,")
    logger.info(f"         winning_rank, winning_source, winning_discovery_rank")
    logger.info(f"  FROM utility.discovery_diagnostics")
    logger.info(f"  ORDER BY run_at DESC LIMIT {stats['processed']};")


if __name__ == "__main__":
    main()

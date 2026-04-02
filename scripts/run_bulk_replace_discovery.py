#!/usr/bin/env python3
"""
Bulk Replace Discovery: Serper search for PWSIDs with bulk-only rates

Purpose:
    Run Serper discovery for PWSIDs that have rate_schedules from bulk
    sources (EFC, Duke, OWRS, etc.) but no scraped_llm data and no
    prior Serper search. These are Tier C of the bulk replacement plan.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - utility_api.agents.discovery (DiscoveryAgent)
    - PostgreSQL utility schema

Usage:
    # Dry run
    python scripts/run_bulk_replace_discovery.py --dry-run

    # Run >= 3k pop
    python scripts/run_bulk_replace_discovery.py --min-pop 3000 2>&1 | tee logs/bulk_replace_discovery.log

    # Run all >= 1k
    python scripts/run_bulk_replace_discovery.py --min-pop 1000
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

QUERIES_PER_PWSID = 4


def get_bulk_only_targets(min_pop: int = 3000) -> list[dict]:
    """Get PWSIDs with bulk rates but no scraped_llm and no Serper search."""
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


def main():
    parser = argparse.ArgumentParser(
        description="Serper discovery for bulk-only PWSIDs (Tier C bulk replacement)")
    parser.add_argument("--min-pop", type=int, default=3000,
                        help="Minimum population (default: 3000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview targets, no API calls")
    args = parser.parse_args()

    targets = get_bulk_only_targets(min_pop=args.min_pop)

    logger.info("=" * 60)
    logger.info(f"Bulk Replace Discovery — {len(targets):,} PWSIDs (pop >= {args.min_pop:,})")
    logger.info(f"Serper queries: ~{len(targets) * QUERIES_PER_PWSID:,}")
    logger.info("=" * 60)

    if not targets:
        logger.info("No targets found.")
        return

    # Import and reuse the discovery runner
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
    from run_discovery_sweep import run_discovery

    result = run_discovery(
        targets,
        label=f"Bulk replace >= {args.min_pop:,}",
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Discovery complete: {result.get('searched', 0)} searched, "
                     f"{result.get('urls_found', 0)} URLs found")
        logger.info(f"Next: scrape + batch parse the discovered URLs")
        logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()

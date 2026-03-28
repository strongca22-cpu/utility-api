#!/usr/bin/env python3
"""
Sprint 22 — Minneapolis Metro SearXNG Discovery Batch

Purpose:
    Run SearXNG-powered URL discovery for MN utilities, starting with
    Minneapolis metro (pop > 5000) and working down. This is a targeted
    test of the Sprint 22 query/scoring improvements.

Author: AI-Generated
Created: 2026-03-28
Modified: 2026-03-28

Dependencies:
    - utility_api (installed in dev mode)
    - SearXNG VPS instance via SSH tunnel on :8889

Usage:
    tmux new-session -d -s mn_discovery "python3 scripts/run_mn_discovery.py 2>&1 | tee logs/mn_discovery.log"

Notes:
    - Processes MN PWSIDs by population descending
    - Respects throttle settings in config/agent_config.yaml
    - Uses Sprint 22 query builder (7 queries) and scoring (path + freshness)
    - Top-3 URLs per PWSID written to scrape_registry
    - After discovery, runs scrape+parse on any newly discovered URLs
    - Session size: 10 PWSIDs per cycle, then scrape cycle, then repeat
    - Estimated throughput: ~10 PWSIDs/hour (7 queries × 8s + 15s between)
    - For 135 MN utilities > 5000 pop: ~14 hours total

Data Sources:
    - Input: utility.pwsid_coverage (MN, pop > 5000, no rate data)
    - Output: utility.scrape_registry (url_source='searxng')
"""

import sys
import time
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from loguru import logger
from sqlalchemy import text

from utility_api.agents.discovery import DiscoveryAgent
from utility_api.config import settings
from utility_api.db import engine


# --- Configuration ---
STATE = "MN"
MIN_POPULATION = 5000
DISCOVERY_BATCH_SIZE = 10       # PWSIDs per discovery cycle
PAUSE_BETWEEN_CYCLES = 120      # 2 minutes between cycles (let SearXNG breathe)
MAX_CYCLES = 20                 # Safety cap: 20 cycles × 10 = 200 PWSIDs max


def get_mn_candidates(limit: int = 10) -> list[dict]:
    """Fetch MN PWSIDs needing discovery, ordered by population."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pc.pwsid, pc.pws_name, pc.population_served, pc.scrape_status
            FROM {schema}.pwsid_coverage pc
            WHERE pc.state_code = :state
              AND pc.has_rate_data = FALSE
              AND pc.population_served >= :min_pop
              -- Sprint 22: use searxng_status (separate from domain guesser scrape_status)
              AND pc.searxng_status = 'not_attempted'
            ORDER BY pc.population_served DESC
            LIMIT :limit
        """), {"state": STATE, "min_pop": MIN_POPULATION, "limit": limit}).fetchall()

    return [{"pwsid": r.pwsid, "pws_name": r.pws_name,
             "population": r.population_served, "scrape_status": r.scrape_status}
            for r in rows]


def run_scrape_cycle():
    """Process any pending URLs discovered this session."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        pending = conn.execute(text(f"""
            SELECT count(*) FROM {schema}.scrape_registry
            WHERE url_source = 'searxng' AND status = 'pending'
              AND pwsid LIKE 'MN%'
        """)).scalar()

    if pending and pending > 0:
        logger.info(f"  Scrape cycle: {pending} pending MN URLs from SearXNG")
        from utility_api.agents.scrape import ScrapeAgent
        from utility_api.agents.parse import ParseAgent
        from utility_api.agents.best_estimate import BestEstimateAgent

        # Process pending URLs
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url, sr.content_type
                FROM {schema}.scrape_registry sr
                WHERE sr.url_source = 'searxng' AND sr.status = 'pending'
                  AND sr.pwsid LIKE 'MN%'
                ORDER BY sr.discovery_score DESC NULLS LAST
                LIMIT 30
            """)).fetchall()

        for r in rows:
            try:
                # Scrape
                scrape_result = ScrapeAgent().run(
                    registry_id=r.id, pwsid=r.pwsid, url=r.url
                )
                # Parse if scrape succeeded
                if scrape_result.get("status") == "success":
                    ParseAgent().run(registry_id=r.id, pwsid=r.pwsid)
            except Exception as e:
                logger.warning(f"  Scrape/parse error for {r.pwsid}: {e}")
                continue

        # Run best estimates for MN
        try:
            BestEstimateAgent().run(state="MN")
        except Exception as e:
            logger.warning(f"  Best estimate error: {e}")
    else:
        logger.info("  No pending MN URLs to scrape")


def main():
    logger.info(f"=== MN Discovery Batch — Sprint 22 ===")
    logger.info(f"State: {STATE}, min_pop: {MIN_POPULATION}")
    logger.info(f"Batch size: {DISCOVERY_BATCH_SIZE}, max cycles: {MAX_CYCLES}")

    discovery = DiscoveryAgent()
    total_discovered = 0
    total_processed = 0

    for cycle in range(1, MAX_CYCLES + 1):
        candidates = get_mn_candidates(limit=DISCOVERY_BATCH_SIZE)
        if not candidates:
            logger.info(f"Cycle {cycle}: no more candidates. Done.")
            break

        logger.info(
            f"\n=== Cycle {cycle}/{MAX_CYCLES} — "
            f"{len(candidates)} PWSIDs (largest: {candidates[0]['pws_name']}, "
            f"pop={candidates[0]['population']:,}) ==="
        )

        cycle_urls = 0
        for i, c in enumerate(candidates, 1):
            logger.info(
                f"  [{i}/{len(candidates)}] {c['pwsid']} | {c['pws_name']} | "
                f"pop={c['population']:,}"
            )
            try:
                result = discovery.run(
                    pwsid=c["pwsid"],
                    utility_name=c["pws_name"],
                    state=STATE,
                    skip_domain_guess=True,  # Domain guesser is a separate pipeline
                )
                urls_found = result.get("urls_written", 0)
                cycle_urls += urls_found
                total_discovered += urls_found
                total_processed += 1
                logger.info(
                    f"    → {urls_found} URLs written "
                    f"(session total: {total_discovered})"
                )
            except Exception as e:
                logger.error(f"    Discovery failed: {e}")
                total_processed += 1

        logger.info(
            f"\nCycle {cycle} complete: {cycle_urls} URLs from "
            f"{len(candidates)} PWSIDs"
        )

        # Run scrape/parse on discovered URLs
        run_scrape_cycle()

        # Coverage check
        with engine.connect() as conn:
            stats = conn.execute(text(f"""
                SELECT
                    sum(case when has_rate_data then 1 else 0 end) as with_rates,
                    count(*) as total,
                    round(100.0 * sum(case when has_rate_data then population_served else 0 end)
                          / nullif(sum(population_served), 0), 1) as pct_pop
                FROM {settings.utility_schema}.pwsid_coverage
                WHERE state_code = 'MN'
            """)).fetchone()
            logger.info(
                f"  MN coverage: {stats.with_rates}/{stats.total} PWSIDs, "
                f"{stats.pct_pop}% pop"
            )

        if cycle < MAX_CYCLES and candidates:
            logger.info(f"  Pausing {PAUSE_BETWEEN_CYCLES}s before next cycle...")
            time.sleep(PAUSE_BETWEEN_CYCLES)

    logger.info(
        f"\n=== MN Discovery Complete ===\n"
        f"  PWSIDs processed: {total_processed}\n"
        f"  URLs discovered: {total_discovered}\n"
    )


if __name__ == "__main__":
    main()

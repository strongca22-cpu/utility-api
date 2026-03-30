#!/usr/bin/env python3
"""
Parse Sweep Daemon

Purpose:
    Continuously processes unparsed URLs from scrape_registry. Finds
    entries with scraped_text in DB but no parse result, parses them,
    and classifies url_quality. Runs in a tmux session, polls on interval.

    Sprint 23 — replaces the need for manual ua-ops process-backlog
    invocations for ongoing pipeline processing.

Author: AI-Generated
Created: 2026-03-28
Modified: 2026-03-28

Dependencies:
    - utility_api (installed in dev mode)
    - PostgreSQL with utility schema

Usage:
    # One-shot (test):
    python scripts/parse_sweep.py --once --max-per-sweep 5

    # Daemon in tmux:
    tmux new-session -d -s parse_sweep \
        "cd ~/projects/utility-api && python scripts/parse_sweep.py \
         --interval 1800 --max-per-sweep 25 2>&1 | tee -a logs/parse_sweep.log"

Notes:
    - Default interval: 30 minutes (1800s). Do not set below 900s.
    - Default max per sweep: 25 entries. At $0.01-0.04/parse, ~$0.25-1.00/sweep.
    - Prioritizes by source (SearXNG > curated > deep_crawl > guesser)
      then by population served descending.
    - Skips blacklisted and probable_junk entries (url_quality filter).
    - If scraped_text not in DB, re-fetches via ScrapeAgent.fetch_single_url().
    - BestEstimate batched per state at end of each sweep.

Data Sources:
    - Input: utility.scrape_registry (active, unparsed, with content)
    - Output: utility.rate_schedules (parsed rates)
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from loguru import logger
from sqlalchemy import text

from utility_api.agents.parse import ParseAgent
from utility_api.agents.scrape import ScrapeAgent
from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema


def get_unparsed_entries(max_count: int = 25) -> list:
    """Find registry entries that need parsing.

    Returns entries ordered by source priority and population.
    Skips blacklisted and probable_junk entries.
    """
    query = f"""
        SELECT sr.id, sr.pwsid, sr.url, sr.url_source,
               sr.scraped_text, sr.last_content_length,
               s.pws_name, s.state_code
        FROM {schema}.scrape_registry sr
        LEFT JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
        WHERE sr.status = 'active'
          AND sr.last_parse_result IS NULL
          AND sr.last_content_length > 500
          AND sr.url LIKE 'http%%'
          AND COALESCE(sr.url_quality, 'unknown') NOT IN ('blacklisted', 'probable_junk')
        ORDER BY
            CASE sr.url_source
                WHEN 'searxng' THEN 1
                WHEN 'curated' THEN 2
                WHEN 'curated_portland' THEN 2
                WHEN 'metro_research' THEN 2
                WHEN 'duke_reference' THEN 3
                WHEN 'state_directory' THEN 4
                WHEN 'deep_crawl' THEN 5
                WHEN 'domain_guesser' THEN 6
                ELSE 7
            END,
            s.population_served_count DESC NULLS LAST
        LIMIT :max_count
    """
    with engine.connect() as conn:
        return conn.execute(text(query), {"max_count": max_count}).fetchall()


def persist_scraped_text(registry_id: int, raw_text: str) -> None:
    """Write scraped text to scrape_registry for future use."""
    import hashlib

    # Skip binary content (docx/xlsx/zip with null bytes)
    if "\x00" in raw_text:
        logger.debug(f"  Skipping text persistence (binary content)")
        return

    with engine.connect() as conn:
        conn.execute(text(f"""
            UPDATE {schema}.scrape_registry SET
                scraped_text = :text,
                last_content_length = :length,
                last_content_hash = :hash,
                updated_at = NOW()
            WHERE id = :id
        """), {
            "text": raw_text,
            "length": len(raw_text),
            "hash": hashlib.sha256(raw_text.encode()).hexdigest(),
            "id": registry_id,
        })
        conn.commit()


def run_sweep(max_per_sweep: int = 25) -> dict:
    """One sweep: find unparsed entries, parse them."""
    entries = get_unparsed_entries(max_per_sweep)

    if not entries:
        logger.info("Sweep: no unparsed entries found")
        return {"processed": 0, "parsed": 0, "failed": 0, "skipped": 0}

    logger.info(f"Sweep: processing {len(entries)} entries")

    parse = ParseAgent()
    stats = {"processed": 0, "parsed": 0, "failed": 0, "skipped": 0}
    successful_states: set[str] = set()

    for entry in entries:
        stats["processed"] += 1

        # Read text from DB (Sprint 23) or re-fetch if not persisted
        raw_text = entry.scraped_text
        if not raw_text:
            # Text not persisted yet — need to re-fetch
            scrape = ScrapeAgent()
            result = scrape.fetch_single_url(entry.url)
            if result and result.get("text") and len(result["text"]) > 100:
                raw_text = result["text"]
                persist_scraped_text(entry.id, raw_text)
                logger.info(f"  Backfilled {entry.pwsid} ({len(raw_text):,} chars)")
            else:
                logger.debug(f"  {entry.pwsid}: re-fetch returned no content")
                stats["failed"] += 1
                continue

        # Parse
        parse_result = parse.run(
            pwsid=entry.pwsid,
            raw_text=raw_text,
            source_url=entry.url,
            registry_id=entry.id,
            skip_best_estimate=True,  # batch at end
        )

        if parse_result.get("skipped"):
            stats["skipped"] += 1
        elif parse_result.get("success"):
            stats["parsed"] += 1
            if entry.state_code:
                successful_states.add(entry.state_code)
        else:
            stats["failed"] += 1

    # Batch BestEstimate per state
    if successful_states:
        logger.info(f"  Running BestEstimate for {len(successful_states)} states...")
        try:
            from utility_api.agents.best_estimate import BestEstimateAgent

            best_estimate = BestEstimateAgent()
            for state_code in sorted(successful_states):
                best_estimate.run(state=state_code)
        except Exception as e:
            logger.warning(f"  BestEstimate failed: {e}")

    logger.info(
        f"Sweep complete: processed={stats['processed']}, "
        f"parsed={stats['parsed']}, failed={stats['failed']}, "
        f"skipped={stats['skipped']}"
    )
    return stats


def main():
    """Main entry point for parse sweep daemon."""
    parser = argparse.ArgumentParser(
        description="Parse sweep daemon — continuously processes unparsed URLs"
    )
    parser.add_argument(
        "--interval", type=int, default=1800,
        help="Seconds between sweeps (default: 1800 = 30 min, minimum 900)",
    )
    parser.add_argument(
        "--max-per-sweep", type=int, default=25,
        help="Max entries per sweep (default: 25)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run once and exit (for testing)",
    )
    args = parser.parse_args()

    if args.interval < 900:
        logger.warning(f"Interval {args.interval}s too low — clamping to 900s (15 min)")
        args.interval = 900

    logger.info(
        f"Parse sweep starting (interval={args.interval}s, "
        f"max={args.max_per_sweep})"
    )

    while True:
        sweep_start = datetime.now(timezone.utc)
        try:
            stats = run_sweep(args.max_per_sweep)
        except Exception as e:
            logger.error(f"Sweep error: {e}")
            stats = {"processed": 0}

        if args.once:
            break

        # Log timing
        elapsed = (datetime.now(timezone.utc) - sweep_start).total_seconds()
        logger.info(f"Sweep took {elapsed:.0f}s, sleeping {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Parse Deep Crawl Backlog

Purpose:
    Re-fetches and parses 1,182 deep_crawl entries that have rate-relevant
    URLs but were never parsed. The scrape_registry stores URL + metadata
    but not the page content itself, so we must re-fetch each URL before
    parsing.

    Uses the rate_scraper for fetching and ParseAgent for LLM extraction.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-28 (Sprint 23: use unified chain, DB text fallback)

Usage:
    python scripts/parse_deep_crawl_backlog.py
    python scripts/parse_deep_crawl_backlog.py --dry-run
    python scripts/parse_deep_crawl_backlog.py --limit 50
"""

import sys
import time
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.pipeline.chain import scrape_and_parse

schema = settings.utility_schema

# Parse args
dry_run = "--dry-run" in sys.argv
limit = None
for i, arg in enumerate(sys.argv):
    if arg == "--limit" and i + 1 < len(sys.argv):
        limit = int(sys.argv[i + 1])

started = datetime.now(timezone.utc)

# Get all rate-relevant deep_crawl entries that were never parsed
with engine.connect() as conn:
    query = f"""
        SELECT sr.id, sr.pwsid, sr.url, s.pws_name, sr.last_content_length,
               sr.content_type
        FROM {schema}.scrape_registry sr
        JOIN {schema}.sdwis_systems s ON sr.pwsid = s.pwsid
        WHERE sr.url_source = 'deep_crawl'
          AND (sr.last_parse_result IS NULL OR sr.last_parse_result NOT IN ('success', 'skipped'))
          AND sr.last_content_length > 0
          AND (
            sr.url ILIKE '%rate%' OR sr.url ILIKE '%fee%' OR sr.url ILIKE '%tariff%'
            OR sr.url ILIKE '%billing%' OR sr.url ILIKE '%charge%' OR sr.url ILIKE '%utility%'
            OR sr.url ILIKE '%.pdf%'
          )
        ORDER BY sr.last_content_length ASC
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(text(query)).fetchall()

logger.info(f"=== Deep Crawl Parse Backlog ===")
logger.info(f"Rate-relevant entries to process: {len(rows)}")

if dry_run:
    logger.info("[DRY RUN]")
    for r in rows[:20]:
        logger.info(f"  {r[1]} | {r[3][:35]:35s} | {r[4]:>6} chars | {r[2][:70]}")
    logger.info(f"  ... {len(rows)} total")
    sys.exit(0)

successes = 0
failures = 0
skipped = 0
fetch_failures = 0
api_cost = 0.0
affected_states: set[str] = set()

for i, (reg_id, pwsid, url, name, prev_len, content_type) in enumerate(rows):
    if i % 100 == 0 and i > 0:
        logger.info(
            f"--- Progress: {i}/{len(rows)} | "
            f"success={successes} fail={failures} skip={skipped} "
            f"fetch_fail={fetch_failures} cost=${api_cost:.2f} ---"
        )

    try:
        # Sprint 23: unified chain handles scrape + persist + parse
        result = scrape_and_parse(
            pwsid=pwsid,
            registry_id=reg_id,
            skip_best_estimate=True,
        )

        if result.get("error") == "scrape_failed":
            fetch_failures += 1
            continue

        for parse_result in result.get("parse_results", []):
            if parse_result is None:
                skipped += 1
                continue

            conf = parse_result.get("confidence", "failed")
            cost = parse_result.get("cost_usd", 0) or 0
            api_cost += cost

            if parse_result.get("skipped"):
                skipped += 1
            elif conf in ("high", "medium"):
                successes += 1
                affected_states.add(pwsid[:2])
                bill = parse_result.get("bill_10ccf", "?")
                logger.info(
                    f"  [{i+1}] SUCCESS {pwsid} | {name[:30]} | {conf} | "
                    f"@10CCF=${bill} | ${cost:.4f}"
                )
            else:
                failures += 1

    except Exception as e:
        failures += 1
        if "rate limit" in str(e).lower() or "429" in str(e):
            logger.warning(f"  Rate limited — sleeping 30s")
            time.sleep(30)
        else:
            logger.debug(f"  [{i+1}] ERROR {pwsid}: {type(e).__name__}: {str(e)[:80]}")

    # Pacing: 0.5s between fetches to be polite
    time.sleep(0.5)

# Run BestEstimate once per affected state
if affected_states:
    logger.info(f"  Updating best estimates for {len(affected_states)} states...")
    try:
        from utility_api.agents.best_estimate import BestEstimateAgent
        for st in sorted(affected_states):
            BestEstimateAgent().run(state=st)
    except Exception as e:
        logger.warning(f"  Best estimate update failed: {e}")

elapsed = (datetime.now(timezone.utc) - started).total_seconds()

logger.info(f"\n{'=' * 60}")
logger.info(f"=== Deep Crawl Parse Backlog Complete ===")
logger.info(f"Total processed:  {len(rows)}")
logger.info(f"Successes:        {successes}")
logger.info(f"Parse failures:   {failures}")
logger.info(f"Parse skipped:    {skipped}")
logger.info(f"Fetch failures:   {fetch_failures}")
logger.info(f"API cost:         ${api_cost:.2f}")
logger.info(f"Elapsed:          {elapsed:.0f}s ({elapsed/60:.1f}m)")
logger.info(f"Success rate:     {successes/len(rows)*100:.1f}%" if rows else "N/A")

# Log pipeline run
with engine.connect() as conn:
    conn.execute(text(f"""
        INSERT INTO {schema}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "parse-deep-crawl-backlog",
        "started": started,
        "count": successes,
        "notes": (
            f"total={len(rows)}, success={successes}, "
            f"parse_fail={failures}, parse_skip={skipped}, "
            f"fetch_fail={fetch_failures}, "
            f"api_cost=${api_cost:.2f}, elapsed={elapsed:.0f}s"
        ),
    })
    conn.commit()

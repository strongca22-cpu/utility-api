#!/usr/bin/env python3
"""
Parse Deep Crawl Backlog

Purpose:
    Runs the parse agent on 1,182 deep_crawl entries that have fetched
    content but were never parsed. These are URLs discovered during deep
    crawling that have rate-relevant URL patterns (rate/fee/tariff/billing/
    utility/PDF in path).

    Content is already in the DB — no scraping needed.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

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
from utility_api.agents.parse import ParseAgent

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
        SELECT sr.pwsid, sr.url, s.pws_name, sr.last_content_length
        FROM {schema}.scrape_registry sr
        JOIN {schema}.sdwis_systems s ON sr.pwsid = s.pwsid
        WHERE sr.url_source = 'deep_crawl'
          AND sr.last_parse_result IS NULL
          AND sr.last_content_length > 0
          AND (
            sr.url ILIKE '%rate%' OR sr.url ILIKE '%fee%' OR sr.url ILIKE '%tariff%'
            OR sr.url ILIKE '%billing%' OR sr.url ILIKE '%charge%' OR sr.url ILIKE '%utility%'
            OR sr.url ILIKE '%.pdf%'
          )
        ORDER BY sr.last_content_length DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(text(query)).fetchall()

logger.info(f"=== Deep Crawl Parse Backlog ===")
logger.info(f"Rate-relevant entries to parse: {len(rows)}")
if dry_run:
    logger.info("[DRY RUN] — will not invoke parser")
    for r in rows[:20]:
        logger.info(f"  {r[0]} | {r[2][:35]:35s} | {r[3]:>6} chars | {r[1][:60]}")
    logger.info(f"  ... {len(rows)} total")
    sys.exit(0)

parser = ParseAgent()
successes = 0
failures = 0
skipped = 0
api_cost = 0.0

for i, (pwsid, url, name, content_len) in enumerate(rows):
    if i % 50 == 0 and i > 0:
        logger.info(f"--- Progress: {i}/{len(rows)} | success={successes} fail={failures} skip={skipped} cost=${api_cost:.2f} ---")

    try:
        result = parser.run(pwsid=pwsid, url=url)

        if result is None:
            skipped += 1
            continue

        conf = result.get("confidence", "failed")
        cost = result.get("cost", 0) or 0
        api_cost += cost

        if conf in ("high", "medium"):
            successes += 1
            bill = result.get("bill_10ccf", "?")
            logger.info(f"  [{i+1}] SUCCESS {pwsid} | {name[:30]} | {conf} | @10CCF=${bill} | {content_len}ch")
        else:
            failures += 1
    except Exception as e:
        failures += 1
        logger.debug(f"  [{i+1}] ERROR {pwsid}: {e}")

    # Light pacing — no scraping needed, just API calls
    time.sleep(0.3)

elapsed = (datetime.now(timezone.utc) - started).total_seconds()

logger.info(f"\n{'=' * 60}")
logger.info(f"=== Deep Crawl Parse Backlog Complete ===")
logger.info(f"Total processed: {len(rows)}")
logger.info(f"Successes: {successes}")
logger.info(f"Failures: {failures}")
logger.info(f"Skipped: {skipped}")
logger.info(f"API cost: ${api_cost:.2f}")
logger.info(f"Elapsed: {elapsed:.0f}s ({elapsed/60:.1f}m)")
logger.info(f"Success rate: {successes/len(rows)*100:.1f}%" if rows else "N/A")

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
            f"fail={failures}, skip={skipped}, "
            f"api_cost=${api_cost:.2f}, elapsed={elapsed:.0f}s"
        ),
    })
    conn.commit()

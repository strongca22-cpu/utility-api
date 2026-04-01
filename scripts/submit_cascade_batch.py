#!/usr/bin/env python3
"""
Submit Cascade Batch — Rank 2-5 for Prior Failures

Purpose:
    Submits rank 2-5 URLs to the Batch API for PWSIDs where rank 1
    already failed. These are PWSIDs that went through Scenario A or
    truncation batch and failed — the cascade pipeline data shows 49%
    of successes come from rank 2/3, so these are high-value attempts.

    Each PWSID gets up to 4 batch tasks (rank 2, 3, 4, 5). First
    success wins; redundant successes are handled by ON CONFLICT in
    rate_schedules.

    Separate from the new-discovery batch — these PWSIDs already have
    scraped text from prior runs.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)
    - ANTHROPIC_API_KEY in .env

Usage:
    python scripts/submit_cascade_batch.py --dry-run
    python scripts/submit_cascade_batch.py

Notes:
    - Excludes rank 1 (already tried and failed)
    - Only includes PWSIDs that have been attempted before (have search_log)
      but have no scraped_llm rate
    - Batch API: 50% cost savings, ~24hr SLA
    - After batch completes: python scripts/process_scenario_a_batch.py
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.batch import BatchAgent
from utility_api.config import settings
from utility_api.db import engine


def get_cascade_tasks() -> list[dict]:
    """Get rank 2-5 parse tasks for PWSIDs where rank 1 failed.

    Returns parse tasks with scraped text from rank 2+ URLs.
    Only includes PWSIDs that were previously attempted (have entries
    in search_log from before the current sweep) but have no scraped_llm.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id as registry_id, sr.pwsid, sr.scraped_text,
                   sr.content_type, sr.url as source_url,
                   sr.discovery_rank, sr.discovery_score,
                   LENGTH(sr.scraped_text) as text_len,
                   c.population_served, c.state_code
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.url_source = :src
              AND sr.discovery_rank >= 2
              AND sr.scraped_text IS NOT NULL
              AND LENGTH(sr.scraped_text) > 200
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = sr.pwsid AND rs.source_key = :llm
              )
              AND EXISTS (
                SELECT 1 FROM {schema}.search_log sl
                WHERE sl.pwsid = sr.pwsid
                  AND sl.search_engine = :src
                  AND sl.searched_at < '2026-03-31 17:15:00'
              )
            ORDER BY c.population_served DESC, sr.discovery_rank ASC
        """), {"src": "serper", "llm": "scraped_llm"}).fetchall()

    tasks = []
    for r in rows:
        tasks.append({
            "pwsid": r.pwsid,
            "raw_text": r.scraped_text[:45000],
            "content_type": r.content_type or "html",
            "source_url": r.source_url or "",
            "registry_id": r.registry_id,
        })

    return tasks


def main():
    parser = argparse.ArgumentParser(
        description="Submit rank 2-5 batch for prior cascade failures"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Cascade Batch — Rank 2-5 for Prior Failures")
    logger.info("=" * 60)

    tasks = get_cascade_tasks()

    if not tasks:
        logger.info("No cascade tasks found.")
        return

    # Count unique PWSIDs
    pwsids = set(t["pwsid"] for t in tasks)
    logger.info(f"Tasks:   {len(tasks)} (rank 2-5 URLs)")
    logger.info(f"PWSIDs:  {len(pwsids)} unique")
    logger.info(f"Est. cost: ${len(tasks) * 0.002:.2f} (batch pricing)")

    # Rank distribution
    rank_counts = {}
    for t in tasks:
        # Get rank from registry_id lookup — or just count by position
        pass

    if args.dry_run:
        # Show sample
        seen = set()
        logger.info(f"\nDRY RUN — sample (first occurrence per PWSID):")
        for t in tasks[:20]:
            if t["pwsid"] not in seen:
                seen.add(t["pwsid"])
                logger.info(f"  {t['pwsid']}  {t['content_type']}  "
                            f"text={len(t['raw_text']):>6,}  {t['source_url'][:50]}")
        return

    # Submit
    logger.info(f"\nSubmitting {len(tasks)} tasks to Batch API...")
    agent = BatchAgent()
    result = agent.submit(parse_tasks=tasks, state_filter="cascade_r2_5")

    if result.get("batch_id"):
        batch_id = result["batch_id"]
        logger.info(f"\nBatch submitted: {batch_id}")
        logger.info(f"Tasks: {result.get('task_count', len(tasks))}")
        logger.info(f"\nProcess when complete:")
        logger.info(f"  python scripts/process_scenario_a_batch.py --batch-id {batch_id}")
    else:
        logger.error(f"Submission failed: {result}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Resubmit Truncated Batch — 15k→45k Text Cap Recovery

Purpose:
    Scenario A (batch msgbatch_01FhetQeo9TfoTkBroYFHT1T) was submitted with
    a 15k character text cap. The cap was raised to 45k three hours later
    (commit ec1ed3d). 1,346 PWSIDs had text >15k that was truncated:
    - 647 failed to parse (truncation likely cause, especially 528 PDFs)
    - 699 "succeeded" but with degraded data (rate table may have been cut)

    This script resubmits these PWSIDs to the Batch API with the full text
    (up to 45k), applying all current pipeline improvements (JSON repair,
    bill consistency recovery, expanded normalization).

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - anthropic
    - sqlalchemy
    - loguru
    - utility_api (local package)

Usage:
    python scripts/resubmit_truncated_batch.py --dry-run     # preview
    python scripts/resubmit_truncated_batch.py               # submit batch
    python scripts/resubmit_truncated_batch.py --failures-only  # only the 647 failures

Notes:
    - Uses Batch API (50% cost savings, ~24hr SLA)
    - Estimated cost: ~1,346 tasks × $0.002 = ~$2.70 (batch pricing)
    - After batch completes, process with:
      python scripts/process_scenario_a_batch.py
    - Poll with: tmux session running poll_scenario_a.sh
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


TEXT_CAP = 45000  # Current cap (was 15k when Scenario A submitted)
OLD_CAP = 15000   # Cap at time of Scenario A submission


def get_truncated_pwsids(failures_only: bool = False) -> list[dict]:
    """Find PWSIDs with scraped text >15k that need reprocessing.

    Parameters
    ----------
    failures_only : bool
        If True, only include PWSIDs that failed to parse (no scraped_llm).
        If False, include all truncated PWSIDs (failures + degraded successes).

    Returns
    -------
    list[dict]
        Parse tasks with pwsid, raw_text, content_type, source_url, registry_id.
    """
    schema = settings.utility_schema

    if failures_only:
        exists_clause = f"""
            AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = sr.pwsid AND rs.source_key = 'scraped_llm'
            )
        """
    else:
        exists_clause = ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id as registry_id, sr.pwsid, sr.scraped_text,
                   sr.content_type, sr.url as source_url,
                   LENGTH(sr.scraped_text) as text_len,
                   c.population_served, c.state_code
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.url_source = 'serper'
              AND sr.discovery_rank = 1
              AND sr.scraped_text IS NOT NULL
              AND LENGTH(sr.scraped_text) > :old_cap
              {exists_clause}
            ORDER BY c.population_served DESC
        """), {"old_cap": OLD_CAP}).fetchall()

    tasks = []
    for r in rows:
        tasks.append({
            "pwsid": r.pwsid,
            "raw_text": r.scraped_text[:TEXT_CAP],
            "content_type": r.content_type or "html",
            "source_url": r.source_url or "",
            "registry_id": r.registry_id,
            "text_len": r.text_len,
            "population": r.population_served or 0,
            "state": r.state_code or r.pwsid[:2],
        })

    return tasks


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Resubmit truncated Scenario A tasks with 45k text cap"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview tasks, no API calls",
    )
    parser.add_argument(
        "--failures-only", action="store_true",
        help="Only resubmit PWSIDs that failed (not degraded successes)",
    )
    args = parser.parse_args()

    mode = "failures only" if args.failures_only else "all truncated"
    logger.info(f"=== Resubmit Truncated Batch ({mode}) ===")
    logger.info(f"Old cap: {OLD_CAP:,} chars | New cap: {TEXT_CAP:,} chars")

    tasks = get_truncated_pwsids(failures_only=args.failures_only)

    if not tasks:
        logger.info("No tasks to resubmit.")
        return

    total_pop = sum(t["population"] for t in tasks)
    states = {}
    for t in tasks:
        states[t["state"]] = states.get(t["state"], 0) + 1

    logger.info(f"Tasks: {len(tasks)}")
    logger.info(f"Population: {total_pop:,.0f}")
    logger.info(f"States: {len(states)}")
    logger.info(f"Est. cost: ${len(tasks) * 0.002:.2f} (batch pricing)")

    # Text length distribution
    buckets = {"15k-20k": 0, "20k-30k": 0, "30k-45k": 0}
    for t in tasks:
        tl = t["text_len"]
        if tl <= 20000:
            buckets["15k-20k"] += 1
        elif tl <= 30000:
            buckets["20k-30k"] += 1
        else:
            buckets["30k-45k"] += 1
    logger.info(f"Text length distribution:")
    for b, c in buckets.items():
        logger.info(f"  {b}: {c}")

    if args.dry_run:
        logger.info(f"\nDRY RUN — top 20 by population:")
        for t in tasks[:20]:
            logger.info(
                f"  {t['pwsid']}  {t['state']}  pop={t['population']:>10,}  "
                f"text={t['text_len']:>6,}  {t['content_type']}"
            )
        if len(tasks) > 20:
            logger.info(f"  ... +{len(tasks) - 20} more")
        return

    # Submit via BatchAgent
    logger.info(f"\nSubmitting {len(tasks)} tasks to Batch API...")
    agent = BatchAgent()
    result = agent.submit(
        parse_tasks=tasks,
        state_filter="truncation_reprocess",
    )

    if "error" in result:
        logger.error(f"Submission failed: {result['error']}")
        return

    batch_id = result.get("batch_id")
    logger.info(f"\nBatch submitted: {batch_id}")
    logger.info(f"Tasks: {result.get('task_count', len(tasks))}")
    logger.info(f"\nNext steps:")
    logger.info(f"  1. Wait ~24 hours for batch to complete")
    logger.info(f"  2. Check status: python scripts/run_scenario_a.py --check-status")
    logger.info(f"  3. Process: python scripts/process_scenario_a_batch.py --batch-id {batch_id}")
    logger.info(f"  4. Or use poller: tmux new-session -d -s truncation_poll "
                f"'cd ~/projects/utility-api && ./scripts/poll_scenario_a.sh'")


if __name__ == "__main__":
    main()

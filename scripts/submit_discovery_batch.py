#!/usr/bin/env python3
"""
Submit Parse Batch — Unified Batch Submission for All Parse Workloads

Purpose:
    Submits parse tasks to the Anthropic Batch API using configurable
    strategy (shotgun, cascade, rank1_only). Works for any parse workload:
    new discoveries, cascade retries, re-processing, etc.

    Strategy is set in config/agent_config.yaml (batch_api.default_strategy)
    and overridable via --strategy flag.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)
    - ANTHROPIC_API_KEY in .env

Usage:
    # Submit with config default strategy (currently "shotgun")
    python scripts/submit_discovery_batch.py

    # Override strategy
    python scripts/submit_discovery_batch.py --strategy rank1_only
    python scripts/submit_discovery_batch.py --strategy cascade

    # Filter by population
    python scripts/submit_discovery_batch.py --min-pop 3000

    # Cascade round 2 (skip rank 1, which already failed)
    python scripts/submit_discovery_batch.py --strategy cascade --min-rank 2

    # Specific PWSIDs
    python scripts/submit_discovery_batch.py --pwsids TX1010013 CA3010092

    # Dry run
    python scripts/submit_discovery_batch.py --dry-run

Notes:
    - Strategies:
        shotgun    — all viable URLs per PWSID (fast, ~18% overhead)
        cascade    — best untried URL per PWSID (cheap, multi-day cycle)
        rank1_only — single best URL per PWSID (cheapest one-shot)
    - Batch API: 50% cost savings vs direct API, ~24hr SLA
    - After batch completes: python scripts/process_scenario_a_batch.py
    - Or use poller: ./scripts/poll_scenario_a.sh
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.batch import BatchAgent
from utility_api.ops.batch_task_builder import build_parse_tasks, VALID_STRATEGIES


def main():
    parser = argparse.ArgumentParser(
        description="Submit parse tasks to Anthropic Batch API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategies:
  shotgun    — Submit ALL viable URLs per PWSID. Fast (1 day), ~18% cost overhead.
  cascade    — Submit only the best untried URL. Cheapest, but takes N days.
  rank1_only — Submit only the top-scored URL. Cheapest single-pass.

Examples:
  # Shotgun (default) — all URLs, one batch, done in 24hr
  python scripts/submit_discovery_batch.py

  # Cascade round 1 (rank 1 only)
  python scripts/submit_discovery_batch.py --strategy rank1_only

  # Cascade round 2 (skip rank 1, try rank 2+ for failures)
  python scripts/submit_discovery_batch.py --strategy cascade --min-rank 2

  # Cascade round 3
  python scripts/submit_discovery_batch.py --strategy cascade --min-rank 3
""",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview tasks, no API calls")
    parser.add_argument("--strategy", choices=VALID_STRATEGIES, default=None,
                        help="Batch strategy (default: from config)")
    parser.add_argument("--min-pop", type=int, default=0,
                        help="Minimum population filter")
    parser.add_argument("--min-rank", type=int, default=1,
                        help="Minimum discovery_rank (2+ to skip rank 1)")
    parser.add_argument("--max-rank", type=int, default=5,
                        help="Maximum discovery_rank")
    parser.add_argument("--pwsids", nargs="+", default=None,
                        help="Specific PWSIDs to submit")
    parser.add_argument("--exclude-attempted", action="store_true",
                        help="Skip URLs with existing parse results")
    parser.add_argument("--label", default=None,
                        help="Label for batch_jobs.state_filter (default: auto)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Parse Batch Submission")
    logger.info(f"Strategy: {args.strategy or 'config default'}")
    logger.info("=" * 60)

    tasks = build_parse_tasks(
        strategy=args.strategy,
        min_pop=args.min_pop,
        pwsids=args.pwsids,
        min_rank=args.min_rank,
        max_rank=args.max_rank,
        exclude_attempted=args.exclude_attempted,
    )

    if not tasks:
        logger.info("No tasks to submit.")
        return

    unique_pwsids = len(set(t["pwsid"] for t in tasks))
    est_cost = len(tasks) * 0.002

    logger.info(f"Tasks:   {len(tasks):,}")
    logger.info(f"PWSIDs:  {unique_pwsids:,}")
    logger.info(f"Est. cost: ${est_cost:.2f} (batch pricing)")

    if args.dry_run:
        # Show sample
        seen = set()
        logger.info(f"\nDRY RUN — sample tasks:")
        for t in tasks[:15]:
            if t["pwsid"] not in seen:
                seen.add(t["pwsid"])
                logger.info(f"  {t['pwsid']}  {t['content_type']}  "
                            f"text={len(t['raw_text']):>6,}  "
                            f"{t['source_url'][:55]}")
        remaining = unique_pwsids - len(seen)
        if remaining > 0:
            logger.info(f"  ... +{remaining} more PWSIDs")
        return

    # Submit
    label = args.label or f"batch_{args.strategy or 'default'}_r{args.min_rank}"
    logger.info(f"\nSubmitting {len(tasks):,} tasks to Batch API...")
    agent = BatchAgent()
    result = agent.submit(parse_tasks=tasks, state_filter=label)

    if result.get("batch_id"):
        batch_id = result["batch_id"]
        logger.info(f"\nBatch submitted: {batch_id}")
        logger.info(f"Tasks: {result.get('task_count', len(tasks)):,}")
        logger.info(f"\nProcess when complete:")
        logger.info(f"  python scripts/process_scenario_a_batch.py --batch-id {batch_id}")
        logger.info(f"\nOr start poller:")
        logger.info(f"  tmux new-session -d -s batch_poll ./scripts/poll_scenario_a.sh")
    else:
        logger.error(f"Submission failed: {result}")


if __name__ == "__main__":
    main()

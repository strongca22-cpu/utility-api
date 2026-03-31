#!/usr/bin/env python3
"""
Process Scenario A Batch Results

Purpose:
    Directly processes a completed Scenario A batch by batch_id. Bypasses
    the check_status filter issue where --process-batch can't find batches
    that --check-status already transitioned to 'completed'.

    After processing: rebuilds best_estimate globally.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Usage:
    python scripts/process_scenario_a_batch.py
    python scripts/process_scenario_a_batch.py --batch-id msgbatch_01FhetQeo9TfoTkBroYFHT1T
"""

import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.batch import BatchAgent
from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.best_estimate import run_best_estimate


def find_latest_completed_batch() -> str | None:
    """Find the most recent batch with status='completed' (not yet processed)."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT batch_id, task_count, submitted_at
            FROM {schema}.batch_jobs
            WHERE status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
        """)).fetchone()
    if row:
        logger.info(f"Found completed batch: {row.batch_id} ({row.task_count} tasks)")
        return row.batch_id
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Process Scenario A batch results")
    parser.add_argument("--batch-id", help="Specific batch ID (default: latest completed)")
    args = parser.parse_args()

    batch_id = args.batch_id or find_latest_completed_batch()
    if not batch_id:
        logger.info("No completed batches to process.")
        return

    logger.info(f"Processing batch {batch_id}...")
    agent = BatchAgent()
    result = agent.process_batch(batch_id)

    if "error" in result:
        logger.error(f"Processing failed: {result['error']}")
        return

    logger.info(f"Batch processed:")
    logger.info(f"  Succeeded: {result.get('succeeded', 0)}")
    logger.info(f"  Failed: {result.get('failed', 0)}")
    logger.info(f"  Cost: ${result.get('total_cost', 0):.4f}")

    logger.info("\nRebuilding best estimate globally...")
    run_best_estimate()
    logger.info("Done!")


if __name__ == "__main__":
    main()

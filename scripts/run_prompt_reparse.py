#!/usr/bin/env python3
"""
Prompt Reparse Batch: Resubmit Previously-Failed Parses with Consolidated Prompts

Purpose:
    Reparse all scrape_registry rows that failed under the old prompts,
    using the consolidated prompt changes from Sprint 27:
    - Water/sewer charge separation rule
    - Ordinance/legal format recognition
    - PDF garbled table reconstruction
    - Unified user message with utility context + text delimiters
    - Strengthened retry addendum

    No discovery or scraping needed — all text is already in the DB.
    This is a parse-only batch submission.

    Scope:
    - All scrape_registry rows where last_parse_result = 'failed'
    - Content length > 500 chars (substantive content)
    - PWSID has no existing rate_schedules (scraped_llm source_key)
    - All url_sources included (serper, deep_crawl, searxng, etc.)
    - ALL failed rows submitted (not just best-per-PWSID)

Author: AI-Generated
Created: 2026-04-01
Modified: 2026-04-01

Dependencies:
    - utility_api.agents.batch (BatchAgent)
    - utility_api.agents.parse (route_model)
    - utility_api.ingest.rate_parser (build_parse_user_message)
    - PostgreSQL utility schema

Usage:
    # Dry run: preview target rows and cost estimate
    python scripts/run_prompt_reparse.py --dry-run

    # Submit batch
    python scripts/run_prompt_reparse.py 2>&1 | tee logs/prompt_reparse_submit.log

    # Check batch status
    python scripts/run_prompt_reparse.py --check-status

    # Process completed batch results
    python scripts/run_prompt_reparse.py --process-batch 2>&1 | tee logs/prompt_reparse_process.log

Notes:
    - 2,807 rows across 1,693 PWSIDs (all url_sources)
    - Estimated batch cost: ~$13 (50% batch discount)
    - Expected yield: 500-800 new rate schedules (30-50% recovery)
    - Raw LLM responses stored in last_parse_raw_response for diagnostics
    - Best estimate rebuilds per-state after batch processing
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

BATCH_LABEL = "prompt_reparse_v1"
MIN_CONTENT_LENGTH = 500
# Conservative avg cost per parse task (batch pricing, Haiku-heavy mix)
EST_COST_PER_TASK_BATCH = 0.0047


# --- Target Selection ---

def get_reparse_targets() -> list[dict]:
    """Select all failed parse rows eligible for reparsing.

    Criteria:
    - scraped_text exists and is > 500 chars
    - last_parse_result = 'failed'
    - PWSID has no existing rate_schedule (scraped_llm)
    - All url_sources included

    Returns
    -------
    list[dict]
        Each dict: {registry_id, pwsid, content_type, source_url,
                     utility_name, state_code, text_length, url_source}
        Sorted by population descending, then content length descending.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id AS registry_id,
                   sr.pwsid,
                   sr.scraped_text,
                   COALESCE(sr.content_type, 'html') AS content_type,
                   sr.url AS source_url,
                   sr.url_source,
                   sr.last_content_length AS text_length,
                   c.pws_name,
                   c.state_code,
                   c.population_served
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.scraped_text IS NOT NULL
              AND sr.last_parse_result = 'failed'
              AND sr.last_content_length > :min_len
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.rate_schedules rs
                  WHERE rs.pwsid = sr.pwsid AND rs.source_key = 'scraped_llm'
              )
            ORDER BY c.population_served DESC, sr.last_content_length DESC
        """), {"min_len": MIN_CONTENT_LENGTH}).fetchall()

    targets = []
    for r in rows:
        targets.append({
            "registry_id": r.registry_id,
            "pwsid": r.pwsid,
            "raw_text": r.scraped_text[:45000],
            "content_type": r.content_type,
            "source_url": r.source_url or "",
            "url_source": r.url_source or "unknown",
            "text_length": r.text_length,
            "utility_name": r.pws_name or "",
            "state_code": r.state_code or "",
            "population_served": r.population_served or 0,
        })

    return targets


def print_summary(targets: list[dict]) -> None:
    """Print detailed summary of the reparse target population."""
    if not targets:
        logger.info("No targets found.")
        return

    pwsids = set(t["pwsid"] for t in targets)
    states = Counter(t["state_code"] for t in targets)
    sources = Counter(t["url_source"] for t in targets)
    content_types = Counter(t["content_type"] for t in targets)
    lengths = sorted(t["text_length"] for t in targets)

    from utility_api.agents.parse import route_model
    model_counts = Counter(route_model(t["raw_text"]) for t in targets)

    est_cost = len(targets) * EST_COST_PER_TASK_BATCH

    logger.info(f"\n{'='*60}")
    logger.info(f"PROMPT REPARSE BATCH — Target Summary")
    logger.info(f"{'='*60}")
    logger.info(f"Total rows:    {len(targets):,}")
    logger.info(f"Unique PWSIDs: {len(pwsids):,}")
    logger.info(f"Est. cost:     ~${est_cost:.2f} (batch pricing)")

    logger.info(f"\nURL source:")
    for src, cnt in sources.most_common():
        logger.info(f"  {src}: {cnt:,}")

    logger.info(f"\nContent type:")
    for ct, cnt in content_types.most_common():
        logger.info(f"  {ct}: {cnt:,}")

    logger.info(f"\nModel routing:")
    for model, cnt in model_counts.most_common():
        short = model.split("-")[1] if "-" in model else model
        logger.info(f"  {short}: {cnt:,}")

    logger.info(f"\nContent length:")
    logger.info(f"  P25: {lengths[len(lengths)//4]:,} chars")
    logger.info(f"  P50: {lengths[len(lengths)//2]:,} chars")
    logger.info(f"  P75: {lengths[3*len(lengths)//4]:,} chars")
    logger.info(f"  Max: {lengths[-1]:,} chars")

    logger.info(f"\nTop 10 states:")
    for state, cnt in states.most_common(10):
        logger.info(f"  {state}: {cnt:,}")


# --- Submit Batch ---

def submit_batch(targets: list[dict]) -> str | None:
    """Submit reparse tasks to Anthropic Batch API.

    Parameters
    ----------
    targets : list[dict]
        Reparse targets with raw_text already loaded.

    Returns
    -------
    str | None
        Batch ID, or None on failure.
    """
    from utility_api.agents.batch import BatchAgent

    # Build parse_tasks in the format BatchAgent.submit() expects
    parse_tasks = []
    for t in targets:
        parse_tasks.append({
            "pwsid": t["pwsid"],
            "raw_text": t["raw_text"],
            "content_type": t["content_type"],
            "source_url": t["source_url"],
            "registry_id": t["registry_id"],
            "utility_name": t["utility_name"],
            "state_code": t["state_code"],
        })

    logger.info(f"\nSubmitting {len(parse_tasks):,} parse tasks to Anthropic Batch API")

    agent = BatchAgent()
    result = agent.submit(parse_tasks=parse_tasks, state_filter=BATCH_LABEL)

    if result.get("batch_id"):
        logger.info(f"Batch submitted: {result['batch_id']}")
        logger.info(f"Task count: {result['task_count']}")
        logger.info(f"Status: {result['status']}")
        return result["batch_id"]
    else:
        logger.error(f"Batch submission failed: {result}")
        return None


# --- Process Batch Results ---

def process_batch():
    """Check for completed batches and process results."""
    from utility_api.agents.batch import BatchAgent

    agent = BatchAgent()

    statuses = agent.check_status()
    logger.info(f"Batch status check: {len(statuses)} batches found")
    for s in statuses:
        logger.info(f"  {s.get('batch_id', '?')}: {s.get('local_status', '?')} "
                     f"({s.get('task_count', 0)} tasks)")

    completed = [s for s in statuses if s.get("local_status") == "completed"]
    if not completed:
        logger.info("No completed batches to process.")
        in_progress = [s for s in statuses
                       if s.get("local_status") in ("pending", "in_progress")]
        if in_progress:
            logger.info(f"{len(in_progress)} batch(es) still processing:")
            for s in in_progress:
                logger.info(f"  {s['batch_id']}: {s.get('api_status', 'unknown')} "
                             f"(submitted {s.get('submitted_at', '?')})")
        return

    for batch_info in completed:
        batch_id = batch_info["batch_id"]
        logger.info(f"\nProcessing batch {batch_id}...")
        result = agent.process_batch(batch_id)
        logger.info(f"  Succeeded: {result.get('succeeded', 0)}")
        logger.info(f"  Failed: {result.get('failed', 0)}")
        logger.info(f"  Cost: ${result.get('total_cost', 0):.4f}")

    logger.info("\n=== Batch processing complete ===")
    logger.info("Best estimate has been rebuilt for all affected states.")


# --- Main ---

def main():
    parser = argparse.ArgumentParser(
        description="Reparse failed parse tasks with consolidated prompts (batch API)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview targets and cost (no API calls)
  python scripts/run_prompt_reparse.py --dry-run

  # Submit batch
  python scripts/run_prompt_reparse.py 2>&1 | tee logs/prompt_reparse_submit.log

  # Check batch status
  python scripts/run_prompt_reparse.py --check-status

  # Process completed batch results
  python scripts/run_prompt_reparse.py --process-batch 2>&1 | tee logs/prompt_reparse_process.log
""",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview targets and cost estimate, no API calls")
    parser.add_argument("--check-status", action="store_true",
                        help="Check status of pending batches")
    parser.add_argument("--process-batch", action="store_true",
                        help="Process completed batch results + rebuild best_estimate")

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Prompt Reparse Batch (Sprint 27 — consolidated prompts)")
    logger.info("=" * 60)

    if args.check_status:
        from utility_api.agents.batch import BatchAgent
        statuses = BatchAgent().check_status()
        for s in statuses:
            logger.info(f"  {s}")
        return

    if args.process_batch:
        process_batch()
        return

    # --- Main: query targets + submit ---

    logger.info("Loading reparse targets from scrape_registry...")
    targets = get_reparse_targets()

    if not targets:
        logger.warning("No reparse targets found")
        return

    print_summary(targets)

    if args.dry_run:
        logger.info("\n[DRY RUN] Would submit the above targets. Exiting.")
        return

    # Submit batch
    started = datetime.now(timezone.utc)
    batch_id = submit_batch(targets)

    if batch_id:
        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH SUBMITTED: {batch_id}")
        logger.info(f"Tasks: {len(targets):,}")
        logger.info(f"Label: {BATCH_LABEL}")
        logger.info(f"Next step: wait ~24 hours, then run:")
        logger.info(f"  python scripts/run_prompt_reparse.py --process-batch")
        logger.info(f"{'='*60}")

    # Log to pipeline_runs
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, :finished, :count, :status, :notes)
        """), {
            "step": "prompt_reparse_v1",
            "started": started,
            "finished": datetime.now(timezone.utc),
            "count": len(targets),
            "status": "success" if batch_id else "failed",
            "notes": json.dumps({
                "batch_id": batch_id,
                "total_rows": len(targets),
                "unique_pwsids": len(set(t["pwsid"] for t in targets)),
                "batch_label": BATCH_LABEL,
            }),
        })
        conn.commit()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Scenario A: Comprehensive Gap Sweep (>=3k pop) with Batch API

Purpose:
    Full coverage expansion sweep. Discovers + scrapes all gap PWSIDs
    with population >= 3,000, collects parse tasks, and submits to
    Anthropic Message Batches API (50% cost savings, ~24hr latency).

    Scope:
    - All gap PWSIDs (no rate data) with population >= 3,000
    - 14 zero-candidate PWSIDs from Sprint 25 runs (retry with threshold 45)
    - Duke-only PWSIDs handled separately (not in this script)

    Hard limits:
    - Serper: 49,000 credits available
    - Anthropic: $110 credits available
    - Score threshold: 45 (lowered from 50 in Sprint 25)

    Phases:
    1. Discovery: Serper search for all target PWSIDs
    2. Scrape: Fetch content for all discovered URLs
    3. Collect: Gather parse tasks from scraped content
    4. Submit: Send to Anthropic Batch API
    5. (Wait ~24 hours)
    6. Process: run_scenario_a.py --process-batch to handle results

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Dependencies:
    - utility_api.agents.discovery (DiscoveryAgent)
    - utility_api.agents.scrape (ScrapeAgent)
    - utility_api.agents.batch (BatchAgent)
    - utility_api.agents.parse (route_model)
    - PostgreSQL utility schema

Usage:
    # Phase 1-4: Discover, scrape, submit batch
    python scripts/run_scenario_a.py 2>&1 | tee logs/scenario_a_discovery.log

    # Phase 6: Process completed batch + rebuild best_estimate
    python scripts/run_scenario_a.py --process-batch 2>&1 | tee logs/scenario_a_process.log

    # Check batch status
    python scripts/run_scenario_a.py --check-status

    # Dry run (show target PWSIDs, no API calls)
    python scripts/run_scenario_a.py --dry-run

Notes:
    - Serper queries: ~5,000 PWSIDs × 4 = ~20,000 queries (within 49k limit)
    - Anthropic batch cost: ~$42 (batch pricing)
    - Expected yield: ~3,000 new rates at 60% parse success
    - Best estimate rebuilds per-state after batch processing
"""

import argparse
import json
import sys
import time
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

SERPER_HARD_LIMIT = 49_000
ANTHROPIC_HARD_LIMIT_USD = 110.0
MIN_POPULATION = 3000
QUERIES_PER_PWSID = 4
# Conservative avg cost per parse task (batch pricing, based on Sprint 25 data)
EST_COST_PER_TASK_BATCH = 0.0098


# --- Target Selection ---

def get_target_pwsids() -> list[dict]:
    """Select PWSIDs for the comprehensive sweep.

    Returns gap PWSIDs (no rate data) with population >= 3,000,
    plus zero-candidate retries from today's Sprint 25 runs.

    Returns
    -------
    list[dict]
        Each dict: {pwsid, population_served, state_code, reason}
    """
    schema = settings.utility_schema
    targets = []

    with engine.connect() as conn:
        # 1. Gap PWSIDs >= 3k pop (no rate data in any table)
        gap_rows = conn.execute(text(f"""
            SELECT c.pwsid, c.population_served, c.state_code
            FROM {schema}.cws_boundaries c
            WHERE c.population_served >= :min_pop
            AND NOT EXISTS(
                SELECT 1 FROM {schema}.rate_schedules rs WHERE rs.pwsid = c.pwsid
            )
            AND NOT EXISTS(
                SELECT 1 FROM {schema}.water_rates wr WHERE wr.pwsid = c.pwsid
            )
            ORDER BY c.population_served DESC
        """), {"min_pop": MIN_POPULATION}).fetchall()

        for r in gap_rows:
            targets.append({
                "pwsid": r[0],
                "population_served": r[1],
                "state_code": r[2],
                "reason": "gap",
            })

        gap_count = len(targets)
        logger.info(f"Gap PWSIDs >= {MIN_POPULATION:,} pop: {gap_count:,}")

        # 2. Zero-candidate retries (PWSIDs with 0 candidates from previous runs)
        #    These may benefit from the lowered threshold (50→45)
        existing_pwsids = {t["pwsid"] for t in targets}
        zero_cand_rows = conn.execute(text(f"""
            SELECT dd.pwsid, c.population_served, c.state_code
            FROM {schema}.discovery_diagnostics dd
            JOIN {schema}.cws_boundaries c ON dd.pwsid = c.pwsid
            WHERE dd.candidates_above_threshold = 0
        """)).fetchall()
        zero_cand_rows = [r for r in zero_cand_rows if r[0] not in existing_pwsids]

        for r in zero_cand_rows:
            targets.append({
                "pwsid": r[0],
                "population_served": r[1],
                "state_code": r[2],
                "reason": "zero_candidate_retry",
            })

        retry_count = len(targets) - gap_count
        logger.info(f"Zero-candidate retries: {retry_count}")

    logger.info(f"Total targets: {len(targets):,}")
    return targets


# --- Budget Check ---

def check_budget(targets: list[dict]) -> bool:
    """Verify the sweep fits within hard limits.

    Parameters
    ----------
    targets : list[dict]
        Target PWSIDs.

    Returns
    -------
    bool
        True if within limits.
    """
    total_queries = len(targets) * QUERIES_PER_PWSID
    # Assume ~65% get URLs, ~60% of those produce parse tasks
    est_parse_tasks = int(len(targets) * 0.89 * 0.65)  # discovery rate * parse attempt rate
    est_anthropic_cost = est_parse_tasks * EST_COST_PER_TASK_BATCH

    logger.info(f"\n=== Budget Check ===")
    logger.info(f"Serper queries needed:  {total_queries:,} / {SERPER_HARD_LIMIT:,} available")
    logger.info(f"Est. parse tasks:       ~{est_parse_tasks:,}")
    logger.info(f"Est. Anthropic cost:    ~${est_anthropic_cost:.2f} / ${ANTHROPIC_HARD_LIMIT_USD:.2f} available")

    if total_queries > SERPER_HARD_LIMIT:
        logger.error(f"OVER SERPER LIMIT by {total_queries - SERPER_HARD_LIMIT:,} queries")
        return False
    if est_anthropic_cost > ANTHROPIC_HARD_LIMIT_USD:
        logger.error(f"OVER ANTHROPIC LIMIT by ${est_anthropic_cost - ANTHROPIC_HARD_LIMIT_USD:.2f}")
        return False

    logger.info("Budget OK ✓")
    return True


# --- Phase 1-2: Discovery + Scrape ---

def discover_and_scrape(targets: list[dict]) -> list[dict]:
    """Run Serper discovery + URL scraping for all targets.

    This is the expensive phase (Serper API calls + web fetches).
    Does NOT call the LLM — that's deferred to batch.

    Parameters
    ----------
    targets : list[dict]
        Target PWSIDs from get_target_pwsids().

    Returns
    -------
    list[dict]
        Parse tasks: [{pwsid, raw_text, content_type, source_url, registry_id}]
    """
    from utility_api.agents.discovery import DiscoveryAgent
    from utility_api.agents.scrape import ScrapeAgent
    from utility_api.pipeline.process import (
        _deep_crawl_url,
        _ensure_fetched,
        _get_starting_urls,
        _get_system_metadata,
    )
    from utility_api.agents.discovery import score_url_relevance, _DISCOVERY_CONFIG

    schema = settings.utility_schema
    discovery_agent = DiscoveryAgent()

    parse_tasks = []
    stats = {
        "total": len(targets),
        "discovered": 0,
        "no_urls": 0,
        "scraped": 0,
        "with_parse_task": 0,
        "errors": 0,
        "serper_queries": 0,
    }

    score_threshold = _DISCOVERY_CONFIG.get("url_score_threshold", 45)
    logger.info(f"Score threshold: {score_threshold}")

    for i, target in enumerate(targets):
        pwsid = target["pwsid"]
        state = target["state_code"]

        if (i + 1) % 100 == 0:
            logger.info(
                f"\n--- Progress: {i+1}/{len(targets)} "
                f"({stats['with_parse_task']} parse tasks collected, "
                f"{stats['no_urls']} no URLs) ---"
            )

        # Step 1: Discovery (Serper search)
        try:
            disc_result = discovery_agent.run(pwsid=pwsid, diagnostic=False)
            stats["serper_queries"] += QUERIES_PER_PWSID
        except Exception as e:
            logger.warning(f"  {pwsid}: discovery error: {e}")
            stats["errors"] += 1
            continue

        # Step 2: Get starting URLs from registry
        starting_urls = _get_starting_urls(pwsid, schema, max_urls=3)
        if not starting_urls:
            stats["no_urls"] += 1
            continue
        stats["discovered"] += 1

        # Step 3: Fetch/scrape each URL
        all_candidates = []
        for start in starting_urls:
            try:
                fetch_result = _ensure_fetched(
                    pwsid=pwsid,
                    registry_id=start["id"],
                    url=start["url"],
                )
                all_candidates.append({
                    "registry_id": start["id"],
                    "url": start["url"],
                    "url_source": start["url_source"],
                    "discovery_rank": start["discovery_rank"],
                    "text_len": fetch_result["text_len"],
                    "scraped_text": fetch_result["text"],
                    "content_type": fetch_result["content_type"],
                })
            except Exception as e:
                logger.debug(f"  {pwsid}: fetch error for {start['url'][:50]}: {e}")

        if not all_candidates:
            stats["no_urls"] += 1
            continue

        # Step 4: Re-score candidates
        meta = _get_system_metadata(pwsid, schema)
        utility_name = meta.get("pws_name", "")
        city = meta.get("city", "")

        for cand in all_candidates:
            snippet_proxy = (cand["scraped_text"] or "")[:200]
            cand["rescore"] = score_url_relevance(
                url=cand["url"],
                title="",
                snippet=snippet_proxy,
                utility_name=utility_name,
                city=city,
                state=state,
            )

        # Sort by re-score, filter
        all_candidates.sort(key=lambda c: c["rescore"], reverse=True)
        parseable = [
            c for c in all_candidates
            if c["rescore"] >= 30 and c.get("text_len") and c["text_len"] > 100
        ]

        if not parseable:
            stats["no_urls"] += 1
            continue

        stats["scraped"] += 1

        # Step 5: Collect parse task for the BEST candidate
        # In batch mode, we submit the highest-scored candidate only.
        # Cascade retries aren't possible in batch — one shot per PWSID.
        # If it fails, the parse_sweep cron will pick it up later with direct API.
        best = parseable[0]
        parse_tasks.append({
            "pwsid": pwsid,
            "raw_text": best["scraped_text"],
            "content_type": best.get("content_type", "html"),
            "source_url": best["url"],
            "registry_id": best["registry_id"],
        })
        stats["with_parse_task"] += 1

    return parse_tasks, stats


# --- Phase 3-4: Submit Batch ---

def submit_batch(parse_tasks: list[dict]) -> str | None:
    """Submit parse tasks to Anthropic Batch API.

    Parameters
    ----------
    parse_tasks : list[dict]
        Parse tasks from discover_and_scrape().

    Returns
    -------
    str | None
        Batch ID, or None on failure.
    """
    from utility_api.agents.batch import BatchAgent

    # Budget guard: estimate cost and cap if needed
    est_cost = len(parse_tasks) * EST_COST_PER_TASK_BATCH
    if est_cost > ANTHROPIC_HARD_LIMIT_USD:
        cap = int(ANTHROPIC_HARD_LIMIT_USD / EST_COST_PER_TASK_BATCH)
        logger.warning(
            f"Capping batch from {len(parse_tasks)} to {cap} tasks "
            f"to stay within ${ANTHROPIC_HARD_LIMIT_USD} limit"
        )
        # Keep highest-population PWSIDs (tasks are already sorted by pop)
        parse_tasks = parse_tasks[:cap]

    logger.info(f"\nSubmitting {len(parse_tasks):,} parse tasks to Anthropic Batch API")
    logger.info(f"Estimated cost: ~${est_cost:.2f} (batch pricing)")

    agent = BatchAgent()
    result = agent.submit(parse_tasks=parse_tasks, state_filter="scenario_a")

    if result.get("batch_id"):
        logger.info(f"Batch submitted: {result['batch_id']}")
        logger.info(f"Task count: {result['task_count']}")
        logger.info(f"Status: {result['status']}")
        logger.info(f"\nBatch will take ~24 hours to process.")
        logger.info(f"Check status:  python scripts/run_scenario_a.py --check-status")
        logger.info(f"Process batch: python scripts/run_scenario_a.py --process-batch")
        return result["batch_id"]
    else:
        logger.error(f"Batch submission failed: {result}")
        return None


# --- Phase 6: Process Batch Results ---

def process_batch():
    """Check for completed batches and process results.

    Downloads results from Anthropic, validates parses, writes to
    rate_schedules, and triggers best_estimate rebuild per affected state.
    """
    from utility_api.agents.batch import BatchAgent

    agent = BatchAgent()

    # Check status first
    statuses = agent.check_status()
    logger.info(f"Batch status check: {len(statuses)} batches found")
    for s in statuses:
        logger.info(f"  {s.get('batch_id', '?')}: {s.get('local_status', '?')} "
                     f"({s.get('task_count', 0)} tasks)")

    # Process any completed
    completed = [s for s in statuses if s.get("local_status") == "completed"]
    if not completed:
        logger.info("No completed batches to process. Check back later.")
        # Show how many are in progress
        in_progress = [s for s in statuses if s.get("local_status") in ("pending", "in_progress")]
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
        description="Scenario A: Comprehensive gap sweep with Anthropic Batch API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full sweep: discover + scrape + submit batch
  python scripts/run_scenario_a.py 2>&1 | tee logs/scenario_a.log

  # Dry run (preview targets, no API calls)
  python scripts/run_scenario_a.py --dry-run

  # Check batch status
  python scripts/run_scenario_a.py --check-status

  # Process completed batch results
  python scripts/run_scenario_a.py --process-batch
""",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview targets and budget, no API calls")
    parser.add_argument("--check-status", action="store_true",
                        help="Check status of pending batches")
    parser.add_argument("--process-batch", action="store_true",
                        help="Process completed batch results + rebuild best_estimate")

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Scenario A: Comprehensive Gap Sweep (>=3k pop)")
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

    # --- Main sweep: discover + scrape + submit ---

    # 1. Select targets
    targets = get_target_pwsids()
    if not targets:
        logger.warning("No targets found")
        return

    # 2. Budget check
    if not check_budget(targets):
        logger.error("Budget exceeded — aborting")
        return

    # 3. State breakdown
    from collections import Counter
    state_counts = Counter(t["state_code"] for t in targets)
    reason_counts = Counter(t["reason"] for t in targets)
    logger.info(f"\nBy reason: {dict(reason_counts)}")
    logger.info(f"States: {len(state_counts)}")
    logger.info(f"Top states: {state_counts.most_common(10)}")

    if args.dry_run:
        logger.info("\n[DRY RUN] Would process the above targets. Exiting.")
        return

    # 4. Discover + scrape (Phase 1-2)
    started = datetime.now(timezone.utc)
    logger.info(f"\nStarting discovery + scrape at {started.isoformat()}")

    parse_tasks, stats = discover_and_scrape(targets)

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(f"\n=== Discovery + Scrape Complete ({elapsed/3600:.1f} hours) ===")
    logger.info(f"  Total targets:     {stats['total']:,}")
    logger.info(f"  Discovered:        {stats['discovered']:,}")
    logger.info(f"  No URLs:           {stats['no_urls']:,}")
    logger.info(f"  Scraped:           {stats['scraped']:,}")
    logger.info(f"  Parse tasks:       {stats['with_parse_task']:,}")
    logger.info(f"  Errors:            {stats['errors']:,}")
    logger.info(f"  Serper queries:    ~{stats['serper_queries']:,}")

    if not parse_tasks:
        logger.warning("No parse tasks collected — nothing to submit")
        return

    # 5. Submit batch (Phase 3-4)
    batch_id = submit_batch(parse_tasks)
    if batch_id:
        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH SUBMITTED: {batch_id}")
        logger.info(f"Tasks: {len(parse_tasks):,}")
        logger.info(f"Next step: wait ~24 hours, then run:")
        logger.info(f"  python scripts/run_scenario_a.py --process-batch")
        logger.info(f"{'='*60}")

    # Log to pipeline_runs
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, :finished, :count, :status, :notes)
        """), {
            "step": "scenario_a_discovery",
            "started": started,
            "finished": datetime.now(timezone.utc),
            "count": stats["with_parse_task"],
            "status": "success" if batch_id else "failed",
            "notes": json.dumps({
                "batch_id": batch_id,
                "targets": stats["total"],
                "discovered": stats["discovered"],
                "parse_tasks": stats["with_parse_task"],
                "serper_queries": stats["serper_queries"],
                "elapsed_hours": round(elapsed / 3600, 1),
            }),
        })
        conn.commit()


if __name__ == "__main__":
    main()

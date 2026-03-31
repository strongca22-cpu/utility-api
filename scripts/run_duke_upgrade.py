#!/usr/bin/env python3
"""
Duke-Only Upgrade Sweep — Replace Duke Reference with LLM-Scraped Rates

Purpose:
    Discovers and scrapes fresh rate data for PWSIDs where Duke NIEPS
    is the only source. Duke data is CC BY-NC-ND 4.0, dated (~2019),
    and provides only aggregate 10CCF bills without rate structure detail.

    LLM-scraped rates (premium tier) provide current rates, tiered
    structure, fixed charges, and source URLs for spot-checking. When
    scraped_llm data exists, best_estimate naturally prefers it over
    Duke (priority 3 vs priority 8 in source_priority.yaml).

    Uses Anthropic Batch API (50% cost savings).

    Designed to run AFTER Scenario A (gap sweep) completes discovery
    phase, to avoid Serper query contention.

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Dependencies:
    - utility_api.agents.discovery (DiscoveryAgent)
    - utility_api.agents.batch (BatchAgent)
    - PostgreSQL utility schema

Usage:
    # Discover + scrape + submit batch
    python scripts/run_duke_upgrade.py 2>&1 | tee logs/duke_upgrade.log

    # Dry run
    python scripts/run_duke_upgrade.py --dry-run

    # Check batch status
    python scripts/run_duke_upgrade.py --check-status

    # Process completed batch
    python scripts/run_duke_upgrade.py --process-batch

Notes:
    - 963 PWSIDs across 10 states (TX 221, CA 167, PA 158, WA 123, NJ 108)
    - Serper: ~3,852 queries
    - Anthropic batch: ~$9 estimated
    - No overlap with Scenario A targets (Duke-only have existing rate data)
    - After batch processing, best_estimate rebuild will automatically
      prefer scraped_llm over Duke where scrape succeeds
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

QUERIES_PER_PWSID = 4
MIN_POPULATION = 3000
EST_COST_PER_TASK_BATCH = 0.0098


# --- Target Selection ---

def get_duke_only_targets() -> list[dict]:
    """Select PWSIDs where Duke is the only rate source.

    Finds PWSIDs where:
    - best_estimate selected Duke as the winning source
    - n_sources = 1 (no other source exists)
    - population >= 3,000

    Returns
    -------
    list[dict]
        Each dict: {pwsid, population_served, state_code}
    """
    schema = settings.utility_schema
    targets = []

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT be.pwsid, c.population_served, c.state_code
            FROM {schema}.rate_best_estimate be
            JOIN {schema}.cws_boundaries c ON be.pwsid = c.pwsid
            WHERE be.selected_source = 'duke_nieps_10state'
            AND c.population_served >= :min_pop
            AND be.n_sources = 1
            ORDER BY c.population_served DESC
        """), {"min_pop": MIN_POPULATION}).fetchall()

        for r in rows:
            targets.append({
                "pwsid": r[0],
                "population_served": r[1],
                "state_code": r[2],
            })

    logger.info(f"Duke-only PWSIDs >= {MIN_POPULATION:,} pop: {len(targets):,}")
    return targets


# --- Discovery + Scrape (reuses Scenario A pattern) ---

def discover_and_scrape(targets: list[dict]) -> tuple[list[dict], dict]:
    """Run Serper discovery + URL scraping for all Duke-only targets.

    Same logic as Scenario A: discover → scrape → collect parse tasks.
    Does NOT call the LLM.

    Parameters
    ----------
    targets : list[dict]
        Target PWSIDs from get_duke_only_targets().

    Returns
    -------
    tuple[list[dict], dict]
        (parse_tasks, stats)
    """
    from utility_api.agents.discovery import (
        DiscoveryAgent,
        _DISCOVERY_CONFIG,
        score_url_relevance,
    )
    from utility_api.pipeline.process import (
        _ensure_fetched,
        _get_starting_urls,
        _get_system_metadata,
    )

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

        if (i + 1) % 50 == 0:
            logger.info(
                f"\n--- Progress: {i+1}/{len(targets)} "
                f"({stats['with_parse_task']} parse tasks, "
                f"{stats['no_urls']} no URLs) ---"
            )

        # Step 1: Discovery
        try:
            discovery_agent.run(pwsid=pwsid, diagnostic=False)
            stats["serper_queries"] += QUERIES_PER_PWSID
        except Exception as e:
            logger.warning(f"  {pwsid}: discovery error: {e}")
            stats["errors"] += 1
            continue

        # Step 2: Get starting URLs
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

        all_candidates.sort(key=lambda c: c["rescore"], reverse=True)
        parseable = [
            c for c in all_candidates
            if c["rescore"] >= 30 and c.get("text_len") and c["text_len"] > 100
        ]

        if not parseable:
            stats["no_urls"] += 1
            continue

        stats["scraped"] += 1

        # Step 5: Collect best candidate as parse task
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


# --- Submit Batch ---

def submit_batch(parse_tasks: list[dict]) -> str | None:
    """Submit parse tasks to Anthropic Batch API."""
    from utility_api.agents.batch import BatchAgent

    est_cost = len(parse_tasks) * EST_COST_PER_TASK_BATCH
    logger.info(f"\nSubmitting {len(parse_tasks):,} parse tasks to Anthropic Batch API")
    logger.info(f"Estimated cost: ~${est_cost:.2f} (batch pricing)")

    agent = BatchAgent()
    result = agent.submit(parse_tasks=parse_tasks, state_filter="duke_upgrade")

    if result.get("batch_id"):
        logger.info(f"Batch submitted: {result['batch_id']}")
        logger.info(f"Task count: {result['task_count']}")
        logger.info(f"\nNext step: wait ~24 hours, then run:")
        logger.info(f"  python scripts/run_duke_upgrade.py --process-batch")
        return result["batch_id"]
    else:
        logger.error(f"Batch submission failed: {result}")
        return None


# --- Process Batch ---

def process_batch():
    """Check for completed Duke upgrade batches and process results."""
    from utility_api.agents.batch import BatchAgent

    agent = BatchAgent()
    statuses = agent.check_status()
    logger.info(f"Batch status check: {len(statuses)} batches found")
    for s in statuses:
        logger.info(f"  {s.get('batch_id', '?')}: {s.get('local_status', '?')} "
                     f"({s.get('task_count', 0)} tasks)")

    completed = [s for s in statuses if s.get("local_status") == "completed"]
    if not completed:
        logger.info("No completed batches. Check back later.")
        in_progress = [s for s in statuses if s.get("local_status") in ("pending", "in_progress")]
        if in_progress:
            for s in in_progress:
                logger.info(f"  {s['batch_id']}: {s.get('api_status', 'unknown')}")
        return

    for batch_info in completed:
        batch_id = batch_info["batch_id"]
        logger.info(f"\nProcessing batch {batch_id}...")
        result = agent.process_batch(batch_id)
        logger.info(f"  Succeeded: {result.get('succeeded', 0)}")
        logger.info(f"  Failed: {result.get('failed', 0)}")
        logger.info(f"  Cost: ${result.get('total_cost', 0):.4f}")

    logger.info("\n=== Duke upgrade batch processing complete ===")
    logger.info("Best estimate rebuilt for affected states.")
    logger.info("Scraped_llm rates will automatically supersede Duke (priority 3 vs 8).")


# --- Main ---

def main():
    parser = argparse.ArgumentParser(
        description="Duke-only upgrade sweep: replace Duke reference data with LLM-scraped rates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_duke_upgrade.py --dry-run
  python scripts/run_duke_upgrade.py 2>&1 | tee logs/duke_upgrade.log
  python scripts/run_duke_upgrade.py --check-status
  python scripts/run_duke_upgrade.py --process-batch
""",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview targets, no API calls")
    parser.add_argument("--check-status", action="store_true",
                        help="Check batch status")
    parser.add_argument("--process-batch", action="store_true",
                        help="Process completed batch + rebuild best_estimate")

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Duke-Only Upgrade Sweep (Batch API)")
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

    # --- Main sweep ---

    # 1. Select targets
    targets = get_duke_only_targets()
    if not targets:
        logger.warning("No Duke-only targets found")
        return

    # 2. Budget check
    total_queries = len(targets) * QUERIES_PER_PWSID
    est_parse = int(len(targets) * 0.89 * 0.65)
    est_cost = est_parse * EST_COST_PER_TASK_BATCH
    logger.info(f"\n=== Budget ===")
    logger.info(f"Serper queries: {total_queries:,}")
    logger.info(f"Est. parse tasks: ~{est_parse:,}")
    logger.info(f"Est. Anthropic batch cost: ~${est_cost:.2f}")

    # 3. State breakdown
    from collections import Counter
    state_counts = Counter(t["state_code"] for t in targets)
    logger.info(f"\nStates: {len(state_counts)}")
    for st, n in state_counts.most_common():
        logger.info(f"  {st}: {n}")

    if args.dry_run:
        logger.info("\n[DRY RUN] Exiting.")
        return

    # 4. Discover + scrape
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
        logger.warning("No parse tasks collected")
        return

    # 5. Submit batch
    batch_id = submit_batch(parse_tasks)
    if batch_id:
        logger.info(f"\n{'='*60}")
        logger.info(f"DUKE UPGRADE BATCH SUBMITTED: {batch_id}")
        logger.info(f"Tasks: {len(parse_tasks):,}")
        logger.info(f"Next: python scripts/run_duke_upgrade.py --process-batch")
        logger.info(f"{'='*60}")

    # Log pipeline run
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, :finished, :count, :status, :notes)
        """), {
            "step": "duke_upgrade_discovery",
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
            }),
        })
        conn.commit()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Process Pending Domain-Guessed URLs Through Pipeline

Purpose:
    Processes domain-guesser URLs through scrape -> filter -> parse,
    separate from the main orchestrator's SearXNG flow. The orchestrator
    handles SearXNG gap-fill; this script handles domain-guesser bulk
    processing.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - sqlalchemy
    - utility_api agents (scrape, parse, best_estimate)

Usage:
    python scripts/process_guesser_batch.py              # process 50 (default)
    python scripts/process_guesser_batch.py --max 200    # process 200
    python scripts/process_guesser_batch.py --state AL   # AL only
    python scripts/process_guesser_batch.py --dry-run    # show what would run

Notes:
    - Uses pre-parse content filter (saves ~74% of API cost on junk)
    - Multi-level deep crawl enabled (default depth 3)
    - Processes by population descending (biggest utilities first)
    - Logs to /var/log/uapi/guesser_processing.log
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.parse import ParseAgent
from utility_api.agents.scrape import ScrapeAgent
from utility_api.config import settings
from utility_api.db import engine

# Lazy import — only needed in --batch mode
# from utility_api.agents.batch import BatchAgent

schema = settings.utility_schema

# Add file logging
LOG_PATH = Path("/var/log/uapi/guesser_processing.log")
if LOG_PATH.parent.exists():
    logger.add(str(LOG_PATH), rotation="10 MB", retention="30 days")


def get_pending_guesser_urls(
    max_count: int = 50, state: str | None = None,
    url_source: str = "domain_guesser",
) -> list[dict]:
    """Get pending URLs, ordered by population (biggest first)."""
    query = f"""
        SELECT sr.id as registry_id, sr.pwsid, sr.url, pc.population_served,
               pc.pws_name, pc.state_code
        FROM {schema}.scrape_registry sr
        JOIN {schema}.pwsid_coverage pc ON pc.pwsid = sr.pwsid
        WHERE sr.url_source = :url_source
        AND sr.status = 'pending'
    """
    params: dict = {"url_source": url_source}

    if state:
        query += " AND pc.state_code = :state"
        params["state"] = state.upper()

    query += " ORDER BY pc.population_served DESC NULLS LAST LIMIT :max_count"
    params["max_count"] = max_count

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [dict(row._mapping) for row in result]


def process_batch(
    max_count: int = 50,
    state: str | None = None,
    dry_run: bool = False,
    url_source: str = "domain_guesser",
    use_batch_api: bool = False,
) -> dict:
    """Process a batch of pending URLs through scrape → parse pipeline.

    In default mode: scrape + parse synchronously per URL.
    With use_batch_api=True: scrape synchronously, then submit all parse
    tasks to the Anthropic Batch API (50% cheaper, async results).
    """
    pending = get_pending_guesser_urls(max_count, state, url_source=url_source)

    if not pending:
        logger.info("No pending URLs to process")
        return {"processed": 0}

    state_label = f" (state={state})" if state else ""
    mode_label = " [BATCH API]" if use_batch_api else ""
    logger.info(
        f"Processing {len(pending)} {url_source} URLs{state_label}"
        f"{' [DRY RUN]' if dry_run else mode_label}"
    )

    if dry_run:
        for p in pending[:20]:
            pop = p["population_served"] or "?"
            logger.info(
                f"  {p['pwsid']} | {p['pws_name'][:30]:30s} | "
                f"pop {pop:>7} | {p['url'][:50]}"
            )
        if len(pending) > 20:
            logger.info(f"  ... +{len(pending) - 20} more")
        return {"processed": 0, "dry_run": True}

    scrape = ScrapeAgent()

    stats = {
        "processed": 0,
        "scrape_ok": 0,
        "filtered": 0,
        "parse_ok": 0,
        "parse_failed": 0,
        "scrape_failed": 0,
        "total_cost": 0.0,
    }

    # In batch mode, collect parse tasks; in immediate mode, parse inline
    pending_parse_tasks = []
    parse = None if use_batch_api else ParseAgent()

    for item in pending:
        pwsid = item["pwsid"]
        stats["processed"] += 1

        try:
            # Scrape (free — just HTTP)
            scrape_result = scrape.run(pwsid=pwsid)

            if not scrape_result.get("raw_texts"):
                stats["scrape_failed"] += 1
                continue

            stats["scrape_ok"] += 1
            text_entry = scrape_result["raw_texts"][0]

            if use_batch_api:
                # Collect for batch submission
                pending_parse_tasks.append({
                    "pwsid": pwsid,
                    "raw_text": text_entry["text"],
                    "content_type": text_entry["content_type"],
                    "source_url": text_entry["url"],
                    "registry_id": text_entry.get("registry_id", item["registry_id"]),
                })
            else:
                # Parse immediately
                parse_result = parse.run(
                    pwsid=pwsid,
                    raw_text=text_entry["text"],
                    content_type=text_entry["content_type"],
                    source_url=text_entry["url"],
                    registry_id=text_entry.get("registry_id", item["registry_id"]),
                )

                cost = parse_result.get("cost_usd", 0)
                stats["total_cost"] += cost

                if parse_result.get("skipped"):
                    stats["filtered"] += 1
                elif parse_result.get("success"):
                    stats["parse_ok"] += 1
                    bill = parse_result.get("bill_10ccf")
                    bill_str = f"${bill:.2f}" if bill else "N/A"
                    logger.info(
                        f"  \u2713 {pwsid} | {item['pws_name'][:30]} | "
                        f"bill@10CCF={bill_str}"
                    )
                else:
                    stats["parse_failed"] += 1

        except Exception as e:
            logger.error(f"  Error processing {pwsid}: {e}")
            continue

        # Progress every 50 utilities
        if stats["processed"] % 50 == 0:
            logger.info(
                f"  Progress: {stats['processed']}/{len(pending)} | "
                f"scrape_ok={stats['scrape_ok']} failed={stats['scrape_failed']}"
            )

    # Batch mode: submit all collected parse tasks
    if use_batch_api and pending_parse_tasks:
        from utility_api.agents.batch import BatchAgent

        logger.info(
            f"\nSubmitting {len(pending_parse_tasks)} parse tasks to Batch API "
            f"(50% cost savings)"
        )
        batch_agent = BatchAgent()
        batch_result = batch_agent.submit(
            parse_tasks=pending_parse_tasks,
            state_filter=state,
        )

        if batch_result.get("batch_id"):
            logger.info(f"  Batch submitted: {batch_result['batch_id']}")
            logger.info(f"  Tasks: {batch_result['task_count']}")
            logger.info(f"  Check with: ua-ops batch-status {batch_result['batch_id']}")
            logger.info(f"  Collect with: ua-ops process-batches")
        else:
            logger.error(
                f"  Batch submission failed: {batch_result.get('error', 'unknown')}"
            )

    # Final summary
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Batch Processing Complete ({url_source})")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Processed:         {stats['processed']}")
    logger.info(f"  Scrape OK:         {stats['scrape_ok']}")
    logger.info(f"  Scrape failed:     {stats['scrape_failed']}")

    if use_batch_api:
        logger.info(f"  Parse tasks queued: {len(pending_parse_tasks)}")
    else:
        parsed_total = stats["parse_ok"] + stats["parse_failed"]
        success_rate = (
            f"{100 * stats['parse_ok'] / parsed_total:.1f}%"
            if parsed_total > 0
            else "N/A"
        )
        filter_rate = (
            f"{100 * stats['filtered'] / stats['processed']:.1f}%"
            if stats["processed"] > 0
            else "N/A"
        )
        logger.info(f"  Pre-parse filtered:{stats['filtered']}")
        logger.info(f"  Parse succeeded:   {stats['parse_ok']}")
        logger.info(f"  Parse failed:      {stats['parse_failed']}")
        logger.info(f"  Success rate:      {success_rate} (of parsed)")
        logger.info(f"  Filter rate:       {filter_rate}")
        logger.info(f"  Total API cost:    ${stats['total_cost']:.2f}")

    logger.info(f"{'=' * 60}")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process pending domain-guessed URLs through pipeline"
    )
    parser.add_argument(
        "--max", type=int, default=50, help="Max URLs to process (default: 50)"
    )
    parser.add_argument("--state", help="Filter to single state code")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be processed"
    )
    parser.add_argument(
        "--url-source", default="domain_guesser",
        help="URL source to process (domain_guesser, metro_research, etc.)",
    )
    parser.add_argument(
        "--batch", action="store_true",
        help="Submit parse tasks to Batch API (50%% cheaper, async results)",
    )
    args = parser.parse_args()

    process_batch(
        max_count=args.max, state=args.state, dry_run=args.dry_run,
        url_source=args.url_source, use_batch_api=args.batch,
    )

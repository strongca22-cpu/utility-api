#!/usr/bin/env python3
"""
Gap Cascade: Re-parse Remaining High-Pop PWSIDs Through Untried Ranks

Purpose:
    For the ~640 PWSIDs (>=3k pop) that have scraped text but no rate,
    find the best UNTRIED rank URL (non-blacklisted, longest content)
    and submit to Sonnet for parsing. This is the highest-ROI batch
    remaining — every recovery has outsized population impact.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - utility_api.ingest.rate_parser (DOMAIN_BLACKLIST, SYSTEM_PROMPT,
      build_parse_user_message)
    - utility_api.agents.batch (BatchAgent)
    - PostgreSQL utility schema

Usage:
    python scripts/run_gap_cascade.py --dry-run
    python scripts/run_gap_cascade.py 2>&1 | tee logs/gap_cascade.log

Notes:
    - Sonnet-only (no Haiku) — these are the hardest content
    - Domain blacklist applied — skips near-100% failure domains
    - Prioritizes longest content per PWSID (length > score for prediction)
    - Reports PWSIDs with no viable untried URLs separately
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_parser import DOMAIN_BLACKLIST


SONNET_MODEL = "claude-sonnet-4-20250514"
MIN_TEXT_LENGTH = 200
BATCH_LABEL = "gap_cascade_v1"


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def get_gap_targets() -> dict:
    """Find gap PWSIDs >= 3k pop and their best untried rank URL.

    Returns dict with keys:
        submittable: list of task dicts (have viable untried URL)
        exhausted: list of PWSIDs with no untried URLs
        blacklisted_only: list of PWSIDs where only blacklisted URLs remain
        ny_portal_only: list of NY PWSIDs with only ny.gov/nyc.gov sources
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        # All gap PWSIDs >= 3k pop
        gap_pwsids = conn.execute(text(f"""
            SELECT c.pwsid, c.pws_name, c.state_code, c.population_served
            FROM {schema}.cws_boundaries c
            WHERE c.population_served >= 3000
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.rate_best_estimate rbe WHERE rbe.pwsid = c.pwsid
              )
            ORDER BY c.population_served DESC
        """)).fetchall()

        logger.info(f"Gap PWSIDs >= 3k: {len(gap_pwsids):,}")

        submittable = []
        exhausted = []
        blacklisted_only = []
        ny_portal_only = []

        for gap in gap_pwsids:
            pwsid = gap.pwsid

            # Get all URLs for this PWSID with scraped text
            urls = conn.execute(text(f"""
                SELECT sr.id AS registry_id, sr.url, sr.discovery_rank,
                       sr.scraped_text, sr.last_content_length,
                       sr.content_type, sr.last_parse_result, sr.url_source
                FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = :pwsid
                  AND sr.scraped_text IS NOT NULL
                  AND LENGTH(sr.scraped_text) >= :min_len
                  AND sr.status != 'dead'
                ORDER BY sr.last_content_length DESC
            """), {"pwsid": pwsid, "min_len": MIN_TEXT_LENGTH}).fetchall()

            if not urls:
                exhausted.append({
                    "pwsid": pwsid, "pws_name": gap.pws_name,
                    "state_code": gap.state_code, "population_served": gap.population_served,
                    "reason": "no_text_available",
                })
                continue

            # Find best untried, non-blacklisted URL (longest content first)
            best_untried = None
            has_non_blacklisted = False
            all_blacklisted = True
            ny_portal_count = 0
            total_urls = len(urls)

            for u in urls:
                domain = get_domain(u.url)
                is_blacklisted = domain in DOMAIN_BLACKLIST
                is_ny_portal = domain in ("www.ny.gov", "www.nyc.gov")

                if not is_blacklisted:
                    all_blacklisted = False
                if is_ny_portal:
                    ny_portal_count += 1

                # Skip if already parsed (success or fail — we want UNTRIED)
                if u.last_parse_result is not None:
                    continue
                # Skip blacklisted domains
                if is_blacklisted:
                    continue

                # This is a viable untried URL
                if best_untried is None:
                    best_untried = u

            if best_untried is not None:
                submittable.append({
                    "pwsid": pwsid,
                    "pws_name": gap.pws_name,
                    "state_code": gap.state_code,
                    "population_served": gap.population_served,
                    "registry_id": best_untried.registry_id,
                    "url": best_untried.url,
                    "discovery_rank": best_untried.discovery_rank,
                    "content_type": best_untried.content_type or "html",
                    "text_length": best_untried.last_content_length,
                    "raw_text": best_untried.scraped_text[:45000],
                })
            elif all_blacklisted:
                blacklisted_only.append({
                    "pwsid": pwsid, "pws_name": gap.pws_name,
                    "state_code": gap.state_code, "population_served": gap.population_served,
                    "reason": "all_urls_blacklisted",
                })
            elif gap.state_code == "NY" and ny_portal_count == total_urls:
                ny_portal_only.append({
                    "pwsid": pwsid, "pws_name": gap.pws_name,
                    "state_code": gap.state_code, "population_served": gap.population_served,
                    "reason": "ny_portal_only",
                })
            else:
                exhausted.append({
                    "pwsid": pwsid, "pws_name": gap.pws_name,
                    "state_code": gap.state_code, "population_served": gap.population_served,
                    "reason": "all_ranks_tried",
                })

    return {
        "submittable": submittable,
        "exhausted": exhausted,
        "blacklisted_only": blacklisted_only,
        "ny_portal_only": ny_portal_only,
    }


def print_summary(targets: dict) -> None:
    """Print the pre-submission summary report."""
    sub = targets["submittable"]
    exh = targets["exhausted"]
    bl = targets["blacklisted_only"]
    ny = targets["ny_portal_only"]

    total_gap = len(sub) + len(exh) + len(bl) + len(ny)

    # Count blacklisted URLs skipped
    bl_skipped = sum(1 for t in sub if get_domain(t["url"]) in DOMAIN_BLACKLIST)

    content_types = Counter(t["content_type"] for t in sub)
    lengths = sorted(t["text_length"] for t in sub) if sub else [0]
    states = Counter(t["state_code"] for t in sub)

    est_cost = len(sub) * 0.007  # Sonnet batch pricing

    logger.info(f"\n{'='*60}")
    logger.info(f"GAP CASCADE BATCH SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Total gap PWSIDs (>=3k pop):                {total_gap:,}")
    logger.info(f"  Have untried rank URLs (submitting):        {len(sub):,} → SUBMITTING")
    logger.info(f"  All ranks exhausted (no untried URLs):      {len(exh):,} → NEEDS FRESH DISCOVERY")
    logger.info(f"  All URLs from blacklisted domains:          {len(bl):,} → NEEDS FRESH DISCOVERY")
    logger.info(f"  NY portal-only (ny.gov/nyc.gov):            {len(ny):,} → NEEDS NY DISCOVERY PASS")
    logger.info(f"")
    logger.info(f"  Batch details:")
    logger.info(f"    Tasks: {len(sub):,}")
    logger.info(f"    Model: Sonnet (claude-sonnet-4)")
    logger.info(f"    Est. cost: ${est_cost:.2f} (at ~$0.007/task)")
    logger.info(f"    Content type: {content_types.get('pdf', 0)} PDF, {content_types.get('html', 0)} HTML")
    if lengths:
        logger.info(f"    Median text length: {lengths[len(lengths)//2]:,} chars")
        logger.info(f"    P25: {lengths[len(lengths)//4]:,}, P75: {lengths[3*len(lengths)//4]:,}")

    logger.info(f"")
    logger.info(f"  Top 10 by population (highest-value recoveries):")
    for t in sub[:10]:
        logger.info(
            f"    {t['pwsid']} {t['state_code']} {t['population_served']:>10,} "
            f"{t['pws_name'][:30]:30s} rank={t['discovery_rank']} "
            f"{t['text_length']:>6,}ch"
        )

    if exh:
        logger.info(f"\n  Exhausted PWSIDs (top 5 by pop):")
        for e in sorted(exh, key=lambda x: x["population_served"], reverse=True)[:5]:
            logger.info(f"    {e['pwsid']} {e['state_code']} {e['population_served']:>10,} {e['pws_name'][:35]} ({e['reason']})")

    if ny:
        logger.info(f"\n  NY portal-only PWSIDs: {len(ny)}")
        for n in sorted(ny, key=lambda x: x["population_served"], reverse=True)[:5]:
            logger.info(f"    {n['pwsid']} {n['state_code']} {n['population_served']:>10,} {n['pws_name'][:35]}")


def submit_batch(targets: list[dict]) -> str | None:
    """Submit gap cascade batch — Sonnet only."""
    from utility_api.agents.batch import BatchAgent
    from utility_api.ingest.rate_parser import SYSTEM_PROMPT, build_parse_user_message

    # Build batch requests manually to force Sonnet model
    import anthropic
    client = anthropic.Anthropic()

    batch_requests = []
    task_details = []

    for t in targets:
        user_message = build_parse_user_message(
            t["raw_text"],
            utility_name=t.get("pws_name", ""),
            state_code=t.get("state_code", ""),
            content_type=t.get("content_type", "html"),
        )

        batch_requests.append({
            "custom_id": f"{t['pwsid']}_{t['registry_id']}",
            "params": {
                "model": SONNET_MODEL,
                "max_tokens": 1024,
                "system": [{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                "messages": [
                    {"role": "user", "content": user_message},
                    {"role": "assistant", "content": "{"},
                ],
            },
        })

        task_details.append({
            "pwsid": t["pwsid"],
            "registry_id": t["registry_id"],
            "source_url": t.get("url", ""),
            "content_type": t.get("content_type", "html"),
            "model": SONNET_MODEL,
            "text_length": t.get("text_length", 0),
        })

    logger.info(f"\nSubmitting {len(batch_requests):,} tasks to Anthropic Batch API (Sonnet)")

    try:
        batch = client.messages.batches.create(requests=batch_requests)
        batch_id = batch.id
        logger.info(f"  Batch submitted: {batch_id}")
    except Exception as e:
        logger.error(f"  Batch submission failed: {e}")
        return None

    # Record in batch_jobs table
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.batch_jobs
                (batch_id, submitted_at, task_count, status, task_details, state_filter)
            VALUES (:batch_id, :now, :count, 'pending', :details, :label)
        """), {
            "batch_id": batch_id,
            "now": datetime.now(timezone.utc),
            "count": len(batch_requests),
            "details": json.dumps(task_details),
            "label": BATCH_LABEL,
        })
        conn.commit()

    return batch_id


def main():
    parser = argparse.ArgumentParser(
        description="Gap cascade: re-parse remaining high-pop PWSIDs through untried ranks (Sonnet)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show summary only, do not submit batch")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Gap Cascade — Untried Ranks, Sonnet Only")
    logger.info("=" * 60)

    targets = get_gap_targets()
    print_summary(targets)

    if args.dry_run:
        logger.info(f"\n[DRY RUN] Would submit {len(targets['submittable']):,} tasks. Exiting.")
        return

    if not targets["submittable"]:
        logger.warning("No submittable tasks found.")
        return

    batch_id = submit_batch(targets["submittable"])

    if batch_id:
        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH SUBMITTED: {batch_id}")
        logger.info(f"Tasks: {len(targets['submittable']):,}")
        logger.info(f"Model: Sonnet")
        logger.info(f"Label: {BATCH_LABEL}")
        logger.info(f"{'='*60}")

    # Log to pipeline_runs
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :now, :now, :count, :status, :notes)
        """), {
            "step": "gap_cascade_v1",
            "now": datetime.now(timezone.utc),
            "count": len(targets["submittable"]),
            "status": "success" if batch_id else "failed",
            "notes": json.dumps({
                "batch_id": batch_id,
                "submittable": len(targets["submittable"]),
                "exhausted": len(targets["exhausted"]),
                "blacklisted_only": len(targets["blacklisted_only"]),
                "ny_portal_only": len(targets["ny_portal_only"]),
                "model": SONNET_MODEL,
            }),
        })
        conn.commit()


if __name__ == "__main__":
    main()

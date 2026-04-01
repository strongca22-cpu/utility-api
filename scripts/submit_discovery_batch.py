#!/usr/bin/env python3
"""
Submit Discovery Batch — Scrape + Batch Parse for New URLs

Purpose:
    Takes PWSIDs that have Serper URLs but no scraped text or no parsed
    rates, scrapes the URLs, then submits the best candidate per PWSID
    to the Anthropic Batch API for parsing.

    This is the batch equivalent of process_pwsid() but without the
    cascade retry — each PWSID gets its single best-scored URL submitted.
    For cascade retries on failures, use submit_cascade_batch.py after
    this batch completes.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - utility_api (local package)
    - ANTHROPIC_API_KEY in .env

Usage:
    python scripts/submit_discovery_batch.py --dry-run
    python scripts/submit_discovery_batch.py
    python scripts/submit_discovery_batch.py --min-pop 1000

Notes:
    - Scrapes URLs that don't have text yet (ScrapeAgent, HTTP only)
    - Re-scores with content-aware boost after scraping
    - Submits best candidate per PWSID to batch
    - Applies section extraction for multi-area PDFs
    - Batch API: 50% cost, ~24hr SLA
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.batch import BatchAgent
from utility_api.agents.discovery import score_url_relevance, _DISCOVERY_CONFIG
from utility_api.config import settings
from utility_api.db import engine
from utility_api.utils.content_scoring import compute_content_boost


def get_candidates(min_pop: int = 0) -> list[dict]:
    """Get PWSIDs with Serper URLs but no scraped_llm rate.

    Returns one row per PWSID-URL combination, all ranks.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id as registry_id, sr.pwsid, sr.url, sr.scraped_text,
                   sr.content_type, sr.discovery_rank, sr.discovery_score,
                   LENGTH(sr.scraped_text) as text_len,
                   c.population_served, c.state_code, c.pws_name
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.url_source = :src
              AND c.population_served >= :min_pop
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = sr.pwsid AND rs.source_key = :llm
              )
            ORDER BY c.population_served DESC, sr.pwsid, sr.discovery_rank
        """), {"src": "serper", "llm": "scraped_llm", "min_pop": min_pop}).fetchall()

    return [dict(r._mapping) for r in rows]


def scrape_url(registry_id: int, url: str, pwsid: str) -> dict | None:
    """Scrape a URL and update scrape_registry with text.

    Returns dict with scraped_text, content_type, text_len or None on failure.
    """
    try:
        from utility_api.agents.scrape import ScrapeAgent
        agent = ScrapeAgent()
        result = agent.fetch(url, max_depth=0)
        if result and result.get("text") and len(result["text"]) > 100:
            # Write text back to scrape_registry
            schema = settings.utility_schema
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry
                    SET scraped_text = :text, content_type = :ctype, status = 'active'
                    WHERE id = :id
                """), {
                    "text": result["text"],
                    "ctype": result.get("content_type", "html"),
                    "id": registry_id,
                })
                conn.commit()
            return {
                "scraped_text": result["text"],
                "content_type": result.get("content_type", "html"),
                "text_len": len(result["text"]),
            }
    except Exception as e:
        logger.debug(f"  Scrape failed for {pwsid} {url[:50]}: {e}")
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Scrape new discovery URLs and submit batch for parsing"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-pop", type=int, default=0,
                        help="Minimum population filter")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Only use URLs that already have scraped text")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Discovery Batch — Scrape + Submit for New URLs")
    logger.info("=" * 60)

    candidates = get_candidates(min_pop=args.min_pop)
    pwsid_set = set(c["pwsid"] for c in candidates)
    logger.info(f"Candidates: {len(candidates)} URLs across {len(pwsid_set)} PWSIDs")

    if not candidates:
        logger.info("No candidates.")
        return

    # Group by PWSID
    by_pwsid = {}
    for c in candidates:
        by_pwsid.setdefault(c["pwsid"], []).append(c)

    # For each PWSID: scrape if needed, re-score, pick best
    parse_tasks = []
    skipped_no_text = 0
    scraped_new = 0

    for i, (pwsid, urls) in enumerate(by_pwsid.items()):
        if (i + 1) % 200 == 0:
            logger.info(f"  Progress: {i+1}/{len(by_pwsid)} PWSIDs, "
                        f"{len(parse_tasks)} tasks, {scraped_new} scraped")

        best = None
        best_score = -1

        for u in urls:
            # Scrape if no text
            if not u.get("scraped_text") or (u.get("text_len") or 0) < 100:
                if args.skip_scrape or args.dry_run:
                    continue
                result = scrape_url(u["registry_id"], u["url"], pwsid)
                if result:
                    u["scraped_text"] = result["scraped_text"]
                    u["content_type"] = result["content_type"]
                    u["text_len"] = result["text_len"]
                    scraped_new += 1
                else:
                    continue

            # Re-score with content boost
            state = u.get("state_code", pwsid[:2])
            pws_name = u.get("pws_name", "")
            snippet = (u["scraped_text"] or "")[:200]
            base = score_url_relevance(
                url=u["url"], title="", snippet=snippet,
                utility_name=pws_name, state=state,
            )
            boost = compute_content_boost(u.get("scraped_text", ""))
            score = min(base + boost, 100)

            if score > best_score and score >= 30:
                best_score = score
                best = u

        if best and best.get("scraped_text"):
            raw_text = best["scraped_text"]

            # Section extraction for multi-area PDFs
            try:
                from utility_api.ingest.rate_scraper import extract_service_area_section
                pws_name = best.get("pws_name", "")
                section = extract_service_area_section(raw_text, pws_name)
                if section:
                    raw_text = section
            except Exception:
                pass

            parse_tasks.append({
                "pwsid": pwsid,
                "raw_text": raw_text[:45000],
                "content_type": best.get("content_type", "html"),
                "source_url": best["url"],
                "registry_id": best["registry_id"],
            })
        else:
            skipped_no_text += 1

    logger.info(f"\nParse tasks collected: {len(parse_tasks)}")
    logger.info(f"Scraped new: {scraped_new}")
    logger.info(f"Skipped (no viable text): {skipped_no_text}")
    logger.info(f"Est. cost: ${len(parse_tasks) * 0.002:.2f} (batch pricing)")

    if args.dry_run or not parse_tasks:
        return

    # Submit batch
    logger.info(f"\nSubmitting {len(parse_tasks)} tasks to Batch API...")
    agent = BatchAgent()
    result = agent.submit(parse_tasks=parse_tasks, state_filter="discovery_batch")

    if result.get("batch_id"):
        batch_id = result["batch_id"]
        logger.info(f"\nBatch submitted: {batch_id}")
        logger.info(f"Tasks: {result.get('task_count', len(parse_tasks))}")
        logger.info(f"\nProcess when complete:")
        logger.info(f"  python scripts/process_scenario_a_batch.py --batch-id {batch_id}")
    else:
        logger.error(f"Submission failed: {result}")


if __name__ == "__main__":
    main()

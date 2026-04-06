#!/usr/bin/env python3
"""
WS1 CO Gap — Scrape Pending Locality URLs + Batch Parse (Sprint 30)

Purpose:
    Closes the WS1 auto-parse queue for 5 CO PWSIDs (~56k pop).
    Parse_sweep stalled on these — wrong-entity URLs contaminated the
    active queue, and high-confidence locality_discovery URLs are stuck
    in 'pending' status (never fetched).

    This script:
    1. Scrapes all pending locality_discovery URLs for the 5 PWSIDs
    2. Collects the best scraped URL per PWSID (correct entity, most content)
    3. Submits as one Anthropic batch (~$0.05 at batch pricing)

    Target PWSIDs:
    - CO0103614  Platte Canyon WSD     19,485 pop
    - CO0107725  Superior MD No 1      17,900 pop
    - CO0144005  Fort Morgan            12,000 pop
    - CO0163020  Yuma                    4,049 pop
    - CO0139600  Palisade               3,060 pop

Author: AI-Generated
Created: 2026-04-03
Modified: 2026-04-03

Dependencies:
    - utility_api (local package)
    - anthropic (for batch submission)

Usage:
    # Dry run — show what would be scraped and batched
    python scripts/scrape_batch_ws1_co.py --dry-run

    # Run: scrape + submit batch
    python scripts/scrape_batch_ws1_co.py

    # Check batch status after ~24h
    python scripts/scrape_batch_ws1_co.py --check-status

    # Process completed batch
    python scripts/scrape_batch_ws1_co.py --process-batch

Notes:
    - Excludes known wrong-state URLs (yumacountyaz.gov, azwater.com, etc.)
    - Picks best URL per PWSID by: correct entity > content length > recency
    - Uses ScrapeAgent with max_depth=0 (no deep crawl — these are targeted URLs)
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.batch import BatchAgent
from utility_api.agents.scrape import ScrapeAgent
from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema

# Target PWSIDs for WS1
TARGET_PWSIDS = [
    "CO0103614",  # Platte Canyon WSD
    "CO0107725",  # Superior MD No 1
    "CO0144005",  # Fort Morgan
    "CO0163020",  # Yuma
    "CO0139600",  # Palisade
]

# Wrong-entity domain patterns to exclude from batch candidates
WRONG_ENTITY_DOMAINS = [
    "yumacountyaz.gov",     # Yuma, AZ (not Yuma, CO)
    "azwater.com",          # Arizona water
    "yuma.org",             # Yuma AZ school district
    "mesa.gov",             # Mesa, AZ
    "castlewoodwsd",        # Wrong district for Platte Canyon
    "semswa.org",           # Stormwater entity, not water utility
    "columbinewsd",         # Wrong district
    "niwot",                # Niwot SD, not Superior
    "fmc.com",              # FMC Corporation (chemical company)
    "kentucky",             # Kentucky PSC (wrong state)
    "mcqwd.org",            # Morgan County Quality WD (different PWSID)
    "morgan-county-ky",     # Kentucky (wrong state)
    "boulder",              # Boulder County (not Superior MD)
    "mesa-cortina",         # Mesa-Cortina WD (different district)
]


def is_wrong_entity(url: str) -> bool:
    """Check if URL is from a known wrong-entity domain."""
    url_lower = url.lower()
    return any(domain in url_lower for domain in WRONG_ENTITY_DOMAINS)


def get_pending_urls() -> list[dict]:
    """Get pending locality_discovery URLs for target PWSIDs."""
    pwsid_list = ",".join(f"'{p}'" for p in TARGET_PWSIDS)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, pwsid, url, status, url_source
            FROM {schema}.scrape_registry
            WHERE pwsid IN ({pwsid_list})
              AND status IN ('pending', 'pending_retry')
            ORDER BY pwsid, id
        """)).fetchall()
    return [dict(r._mapping) for r in rows]


def get_scraped_candidates() -> list[dict]:
    """Get all scraped URLs with content for target PWSIDs.

    Returns URLs that have scraped_text > 500 chars and haven't been
    successfully parsed. Includes both previously-scraped and newly-scraped.
    """
    pwsid_list = ",".join(f"'{p}'" for p in TARGET_PWSIDS)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id, sr.pwsid, sr.url, sr.status, sr.url_source,
                   sr.content_type,
                   char_length(sr.scraped_text) as content_len,
                   sr.scraped_text,
                   sr.last_parse_result,
                   s.pws_name
            FROM {schema}.scrape_registry sr
            JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
            WHERE sr.pwsid IN ({pwsid_list})
              AND sr.scraped_text IS NOT NULL
              AND char_length(sr.scraped_text) > 500
              AND sr.status = 'active'
            ORDER BY sr.pwsid, char_length(sr.scraped_text) DESC
        """)).fetchall()
    return [dict(r._mapping) for r in rows]


def select_best_per_pwsid(candidates: list[dict]) -> list[dict]:
    """Select the best scraped URL per PWSID for batch submission.

    Priority: correct entity > content length > never-parsed over failed.
    """
    by_pwsid: dict[str, list[dict]] = {}
    for c in candidates:
        by_pwsid.setdefault(c["pwsid"], []).append(c)

    best = []
    for pwsid in TARGET_PWSIDS:
        options = by_pwsid.get(pwsid, [])
        if not options:
            logger.warning(f"  {pwsid}: NO scraped candidates available")
            continue

        # Filter out wrong-entity URLs
        correct = [o for o in options if not is_wrong_entity(o["url"])]
        if not correct:
            logger.warning(
                f"  {pwsid}: all {len(options)} candidates are wrong-entity"
            )
            continue

        # Prefer unparsed over failed (fresh attempt), then by content length
        correct.sort(
            key=lambda x: (
                0 if x["last_parse_result"] is None else 1,
                -(x["content_len"] or 0),
            )
        )

        pick = correct[0]
        logger.info(
            f"  {pwsid}: best = {pick['url'][:70]} "
            f"({pick['content_len']} chars, parse={pick['last_parse_result']})"
        )
        best.append(pick)

    return best


def main():
    parser = argparse.ArgumentParser(
        description="WS1 CO Gap — Scrape + Batch Parse (Sprint 30)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show targets without scraping or submitting")
    parser.add_argument("--check-status", action="store_true",
                        help="Check batch status")
    parser.add_argument("--process-batch", action="store_true",
                        help="Process completed batch results")
    args = parser.parse_args()

    # Handle status check / process modes
    if args.check_status:
        results = BatchAgent().check_status()
        for r in results:
            print(f"  {r.get('batch_id')}: {r.get('local_status')} "
                  f"({r.get('succeeded', '?')}/{r.get('task_count', '?')} succeeded)")
        return

    if args.process_batch:
        agent = BatchAgent()
        result = agent.process_all_pending()
        print(f"Processed: {result}")
        return

    # ---- Phase 1: Scrape pending URLs ----
    print("=" * 60)
    print("Phase 1: Scraping pending locality_discovery URLs")
    print("=" * 60)

    pending = get_pending_urls()
    # Filter out wrong-entity URLs before scraping
    pending_good = [p for p in pending if not is_wrong_entity(p["url"])]
    pending_bad = [p for p in pending if is_wrong_entity(p["url"])]

    print(f"  Pending URLs: {len(pending)} total, {len(pending_good)} good, "
          f"{len(pending_bad)} wrong-entity (skipped)")
    for p in pending_bad:
        print(f"    SKIP: {p['pwsid']} {p['url'][:60]}")

    if args.dry_run:
        print("\n  Would scrape:")
        for p in pending_good:
            print(f"    {p['pwsid']} [id={p['id']}] {p['url'][:70]}")
    else:
        agent = ScrapeAgent()
        ok = 0
        fail = 0
        for i, p in enumerate(pending_good, 1):
            logger.info(
                f"  [{i}/{len(pending_good)}] {p['pwsid']} — {p['url'][:60]}"
            )
            try:
                result = agent.run(registry_id=p["id"], max_depth=0)
                if result.get("succeeded", 0) > 0:
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                logger.error(f"    FAIL: {e}")
                fail += 1

        print(f"\n  Scrape complete: {ok} ok, {fail} failed / {len(pending_good)} total")

    # ---- Phase 2: Collect best candidates ----
    print("\n" + "=" * 60)
    print("Phase 2: Selecting best candidate per PWSID")
    print("=" * 60)

    candidates = get_scraped_candidates()
    print(f"  Total scraped candidates: {len(candidates)}")

    best = select_best_per_pwsid(candidates)
    print(f"\n  Selected {len(best)} / {len(TARGET_PWSIDS)} PWSIDs for batch")

    if not best:
        print("  No candidates available for batch submission.")
        return

    # Show selections
    for b in best:
        print(f"    {b['pwsid']} ({b['pws_name']})")
        print(f"      URL: {b['url'][:80]}")
        print(f"      Content: {b['content_len']} chars | "
              f"Prior parse: {b['last_parse_result'] or 'never'}")

    if args.dry_run:
        print(f"\n  Dry run — would submit {len(best)} tasks to Anthropic batch.")
        return

    # ---- Phase 3: Submit batch ----
    print("\n" + "=" * 60)
    print("Phase 3: Submitting batch to Anthropic")
    print("=" * 60)

    parse_tasks = []
    for b in best:
        parse_tasks.append({
            "pwsid": b["pwsid"],
            "raw_text": b["scraped_text"],
            "content_type": b.get("content_type", "html"),
            "source_url": b["url"],
            "registry_id": b["id"],
            "utility_name": b.get("pws_name", ""),
            "state_code": "CO",
        })

    batch_agent = BatchAgent()
    result = batch_agent.submit(parse_tasks=parse_tasks, state_filter="CO")

    print(f"\n  Batch submitted: {result.get('batch_id')}")
    print(f"  Tasks: {result.get('task_count')}")
    print(f"  Status: {result.get('status')}")
    print(f"\n  Next: wait ~24h, then run:")
    print(f"    python scripts/scrape_batch_ws1_co.py --check-status")
    print(f"    python scripts/scrape_batch_ws1_co.py --process-batch")


if __name__ == "__main__":
    main()

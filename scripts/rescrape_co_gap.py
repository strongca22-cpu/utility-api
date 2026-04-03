#!/usr/bin/env python3
"""
Re-Scrape CO Gap PWSIDs (Sprint 29 — Playwright Wait + PDF 403 Fixes)

Purpose:
    Rescrapes CO gap PWSID URLs that failed due to:
    1. JS-rendered rate tables (CivicPlus etc.) — headers scraped but rate
       values in <td> cells were empty. Now fixed with 12s Playwright wait.
    2. PDF 403 Forbidden — .colorado.gov CMS blocks bot User-Agent.
       Sprint 27 fix retries with browser UA headers.
    3. Thin pages — Playwright got <500 chars, likely JS not rendered.

    Targets: CO PWSIDs with pop >= 3k and no rate_best_estimate.
    Tags rescrape entries with '[rescrape:sprint29_co_gap]'.

Author: AI-Generated
Created: 2026-04-03
Modified: 2026-04-03

Dependencies:
    - utility_api (local package)

Usage:
    # Dry run — show targets
    python scripts/rescrape_co_gap.py --dry-run

    # Run JS thin pages only (extended Playwright wait)
    python scripts/rescrape_co_gap.py --js-only

    # Run PDF 403s only (browser UA retry)
    python scripts/rescrape_co_gap.py --pdf-only

    # Run all
    python scripts/rescrape_co_gap.py

Notes:
    - JS rescrape: URLs with scraped text but rate table values missing
      (last_content_length > 0, parse_result = 'failed', has rate keywords
      but missing dollar amounts in text)
    - PDF rescrape: URLs with status='pending_retry' and PDF 403 errors
    - Uses ScrapeAgent with max_depth=0 (no deep crawl)
    - Resets status to 'pending' and clears scraped_text before rescrape
"""

import argparse
import re
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.scrape import ScrapeAgent
from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema

TAG = "[rescrape:sprint29_co_gap]"


def get_js_thin_targets() -> list[dict]:
    """Get URLs that rendered JS framework but missed rate table values.

    These are active URLs with scraped_text that contains rate keywords
    (tier, rate schedule, $/1,000) but few or no dollar amounts — indicating
    the table structure loaded but AJAX-populated cell values did not.

    Also includes truly thin pages (<500 chars) that Playwright timed out on.
    """
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH gap AS (
                SELECT s.pwsid
                FROM {schema}.sdwis_systems s
                WHERE s.state_code = 'CO'
                  AND s.population_served_count >= 3000
                  AND s.activity_status_cd = 'A'
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.rate_best_estimate rbe
                      WHERE rbe.pwsid = s.pwsid
                  )
            )
            SELECT sr.id, sr.pwsid, sr.url, sr.content_type,
                   sr.last_content_length, sr.scraped_text,
                   sr.notes,
                   s.population_served_count as pop
            FROM {schema}.scrape_registry sr
            JOIN gap g ON sr.pwsid = g.pwsid
            JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
            WHERE sr.status = 'active'
              AND sr.scraped_text IS NOT NULL
              AND sr.last_content_length > 0
              AND sr.content_type != 'pdf'
              AND sr.url NOT LIKE '%semswa.org%'
              AND sr.url NOT LIKE '%castlewoodwsd%'
            ORDER BY s.population_served_count DESC
        """)).fetchall()

    # Filter to URLs that have rate structure indicators but missing values
    dollar_re = re.compile(r"\$\d+\.\d{2}")
    rate_structure_re = re.compile(
        r"(water\s+volume|per\s+1,?000|tier\s+\d|rate\s+schedule|"
        r"monthly\s+base|service\s+charge|meter\s+size)",
        re.IGNORECASE,
    )

    targets = []
    for r in rows:
        txt = r.scraped_text or ""
        dollars = dollar_re.findall(txt)
        has_structure = bool(rate_structure_re.search(txt))

        # Case 1: Has rate structure indicators but few/no dollar amounts
        # (table headers rendered, cell values didn't)
        if has_structure and len(dollars) < 3:
            targets.append(dict(r._mapping, reason="js_table_empty"))
            continue

        # Case 2: Thin page — Playwright got very little content
        if r.last_content_length < 500:
            targets.append(dict(r._mapping, reason="thin_page"))
            continue

    return targets


def get_pdf_403_targets() -> list[dict]:
    """Get PDF URLs that failed with 403 Forbidden.

    Sprint 27 added browser User-Agent retry for PDF 403s.
    These .colorado.gov PDFs should now succeed.
    """
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH gap AS (
                SELECT s.pwsid
                FROM {schema}.sdwis_systems s
                WHERE s.state_code = 'CO'
                  AND s.population_served_count >= 3000
                  AND s.activity_status_cd = 'A'
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.rate_best_estimate rbe
                      WHERE rbe.pwsid = s.pwsid
                  )
            )
            SELECT sr.id, sr.pwsid, sr.url, sr.content_type,
                   sr.notes,
                   s.population_served_count as pop
            FROM {schema}.scrape_registry sr
            JOIN gap g ON sr.pwsid = g.pwsid
            JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
            WHERE sr.status = 'pending_retry'
              AND sr.notes LIKE '%403%'
            ORDER BY s.population_served_count DESC
        """)).fetchall()

    return [dict(r._mapping, reason="pdf_403") for r in rows]


def tag_and_reset(targets: list[dict]) -> None:
    """Tag targets and reset for rescrape."""
    with engine.begin() as conn:
        for t in targets:
            conn.execute(text(f"""
                UPDATE {schema}.scrape_registry
                SET status = 'pending',
                    scraped_text = NULL,
                    last_content_length = NULL,
                    last_parse_result = NULL,
                    last_parse_confidence = NULL,
                    last_parse_raw_response = NULL,
                    notes = COALESCE(notes, '') || ' {TAG}'
                WHERE id = :id
            """), {"id": t["id"]})


def main():
    parser = argparse.ArgumentParser(
        description="Re-scrape CO gap PWSIDs with Sprint 29 fixes"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show targets without scraping")
    parser.add_argument("--js-only", action="store_true",
                        help="Only rescrape JS thin/empty-table pages")
    parser.add_argument("--pdf-only", action="store_true",
                        help="Only rescrape PDF 403 failures")
    args = parser.parse_args()

    js_targets = [] if args.pdf_only else get_js_thin_targets()
    pdf_targets = [] if args.js_only else get_pdf_403_targets()

    # Deduplicate by registry ID
    seen_ids = set()
    all_targets = []
    for t in js_targets + pdf_targets:
        if t["id"] not in seen_ids:
            seen_ids.add(t["id"])
            all_targets.append(t)

    print(f"CO Gap Rescrape Targets:")
    print(f"  JS thin/empty-table: {len(js_targets)}")
    print(f"  PDF 403 failures:    {len(pdf_targets)}")
    print(f"  Total (deduplicated): {len(all_targets)}")

    if not all_targets:
        print("No targets found.")
        return

    # Group by reason for display
    by_reason = {}
    for t in all_targets:
        reason = t.get("reason", "unknown")
        by_reason.setdefault(reason, []).append(t)

    for reason, items in by_reason.items():
        print(f"\n--- {reason} ({len(items)} URLs) ---")
        for t in items[:10]:
            url_short = t["url"][:70] if t["url"] else "?"
            print(f"  {t['pwsid']} pop={t['pop']:>7,} | {url_short}")
        if len(items) > 10:
            print(f"  ... and {len(items) - 10} more")

    if args.dry_run:
        print(f"\nDry run — no changes made. Run without --dry-run to execute.")
        return

    # Tag and reset targets
    print(f"\nTagging {len(all_targets)} entries with {TAG} and resetting...")
    tag_and_reset(all_targets)

    # Scrape
    agent = ScrapeAgent()
    ok = 0
    fail = 0
    for i, t in enumerate(all_targets, 1):
        logger.info(
            f"[{i}/{len(all_targets)}] {t['pwsid']} ({t.get('reason','')}) — "
            f"{t['url'][:60]}"
        )
        try:
            result = agent.run(registry_id=t["id"], max_depth=0)
            if result.get("succeeded", 0) > 0:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            logger.error(f"  FAIL: {e}")
            fail += 1

    print(f"\nCO gap rescrape complete: {ok} ok, {fail} failed / {len(all_targets)} total")


if __name__ == "__main__":
    main()

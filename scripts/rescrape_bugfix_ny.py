#!/usr/bin/env python3
"""
Re-Scrape Bug-Affected NY URLs (Sprint 27 Fixes)

Purpose:
    Scrapes URLs that were previously affected by 4 bugs fixed in Sprint 27:
    1. Playwright networkidle → load (chat widget timeout)
    2. <form> tag stripping (ASP.NET CMS content destruction)
    3. PDF 403 browser User-Agent retry (bot-blocked PDFs)
    4. Short-line filter preserved $ amounts (dollar values stripped)

    These URLs were tagged with '[rescrape:sprint27_bugfix]' in notes
    and reset to pending with NULL scraped_text.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - utility_api (local package)

Usage:
    # Dry run — show what would be scraped
    python scripts/rescrape_bugfix_ny.py --dry-run

    # Run (after TC-R2 completes)
    python scripts/rescrape_bugfix_ny.py

    # Run only PDFs
    python scripts/rescrape_bugfix_ny.py --pdf-only

    # Run only HTML
    python scripts/rescrape_bugfix_ny.py --html-only

Notes:
    - Run AFTER TC-R2 scrape completes to avoid resource contention
    - Uses ScrapeAgent with max_depth=0 (no deep crawl, just the URL)
    - 183 URLs: 114 HTML, 69 PDF
    - Tagged URLs can be found with: WHERE notes LIKE '%rescrape:sprint27_bugfix%'
"""

import argparse
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


def get_pending(pdf_only: bool = False, html_only: bool = False) -> list[dict]:
    """Get bug-fix rescrape URLs."""
    type_filter = ""
    if pdf_only:
        type_filter = "AND (sr.content_type = 'pdf' OR sr.url LIKE '%.pdf')"
    elif html_only:
        type_filter = "AND sr.content_type != 'pdf' AND sr.url NOT LIKE '%.pdf'"

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id, sr.pwsid, sr.url, sr.content_type,
                   c.population_served
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.notes LIKE '%rescrape:sprint27_bugfix%'
              AND sr.status = 'pending'
              AND sr.scraped_text IS NULL
              {type_filter}
            ORDER BY c.population_served DESC
        """)).fetchall()

    return [dict(r._mapping) for r in rows]


def main():
    parser = argparse.ArgumentParser(
        description="Re-scrape Sprint 27 bug-affected NY URLs"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pdf-only", action="store_true")
    parser.add_argument("--html-only", action="store_true")
    args = parser.parse_args()

    pending = get_pending(pdf_only=args.pdf_only, html_only=args.html_only)
    label = "PDF" if args.pdf_only else "HTML" if args.html_only else "all"
    print(f"Bug-fix rescrape ({label}): {len(pending)} URLs")

    if not pending:
        return

    if args.dry_run:
        for r in pending[:20]:
            print(f"  {r['pwsid']}  {r['content_type'] or 'html'}  {r['url'][:70]}")
        if len(pending) > 20:
            print(f"  ... and {len(pending) - 20} more")
        return

    agent = ScrapeAgent()
    ok = 0
    fail = 0
    for i, row in enumerate(pending, 1):
        logger.info(f"[{i}/{len(pending)}] {row['pwsid']} — {row['url'][:60]}")
        try:
            result = agent.run(registry_id=row['id'], max_depth=0)
            if result.get("succeeded", 0) > 0:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            logger.error(f"  FAIL: {e}")
            fail += 1

    print(f"\nBug-fix rescrape complete: {ok} succeeded, {fail} failed out of {len(pending)}")


if __name__ == "__main__":
    main()

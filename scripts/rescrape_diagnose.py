#!/usr/bin/env python3
"""
Re-scrape Diagnostic: Identify Rows Affected by Playwright/Form Bugs

Purpose:
    Finds scrape_registry rows likely affected by two fixed bugs:
    1. Playwright networkidle timeout (chat widgets → empty results)
    2. <form> tag stripping (ASP.NET CMS → content destroyed)

    Produces a prioritized candidate list for re-scraping.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    python scripts/rescrape_diagnose.py --dry-run
    python scripts/rescrape_diagnose.py --state NY --min-pop 3000
    python scripts/rescrape_diagnose.py --output candidates.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


def find_candidates(state: str | None = None, min_pop: int = 0) -> list[dict]:
    """Find scrape_registry rows affected by Playwright/form bugs.

    Returns list of candidate dicts with priority ranking.
    """
    schema = settings.utility_schema

    state_filter = ""
    pop_filter = ""
    params = {}

    if state:
        state_filter = "AND sr.pwsid LIKE :state_prefix"
        params["state_prefix"] = f"{state}%"
    if min_pop > 0:
        pop_filter = "AND c.population_served >= :min_pop"
        params["min_pop"] = min_pop

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH candidates AS (
                -- Set A: Playwright timeout victims
                SELECT DISTINCT sr.id, sr.pwsid, sr.url, sr.last_content_length,
                       sr.url_source, sr.content_type, 'playwright_timeout' AS bug_type
                FROM {schema}.scrape_registry sr
                WHERE sr.status != 'dead'
                  AND COALESCE(sr.content_type, 'html') != 'pdf'
                  AND (
                    sr.notes LIKE '%%playwright_reason=thin_still_thin%%'
                    OR sr.notes LIKE '%%playwright_reason=error%%'
                  )
                  AND sr.url_source IN ('serper','curated','curated_portland',
                                         'metro_research','searxng','state_directory')
                  {state_filter}

                UNION

                -- Set B: Form-stripping victims (ASP.NET)
                SELECT DISTINCT sr.id, sr.pwsid, sr.url, sr.last_content_length,
                       sr.url_source, sr.content_type, 'form_stripping' AS bug_type
                FROM {schema}.scrape_registry sr
                WHERE sr.status != 'dead'
                  AND COALESCE(sr.content_type, 'html') != 'pdf'
                  AND (
                    (sr.url LIKE '%%.aspx%%' AND COALESCE(sr.last_content_length, 0) < 100)
                    OR (sr.last_content_length BETWEEN 20 AND 100
                        AND COALESCE(sr.last_http_status, 200) = 200
                        AND COALESCE(sr.content_type, 'html') = 'html')
                  )
                  {state_filter}

                UNION

                -- Set C: General thin HTML on high-confidence sources
                SELECT DISTINCT sr.id, sr.pwsid, sr.url, sr.last_content_length,
                       sr.url_source, sr.content_type, 'thin_html' AS bug_type
                FROM {schema}.scrape_registry sr
                WHERE sr.status != 'dead'
                  AND COALESCE(sr.content_type, 'html') != 'pdf'
                  AND COALESCE(sr.last_http_status, 200) = 200
                  AND COALESCE(sr.last_content_length, 0) < 200
                  AND sr.url_source IN ('serper','curated','curated_portland',
                                         'metro_research','searxng','state_directory')
                  {state_filter}
            )
            SELECT c.id, c.pwsid, c.url, c.last_content_length,
                   c.url_source, c.content_type, c.bug_type,
                   cb.pws_name, cb.state_code, cb.population_served,
                   CASE
                       WHEN rbe.pwsid IS NULL THEN 1
                       WHEN NOT EXISTS (
                           SELECT 1 FROM {schema}.rate_schedules rs
                           WHERE rs.pwsid = c.pwsid AND rs.source_key = 'scraped_llm'
                       ) THEN 2
                       ELSE 3
                   END AS priority
            FROM candidates c
            JOIN {schema}.cws_boundaries cb ON cb.pwsid = c.pwsid
            LEFT JOIN {schema}.rate_best_estimate rbe ON rbe.pwsid = c.pwsid
            WHERE 1=1
              {pop_filter}
            ORDER BY priority ASC, cb.population_served DESC
        """), params).fetchall()

    return [dict(r._mapping) for r in rows]


def print_summary(candidates: list[dict]) -> None:
    """Print diagnostic summary."""
    if not candidates:
        logger.info("No candidates found.")
        return

    from collections import Counter

    total = len(candidates)
    by_priority = Counter(c["priority"] for c in candidates)
    by_bug = Counter(c["bug_type"] for c in candidates)
    by_state = Counter(c["state_code"] for c in candidates)
    unique_pwsids = len(set(c["pwsid"] for c in candidates))

    logger.info(f"\n{'='*60}")
    logger.info(f"RE-SCRAPE DIAGNOSTIC SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total candidate URLs: {total:,}")
    logger.info(f"Unique PWSIDs: {unique_pwsids:,}")

    logger.info(f"\nBy priority:")
    logger.info(f"  P1 (gap — no rate):        {by_priority.get(1, 0):,}")
    logger.info(f"  P2 (bulk-only — upgrade):  {by_priority.get(2, 0):,}")
    logger.info(f"  P3 (has scraped rate):      {by_priority.get(3, 0):,}")

    logger.info(f"\nBy bug type:")
    for bug, cnt in by_bug.most_common():
        logger.info(f"  {bug:25s}: {cnt:,}")

    logger.info(f"\nTop 10 states:")
    for st, cnt in by_state.most_common(10):
        logger.info(f"  {st}: {cnt:,}")

    # Top 10 P1 candidates by population
    p1 = [c for c in candidates if c["priority"] == 1]
    if p1:
        logger.info(f"\nTop 10 P1 (gap) by population:")
        for c in p1[:10]:
            logger.info(
                f"  {c['pwsid']} {c['state_code']} {c['population_served']:>10,} "
                f"{(c['pws_name'] or '')[:30]:30s} {c['last_content_length'] or 0:>6}ch "
                f"[{c['bug_type']}]"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose scrape_registry rows affected by Playwright/form bugs")
    parser.add_argument("--state", default=None, help="Filter to state code (e.g., NY)")
    parser.add_argument("--min-pop", type=int, default=0, help="Min population")
    parser.add_argument("--output", default=None, help="Write candidate IDs to JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Summary only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Re-scrape Diagnostic")
    logger.info("=" * 60)

    candidates = find_candidates(state=args.state, min_pop=args.min_pop)
    print_summary(candidates)

    if args.output and not args.dry_run:
        output_data = [{"id": c["id"], "pwsid": c["pwsid"], "priority": c["priority"]}
                       for c in candidates]
        with open(args.output, "w") as f:
            json.dump(output_data, f)
        logger.info(f"\nWrote {len(output_data)} candidates to {args.output}")

    if args.dry_run:
        logger.info(f"\n[DRY RUN] No output written.")


if __name__ == "__main__":
    main()

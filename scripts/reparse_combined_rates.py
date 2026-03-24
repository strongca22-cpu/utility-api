#!/usr/bin/env python3
"""
Re-parse Combined Water+Sewer Rates as Water-Only

Purpose:
    Re-scrapes and re-parses 7 utilities whose scraped rates likely include
    combined water+sewer charges. Uses an enhanced prompt that explicitly
    instructs "WATER charges only, NOT combined water+sewer."

    Replaces existing scraped_llm records for these PWSIDs if the re-parse
    succeeds with high confidence.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - anthropic
    - httpx / playwright
    - sqlalchemy, loguru

Usage:
    python scripts/reparse_combined_rates.py              # Run re-parse
    python scripts/reparse_combined_rates.py --dry-run     # Preview only

Notes:
    - Requires ANTHROPIC_API_KEY in .env
    - Estimated cost: ~$0.05 (7 Sonnet calls)
    - After success, re-run build_best_estimate.py to update selections
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_scraper import scrape_rate_page
from utility_api.ingest.rate_calculator import calculate_bills_from_parse


# Enhanced water-only prompt prefix
WATER_ONLY_ADDENDUM = """
CRITICAL INSTRUCTION: Extract WATER charges ONLY. Many utilities publish combined
water+sewer rate schedules on the same page. You MUST separate them and return
ONLY the water portion. Specifically:
- If the page lists "Water" and "Sewer/Wastewater" as separate line items, use ONLY the Water amounts
- If the page shows a single "Utility" or "Combined" charge, set parse_confidence to "low" and note that rates may be combined
- Fixed/service charges: use the WATER service charge only, not a combined utility charge
- Volumetric rates: use WATER volumetric rates only
- If you cannot distinguish water from sewer, set parse_confidence to "low"
"""


def run_reparse(dry_run: bool = False) -> dict:
    """Re-parse suspected combined water+sewer rates.

    Parameters
    ----------
    dry_run : bool
        If True, scrape and parse but don't update DB.

    Returns
    -------
    dict
        Stats: attempted, scraped, parsed, updated.
    """
    logger.info("=== Re-parse Combined Water+Sewer Rates ===")

    schema = settings.utility_schema

    with engine.connect() as conn:
        targets = conn.execute(text(f"""
            SELECT pwsid, utility_name, source_url, state_code, county
            FROM {schema}.water_rates
            WHERE source = 'scraped_llm' AND parse_confidence = 'low'
              AND parse_notes LIKE '%combined water+sewer%'
            ORDER BY pwsid
        """)).fetchall()

    logger.info(f"Found {len(targets)} utilities to re-parse")

    stats = {"attempted": 0, "scraped": 0, "parsed": 0, "updated": 0, "failed": 0}

    for target in targets:
        pwsid = target[0]
        name = target[1] or ""
        url = target[2]
        state = target[3] or "CA"
        county = target[4]

        stats["attempted"] += 1
        logger.info(f"\n--- {pwsid} {name[:40]} ---")

        if not url:
            logger.warning(f"  No source URL, skipping")
            stats["failed"] += 1
            continue

        # Step 1: Re-scrape
        logger.info(f"  Scraping: {url[:80]}...")
        try:
            scrape_result = scrape_rate_page(url)
            if not scrape_result or not scrape_result.text or len(scrape_result.text.strip()) < 100:
                logger.warning(f"  Scrape returned insufficient text ({len(scrape_result.text) if scrape_result else 0} chars)")
                stats["failed"] += 1
                continue
            stats["scraped"] += 1
            logger.info(f"  Scraped {len(scrape_result.text)} chars")
        except Exception as e:
            logger.warning(f"  Scrape failed: {e}")
            stats["failed"] += 1
            continue

        # Step 2: Parse with water-only prompt
        logger.info(f"  Parsing with water-only prompt...")
        try:
            from utility_api.ingest.rate_parser import parse_rate_text, SYSTEM_PROMPT

            # Inject water-only addendum into the system prompt
            enhanced_prompt = SYSTEM_PROMPT + WATER_ONLY_ADDENDUM

            # We need to call the parser with the enhanced prompt
            # Since parse_rate_text uses the module-level SYSTEM_PROMPT,
            # we temporarily patch it
            import utility_api.ingest.rate_parser as parser_mod
            original_prompt = parser_mod.SYSTEM_PROMPT
            parser_mod.SYSTEM_PROMPT = enhanced_prompt

            result = parse_rate_text(
                scrape_result.text,
                utility_name=name,
                state_code=state,
            )

            # Restore original prompt
            parser_mod.SYSTEM_PROMPT = original_prompt

            if result.parse_confidence == "failed":
                logger.warning(f"  Parse failed: {result.parse_notes}")
                stats["failed"] += 1
                continue

            stats["parsed"] += 1
            logger.info(
                f"  Parsed: {result.rate_structure_type} "
                f"fixed=${result.fixed_charge_monthly or 0:.2f} "
                f"t1=${result.tier_1_rate or 0:.4f}/CCF "
                f"[{result.parse_confidence}]"
            )

            # Step 3: Calculate bills
            bill_5, bill_10 = calculate_bills_from_parse(result)
            logger.info(f"  Bills: @5CCF=${bill_5 or 0:.2f} @10CCF=${bill_10 or 0:.2f}")

            if dry_run:
                logger.info(f"  [DRY RUN] Would update DB")
                continue

            # Step 4: Update the DB record
            now = datetime.now(timezone.utc)
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.water_rates SET
                        rate_effective_date = :date,
                        rate_structure_type = :structure,
                        billing_frequency = :freq,
                        fixed_charge_monthly = :fixed,
                        tier_1_limit_ccf = :t1l, tier_1_rate = :t1r,
                        tier_2_limit_ccf = :t2l, tier_2_rate = :t2r,
                        tier_3_limit_ccf = :t3l, tier_3_rate = :t3r,
                        tier_4_limit_ccf = :t4l, tier_4_rate = :t4r,
                        bill_5ccf = :b5, bill_10ccf = :b10,
                        parse_confidence = :conf,
                        parse_notes = :notes,
                        parsed_at = :parsed_at
                    WHERE pwsid = :pwsid AND source = 'scraped_llm'
                """), {
                    "pwsid": pwsid,
                    "date": result.rate_effective_date,
                    "structure": result.rate_structure_type,
                    "freq": result.billing_frequency,
                    "fixed": result.fixed_charge_monthly,
                    "t1l": result.tier_1_limit_ccf, "t1r": result.tier_1_rate,
                    "t2l": result.tier_2_limit_ccf, "t2r": result.tier_2_rate,
                    "t3l": result.tier_3_limit_ccf, "t3r": result.tier_3_rate,
                    "t4l": result.tier_4_limit_ccf, "t4r": result.tier_4_rate,
                    "b5": bill_5, "b10": bill_10,
                    "conf": result.parse_confidence,
                    "notes": f"[REPARSE 2026-03-24] Water-only re-parse. {result.parse_notes or ''}",
                    "parsed_at": now,
                })
                conn.commit()

            stats["updated"] += 1
            logger.info(f"  Updated DB record")

        except Exception as e:
            logger.error(f"  Parse error: {e}")
            stats["failed"] += 1
            continue

    logger.info(f"\n=== Re-parse Complete ===")
    logger.info(f"  Attempted: {stats['attempted']}")
    logger.info(f"  Scraped: {stats['scraped']}")
    logger.info(f"  Parsed: {stats['parsed']}")
    logger.info(f"  Updated: {stats['updated']}")
    logger.info(f"  Failed: {stats['failed']}")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Re-parse combined water+sewer rates")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB updates")
    args = parser.parse_args()
    run_reparse(dry_run=args.dry_run)


if __name__ == "__main__":
    main()

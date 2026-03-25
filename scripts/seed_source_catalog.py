#!/usr/bin/env python3
"""
Seed Source Catalog

Purpose:
    Populate the source_catalog table with all known data sources
    from Sprint 1–9. This is a one-time seed script run after
    migration 009. Safe to re-run (uses UPSERT).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    python scripts/seed_source_catalog.py
    python scripts/seed_source_catalog.py --update-counts  # Also refresh pwsid_count from DB

Notes:
    - Idempotent: uses INSERT ON CONFLICT UPDATE
    - pwsid_count is populated from actual DB counts when --update-counts is used
    - last_ingested_at is set from pipeline_runs table when available
"""

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

# All known data sources as of Sprint 9
SOURCES = [
    {
        "source_key": "swrcb_ear_2022",
        "display_name": "CA SWRCB Electronic Annual Report 2022",
        "source_type": "bulk_government",
        "states_covered": ["CA"],
        "vintage_start": date(2022, 1, 1),
        "vintage_end": date(2022, 12, 31),
        "refresh_cadence": "annual",
        "next_check_date": date(2026, 7, 1),
        "ingest_module": "utility_api.ingest.ear_ingest",
        "ingest_command": "ua-ingest ear --year 2022",
        "notes": "HydroShare processed eAR data. 194 CA utilities. "
                 "Anchor source for CA best-estimate. 97 tier records NULLed after inflation fix.",
    },
    {
        "source_key": "swrcb_ear_2021",
        "display_name": "CA SWRCB Electronic Annual Report 2021",
        "source_type": "bulk_government",
        "states_covered": ["CA"],
        "vintage_start": date(2021, 1, 1),
        "vintage_end": date(2021, 12, 31),
        "refresh_cadence": "one-time",
        "ingest_module": "utility_api.ingest.ear_ingest",
        "ingest_command": "ua-ingest ear --year 2021",
        "notes": "HydroShare processed eAR data. 193 CA utilities. Includes bill snapshots.",
    },
    {
        "source_key": "swrcb_ear_2020",
        "display_name": "CA SWRCB Electronic Annual Report 2020",
        "source_type": "bulk_government",
        "states_covered": ["CA"],
        "vintage_start": date(2020, 1, 1),
        "vintage_end": date(2020, 12, 31),
        "refresh_cadence": "one-time",
        "ingest_module": "utility_api.ingest.ear_ingest",
        "ingest_command": "ua-ingest ear --year 2020",
        "notes": "HydroShare processed eAR data. 194 CA utilities. "
                 "Tier data only, no pre-computed bill columns.",
    },
    {
        "source_key": "owrs",
        "display_name": "Open Water Rate Specification (CA Data Collaborative)",
        "source_type": "curated",
        "states_covered": ["CA"],
        "vintage_start": date(2002, 1, 1),
        "vintage_end": date(2021, 12, 31),
        "refresh_cadence": "one-time",
        "ingest_module": "utility_api.ingest.owrs_ingest",
        "ingest_command": "ua-ingest owrs",
        "notes": "Pre-curated YAML rate structures from utility OWRS filings. "
                 "387 records, 229 net-new PWSIDs not covered by eAR. Median vintage ~2017.",
    },
    {
        "source_key": "efc_nc_2025",
        "display_name": "UNC EFC NC Water Rate Dashboard 2025",
        "source_type": "bulk_government",
        "states_covered": ["NC"],
        "vintage_start": date(2024, 7, 1),
        "vintage_end": date(2024, 7, 1),
        "refresh_cadence": "annual",
        "next_check_date": date(2026, 7, 1),
        "ingest_module": "utility_api.ingest.efc_nc_ingest",
        "ingest_command": "ua-ingest efc-nc",
        "notes": "UNC EFC dashboard CSV. 403 NC utilities. "
                 "Tier structure back-calculated from bill curves. 400/403 high confidence.",
    },
    {
        "source_key": "scraped_llm",
        "display_name": "LLM-Scraped Water Rates (Claude API)",
        "source_type": "scraped",
        "states_covered": ["VA", "CA"],
        "vintage_start": date(2014, 1, 1),
        "vintage_end": date(2026, 3, 1),
        "refresh_cadence": "continuous",
        "ingest_module": "utility_api.ingest.rates",
        "ingest_command": "ua-ingest rates --url-file config/rate_urls_va.yaml",
        "notes": "End-to-end pipeline: SearXNG discovery → web scrape → Claude Sonnet extraction. "
                 "101 records, 97 unique PWSIDs. VA: 22/31 parsed. CA: 5 parsed. "
                 "Total API cost: ~$0.36.",
    },
]


def seed_source_catalog(update_counts: bool = False) -> int:
    """Seed the source_catalog table with known data sources.

    Parameters
    ----------
    update_counts : bool
        If True, also query water_rates to update pwsid_count.

    Returns
    -------
    int
        Number of sources upserted.
    """
    schema = settings.utility_schema
    upserted = 0

    with engine.connect() as conn:
        for src in SOURCES:
            # Build arrays for PostgreSQL
            states_array = "{" + ",".join(src["states_covered"]) + "}"

            conn.execute(text(f"""
                INSERT INTO {schema}.source_catalog (
                    source_key, display_name, source_type, states_covered,
                    vintage_start, vintage_end, refresh_cadence,
                    next_check_date, ingest_module, ingest_command, notes
                ) VALUES (
                    :source_key, :display_name, :source_type, :states_covered,
                    :vintage_start, :vintage_end, :refresh_cadence,
                    :next_check_date, :ingest_module, :ingest_command, :notes
                )
                ON CONFLICT (source_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    source_type = EXCLUDED.source_type,
                    states_covered = EXCLUDED.states_covered,
                    vintage_start = EXCLUDED.vintage_start,
                    vintage_end = EXCLUDED.vintage_end,
                    refresh_cadence = EXCLUDED.refresh_cadence,
                    next_check_date = EXCLUDED.next_check_date,
                    ingest_module = EXCLUDED.ingest_module,
                    ingest_command = EXCLUDED.ingest_command,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
            """), {
                "source_key": src["source_key"],
                "display_name": src["display_name"],
                "source_type": src["source_type"],
                "states_covered": states_array,
                "vintage_start": src.get("vintage_start"),
                "vintage_end": src.get("vintage_end"),
                "refresh_cadence": src.get("refresh_cadence"),
                "next_check_date": src.get("next_check_date"),
                "ingest_module": src.get("ingest_module"),
                "ingest_command": src.get("ingest_command"),
                "notes": src.get("notes"),
            })
            upserted += 1
            logger.info(f"  Upserted: {src['source_key']}")

        # Update pwsid_count and last_ingested_at from actual DB data
        if update_counts:
            logger.info("Updating pwsid_count from water_rates...")
            conn.execute(text(f"""
                UPDATE {schema}.source_catalog sc
                SET pwsid_count = sub.cnt,
                    updated_at = NOW()
                FROM (
                    SELECT source, COUNT(DISTINCT pwsid) AS cnt
                    FROM {schema}.water_rates
                    GROUP BY source
                ) sub
                WHERE sc.source_key = sub.source
            """))

            logger.info("Updating last_ingested_at from pipeline_runs...")
            conn.execute(text(f"""
                UPDATE {schema}.source_catalog sc
                SET last_ingested_at = sub.last_run,
                    updated_at = NOW()
                FROM (
                    SELECT step_name, MAX(finished_at) AS last_run
                    FROM {schema}.pipeline_runs
                    WHERE status = 'success'
                    GROUP BY step_name
                ) sub
                WHERE (
                    (sc.source_key LIKE 'swrcb_ear_%' AND sub.step_name = 'ear_ingest')
                    OR (sc.source_key = 'owrs' AND sub.step_name = 'owrs_ingest')
                    OR (sc.source_key = 'efc_nc_2025' AND sub.step_name = 'efc_nc_ingest')
                    OR (sc.source_key = 'scraped_llm' AND sub.step_name = 'rate_ingest')
                )
            """))

        conn.commit()

    logger.info(f"Seeded {upserted} sources into source_catalog")
    return upserted


def main():
    parser = argparse.ArgumentParser(description="Seed source_catalog table")
    parser.add_argument("--update-counts", action="store_true",
                        help="Also refresh pwsid_count from water_rates")
    args = parser.parse_args()

    logger.info("=== Seeding Source Catalog ===")
    count = seed_source_catalog(update_counts=args.update_counts)
    logger.info(f"=== Done: {count} sources ===")


if __name__ == "__main__":
    main()

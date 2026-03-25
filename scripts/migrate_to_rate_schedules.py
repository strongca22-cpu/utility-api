#!/usr/bin/env python3
"""
Migrate water_rates → rate_schedules

Purpose:
    One-time migration of all 1,472 water_rates records into the
    canonical rate_schedules table with JSONB tier storage. Converts
    fixed tier columns to JSONB arrays, computes derived metrics
    (conservation_signal, bill_20ccf, tier_count).

    Safe to re-run (uses UPSERT on unique constraint).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru

Usage:
    python scripts/migrate_to_rate_schedules.py
    python scripts/migrate_to_rate_schedules.py --dry-run

Notes:
    - Converts CCF/$/CCF to gallons/$/1000gal for JSONB tiers
    - Computes bill_20ccf from tier structure + fixed charges
    - Computes conservation_signal (highest/lowest tier ratio)
    - water_rates records are NOT modified — this is additive only
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import (
    water_rate_to_schedule,
    write_rate_schedule,
)


def run_migration(dry_run: bool = False) -> dict:
    """Migrate all water_rates records to rate_schedules.

    Parameters
    ----------
    dry_run : bool
        Preview only, no DB writes.

    Returns
    -------
    dict
        Summary statistics.
    """
    schema = settings.utility_schema

    logger.info("=== Migrate water_rates → rate_schedules ===")

    # Load all water_rates records
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT
                pwsid, source, utility_name, state_code,
                rate_effective_date, rate_structure_type, rate_class,
                billing_frequency, fixed_charge_monthly, meter_size_inches,
                tier_1_limit_ccf, tier_1_rate,
                tier_2_limit_ccf, tier_2_rate,
                tier_3_limit_ccf, tier_3_rate,
                tier_4_limit_ccf, tier_4_rate,
                bill_5ccf, bill_10ccf,
                bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf,
                source_url, raw_text_hash, parse_confidence,
                parse_model, parse_notes, scraped_at, parsed_at
            FROM {schema}.water_rates
            ORDER BY pwsid, source
        """), conn)

    logger.info(f"Loaded {len(df)} water_rates records")

    # Convert each record
    schedules = []
    for _, row in df.iterrows():
        schedule = water_rate_to_schedule(row.to_dict())
        schedules.append(schedule)

    # Stats
    has_tiers = sum(1 for s in schedules if s["volumetric_tiers"])
    has_fixed = sum(1 for s in schedules if s["fixed_charges"])
    has_conservation = sum(1 for s in schedules if s["conservation_signal"])
    has_bill_20 = sum(1 for s in schedules if s["bill_20ccf"])

    logger.info(f"\nConversion stats:")
    logger.info(f"  With volumetric tiers: {has_tiers}/{len(schedules)}")
    logger.info(f"  With fixed charges: {has_fixed}/{len(schedules)}")
    logger.info(f"  With conservation signal: {has_conservation}/{len(schedules)}")
    logger.info(f"  With bill @20CCF: {has_bill_20}/{len(schedules)}")

    if dry_run:
        logger.info("\n[DRY RUN] Sample conversions:")
        for s in schedules[:5]:
            logger.info(f"  {s['pwsid']} [{s['source_key']}] "
                        f"tiers={s['tier_count']} "
                        f"conserv={s['conservation_signal']} "
                        f"bill20={s['bill_20ccf']}")
        return {
            "total": len(schedules),
            "has_tiers": has_tiers,
            "has_conservation": has_conservation,
        }

    # Write to database
    inserted = 0
    skipped = 0
    with engine.connect() as conn:
        for s in schedules:
            try:
                if write_rate_schedule(conn, s):
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"  Error on {s['pwsid']}: {e}")
                skipped += 1
                conn.rollback()
        conn.commit()

    logger.info(f"\nInserted/updated {inserted} rate_schedules ({skipped} skipped)")

    # Log pipeline run
    from datetime import datetime, timezone
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES
                ('migrate_to_rate_schedules', NOW(), NOW(), :count, 'success', :notes)
        """), {
            "count": inserted,
            "notes": f"Migrated {inserted} water_rates records to rate_schedules (JSONB tiers)",
        })
        conn.commit()

    logger.info("=== Migration Complete ===")
    return {
        "total": len(schedules),
        "inserted": inserted,
        "skipped": skipped,
        "has_tiers": has_tiers,
        "has_conservation": has_conservation,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Migrate water_rates → rate_schedules"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without DB writes")
    args = parser.parse_args()
    run_migration(dry_run=args.dry_run)


if __name__ == "__main__":
    main()

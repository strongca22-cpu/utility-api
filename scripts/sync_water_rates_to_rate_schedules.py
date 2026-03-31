#!/usr/bin/env python3
"""
Sync water_rates → rate_schedules (Phase 2 of water_rates deprecation)

Purpose:
    One-time migration script that ensures ALL water_rates records exist in
    rate_schedules. After this runs, rate_schedules is the single source of
    truth for all rate data, and water_rates can be treated as read-only legacy.

    Steps:
    1. Load all water_rates records
    2. For each, convert to rate_schedules format via water_rate_to_schedule()
    3. Upsert into rate_schedules (ON CONFLICT UPDATE)
    4. Backfill eAR bill columns (bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf)
       from water_rates for records that have them
    5. Report: records synced, skipped (FK violations), by source

    Safe to re-run — uses ON CONFLICT upsert, so duplicate records are updated
    rather than duplicated.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - sqlalchemy
    - loguru
    - utility_api (local package)

Usage:
    python scripts/sync_water_rates_to_rate_schedules.py              # full sync
    python scripts/sync_water_rates_to_rate_schedules.py --dry-run    # preview
    python scripts/sync_water_rates_to_rate_schedules.py --source efc_ct_2018  # single source

Notes:
    - Does NOT modify water_rates — read-only access
    - Uses write_rate_schedule() which handles ON CONFLICT upsert
    - FK violations (PWSID not in cws_boundaries) are logged and skipped
    - Re-runnable / idempotent
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import (
    water_rate_to_schedule,
    write_rate_schedule,
)


def load_water_rates(source_filter: str | None = None) -> list[dict]:
    """Load all water_rates records as dicts.

    Parameters
    ----------
    source_filter : str, optional
        Filter to a single source key (e.g., 'efc_ct_2018').

    Returns
    -------
    list[dict]
        All water_rates records.
    """
    schema = settings.utility_schema
    where = ""
    params = {}
    if source_filter:
        where = "WHERE source = :source"
        params = {"source": source_filter}

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT *
            FROM {schema}.water_rates
            {where}
            ORDER BY source, pwsid
        """), params).fetchall()

    records = [dict(r._mapping) for r in rows]
    logger.info(f"Loaded {len(records)} water_rates records")
    return records


def sync_to_rate_schedules(
    records: list[dict],
    dry_run: bool = False,
) -> dict:
    """Convert and upsert water_rates records into rate_schedules.

    Parameters
    ----------
    records : list[dict]
        water_rates records to sync.
    dry_run : bool
        Preview only, no DB writes.

    Returns
    -------
    dict
        Summary with synced, skipped_fk, errors, by_source counts.
    """
    synced = 0
    skipped_fk = 0
    errors = 0
    by_source = {}

    if dry_run:
        # Just count what would be synced
        for r in records:
            src = r.get("source", "unknown")
            by_source[src] = by_source.get(src, 0) + 1
        return {
            "synced": 0,
            "skipped_fk": 0,
            "errors": 0,
            "total": len(records),
            "by_source": by_source,
            "dry_run": True,
        }

    with engine.connect() as conn:
        for i, row in enumerate(records):
            src = row.get("source", "unknown")

            try:
                schedule = water_rate_to_schedule(row)
                success = write_rate_schedule(conn, schedule)

                if success:
                    synced += 1
                    by_source[src] = by_source.get(src, 0) + 1
                else:
                    skipped_fk += 1

            except Exception as e:
                if "violates foreign key" in str(e):
                    skipped_fk += 1
                else:
                    errors += 1
                    if errors <= 5:
                        logger.error(
                            f"  Error syncing {row.get('pwsid')}: {e}"
                        )

            # Progress logging
            if (i + 1) % 500 == 0:
                logger.info(f"  Progress: {i + 1}/{len(records)} "
                            f"(synced={synced}, skipped_fk={skipped_fk})")

        conn.commit()

    return {
        "synced": synced,
        "skipped_fk": skipped_fk,
        "errors": errors,
        "total": len(records),
        "by_source": by_source,
    }


def backfill_ear_bill_columns(dry_run: bool = False) -> dict:
    """Backfill bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf from water_rates.

    These columns exist in water_rates for eAR and other bulk sources but
    weren't in rate_schedules until migration 022. This copies them over
    for any record that has matching data in water_rates.

    Parameters
    ----------
    dry_run : bool
        Preview only.

    Returns
    -------
    dict
        Summary with updated count.
    """
    schema = settings.utility_schema

    if dry_run:
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM {schema}.water_rates wr
                JOIN {schema}.rate_schedules rs
                  ON rs.pwsid = wr.pwsid
                  AND rs.source_key = wr.source
                WHERE wr.bill_6ccf IS NOT NULL
                   OR wr.bill_9ccf IS NOT NULL
                   OR wr.bill_12ccf IS NOT NULL
                   OR wr.bill_24ccf IS NOT NULL
            """)).scalar()
        return {"would_update": count, "dry_run": True}

    with engine.connect() as conn:
        result = conn.execute(text(f"""
            UPDATE {schema}.rate_schedules rs
            SET bill_6ccf = wr.bill_6ccf,
                bill_9ccf = wr.bill_9ccf,
                bill_12ccf = wr.bill_12ccf,
                bill_24ccf = wr.bill_24ccf
            FROM {schema}.water_rates wr
            WHERE rs.pwsid = wr.pwsid
              AND rs.source_key = wr.source
              AND (wr.bill_6ccf IS NOT NULL
                   OR wr.bill_9ccf IS NOT NULL
                   OR wr.bill_12ccf IS NOT NULL
                   OR wr.bill_24ccf IS NOT NULL)
              AND (rs.bill_6ccf IS NULL
                   AND rs.bill_9ccf IS NULL
                   AND rs.bill_12ccf IS NULL
                   AND rs.bill_24ccf IS NULL)
        """))
        updated = result.rowcount
        conn.commit()

    logger.info(f"  Backfilled eAR bill columns for {updated} records")
    return {"updated": updated}


def verify_sync() -> dict:
    """Verify that all water_rates sources exist in rate_schedules.

    Returns
    -------
    dict
        Per-source comparison of row counts.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        # water_rates counts
        wr_counts = {}
        rows = conn.execute(text(f"""
            SELECT source, COUNT(*) as cnt, COUNT(DISTINCT pwsid) as pwsids
            FROM {schema}.water_rates
            GROUP BY source ORDER BY source
        """)).fetchall()
        for r in rows:
            wr_counts[r.source] = {"rows": r.cnt, "pwsids": r.pwsids}

        # rate_schedules counts
        rs_counts = {}
        rows = conn.execute(text(f"""
            SELECT source_key, COUNT(*) as cnt, COUNT(DISTINCT pwsid) as pwsids
            FROM {schema}.rate_schedules
            GROUP BY source_key ORDER BY source_key
        """)).fetchall()
        for r in rows:
            rs_counts[r.source_key] = {"rows": r.cnt, "pwsids": r.pwsids}

    # Compare
    all_sources = sorted(set(list(wr_counts.keys()) + list(rs_counts.keys())))
    results = {}
    missing_from_rs = 0
    for src in all_sources:
        wr = wr_counts.get(src, {"rows": 0, "pwsids": 0})
        rs = rs_counts.get(src, {"rows": 0, "pwsids": 0})
        gap = wr["pwsids"] - rs["pwsids"]
        results[src] = {
            "wr_rows": wr["rows"],
            "wr_pwsids": wr["pwsids"],
            "rs_rows": rs["rows"],
            "rs_pwsids": rs["pwsids"],
            "pwsid_gap": gap,
        }
        if gap > 0:
            missing_from_rs += gap

    return {"sources": results, "total_pwsid_gap": missing_from_rs}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Sync all water_rates records to rate_schedules (Phase 2 deprecation)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would be synced, no DB writes",
    )
    parser.add_argument(
        "--source", metavar="KEY",
        help="Sync only a specific source (e.g., efc_ct_2018)",
    )
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Just run verification, no sync",
    )
    args = parser.parse_args()

    if args.verify_only:
        logger.info("=== Verification Only ===")
        verify = verify_sync()
        print(f"\n{'Source':<35} {'WR rows':>8} {'WR PWS':>8} {'RS rows':>8} {'RS PWS':>8} {'Gap':>6}")
        print("-" * 80)
        for src, v in sorted(verify["sources"].items()):
            gap_str = f"{v['pwsid_gap']:+d}" if v['pwsid_gap'] != 0 else "  0"
            marker = " ⚠" if v["pwsid_gap"] > 0 else ""
            print(f"  {src:<33} {v['wr_rows']:>8} {v['wr_pwsids']:>8} "
                  f"{v['rs_rows']:>8} {v['rs_pwsids']:>8} {gap_str:>6}{marker}")
        print(f"\nTotal PWSID gap: {verify['total_pwsid_gap']}")
        return

    # Step 1: Load water_rates
    logger.info("=== Phase 2: Sync water_rates → rate_schedules ===")
    records = load_water_rates(source_filter=args.source)

    if not records:
        logger.info("No records to sync.")
        return

    # Step 2: Sync
    logger.info(f"\nSyncing {len(records)} records to rate_schedules...")
    result = sync_to_rate_schedules(records, dry_run=args.dry_run)

    if args.dry_run:
        print(f"\nDRY RUN — would sync {result['total']} records:")
        for src, cnt in sorted(result["by_source"].items()):
            print(f"  {src}: {cnt}")
        return

    logger.info(f"\nSync complete:")
    logger.info(f"  Synced:     {result['synced']}")
    logger.info(f"  Skipped FK: {result['skipped_fk']}")
    logger.info(f"  Errors:     {result['errors']}")
    for src, cnt in sorted(result["by_source"].items()):
        logger.info(f"    {src}: {cnt}")

    # Step 3: Backfill eAR bill columns
    logger.info(f"\nBackfilling eAR bill columns (6/9/12/24 CCF)...")
    backfill = backfill_ear_bill_columns(dry_run=False)
    logger.info(f"  Updated: {backfill['updated']} records")

    # Step 4: Verify
    logger.info(f"\nRunning verification...")
    verify = verify_sync()
    print(f"\n{'Source':<35} {'WR rows':>8} {'WR PWS':>8} {'RS rows':>8} {'RS PWS':>8} {'Gap':>6}")
    print("-" * 80)
    for src, v in sorted(verify["sources"].items()):
        gap_str = f"{v['pwsid_gap']:+d}" if v['pwsid_gap'] != 0 else "  0"
        marker = " ⚠" if v["pwsid_gap"] > 0 else ""
        print(f"  {src:<33} {v['wr_rows']:>8} {v['wr_pwsids']:>8} "
              f"{v['rs_rows']:>8} {v['rs_pwsids']:>8} {gap_str:>6}{marker}")
    print(f"\nTotal PWSID gap: {verify['total_pwsid_gap']}")

    if verify["total_pwsid_gap"] > 0:
        logger.warning(
            f"⚠ {verify['total_pwsid_gap']} PWSIDs still missing from rate_schedules. "
            f"These are likely FK violations (PWSID not in cws_boundaries)."
        )
    else:
        logger.info("✓ All water_rates PWSIDs now exist in rate_schedules.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Backfill eAR Computed Bills (bill_5ccf, bill_10ccf, bill_20ccf)

Purpose:
    eAR records have pre-computed state-reported bills at 6/9/12/24 CCF
    but NOT at 5/10/20 CCF — the standard benchmarks used by scraped_llm,
    EFC, and Duke. This script computes bill_5ccf, bill_10ccf, bill_20ccf
    from existing volumetric_tiers + fixed_charges JSONB, filling the gap
    for cross-source comparability.

    Only updates NULL bill columns — never overwrites pre-computed
    state-reported bills.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    python scripts/backfill_ear_bills.py --dry-run    # Preview
    python scripts/backfill_ear_bills.py              # Execute

Notes:
    - Idempotent: only fills NULL columns, safe to re-run
    - Does NOT overwrite state-reported bills (6/9/12/24 CCF)
    - Appends provenance note to parse_notes
    - Only processes records with volumetric_tiers IS NOT NULL

Data Sources:
    - rate_schedules.volumetric_tiers + rate_schedules.fixed_charges
    - Conversion: 5 CCF = 3,740 gal, 10 CCF = 7,480 gal, 20 CCF = 14,960 gal
"""

# Standard library imports
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Third-party imports
from loguru import logger
from sqlalchemy import text

# Local imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import compute_bill_at_gallons

# Constants
SCHEMA = settings.utility_schema
CCF_TO_GAL = 748.0
BILL_LEVELS = {
    "bill_5ccf": 5 * CCF_TO_GAL,    # 3,740 gal
    "bill_10ccf": 10 * CCF_TO_GAL,  # 7,480 gal
    "bill_20ccf": 20 * CCF_TO_GAL,  # 14,960 gal
}
PROVENANCE_TAG = "[COMPUTED 2026-04-02] bill_5/10/20ccf derived from tiers+fixed"


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill eAR computed bills (5/10/20 CCF)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    parser.add_argument(
        "--source-keys", nargs="+",
        default=["swrcb_ear_2020", "swrcb_ear_2021", "swrcb_ear_2022"],
        help="Source keys to process. Default: all 3 eAR vintages.",
    )
    return parser.parse_args()


def backfill_bills(conn, source_keys: list[str], dry_run: bool) -> dict:
    """Compute and backfill missing bill columns for eAR records.

    Returns
    -------
    dict
        Stats: total, computed, skipped per source.
    """
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)

    rows = conn.execute(text(f"""
        SELECT id, pwsid, source_key,
               volumetric_tiers, fixed_charges,
               bill_5ccf, bill_10ccf, bill_20ccf,
               parse_notes
        FROM {SCHEMA}.rate_schedules
        WHERE source_key IN ({sk_list})
          AND volumetric_tiers IS NOT NULL
        ORDER BY source_key, pwsid
    """)).fetchall()

    stats = {"total": len(rows), "updated": 0, "skipped_already_filled": 0}
    per_source = {}
    updates = []

    for row in rows:
        source_key = row.source_key
        if source_key not in per_source:
            per_source[source_key] = {"total": 0, "updated": 0, "skipped": 0}
        per_source[source_key]["total"] += 1

        tiers = row.volumetric_tiers
        fixed = row.fixed_charges

        # Compute missing bills
        new_values = {}
        for col, gallons in BILL_LEVELS.items():
            current = getattr(row, col)
            if current is not None:
                continue  # Don't overwrite existing
            computed = compute_bill_at_gallons(gallons, tiers, fixed)
            if computed is not None:
                new_values[col] = computed

        if not new_values:
            stats["skipped_already_filled"] += 1
            per_source[source_key]["skipped"] += 1
            continue

        # Build parse_notes update
        notes = row.parse_notes or ""
        if PROVENANCE_TAG not in notes:
            notes = f"{notes}; {PROVENANCE_TAG}" if notes else PROVENANCE_TAG

        new_values["parse_notes"] = notes
        new_values["id"] = row.id

        updates.append(new_values)
        stats["updated"] += 1
        per_source[source_key]["updated"] += 1

    # Log summary
    logger.info(f"  Total with tiers: {stats['total']}")
    logger.info(f"  Updated: {stats['updated']}")
    logger.info(f"  Skipped (already filled): {stats['skipped_already_filled']}")

    logger.info("\n  --- Per-Source Breakdown ---")
    for sk in sorted(per_source.keys()):
        ps = per_source[sk]
        logger.info(
            f"  {sk}: total={ps['total']} "
            f"updated={ps['updated']} skipped={ps['skipped']}"
        )

    # Show sample of computed bills
    if updates:
        logger.info("\n  --- Sample Computed Bills (first 10) ---")
        for u in updates[:10]:
            vals = {k: v for k, v in u.items() if k.startswith("bill_")}
            logger.info(f"  id={u['id']}: {vals}")

    if not dry_run and updates:
        for u in updates:
            set_clauses = []
            params = {"id": u["id"], "parse_notes": u["parse_notes"]}
            for col in BILL_LEVELS:
                if col in u:
                    set_clauses.append(f"{col} = :{col}")
                    params[col] = u[col]
            set_clauses.append("parse_notes = :parse_notes")

            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET {', '.join(set_clauses)}
                WHERE id = :id
            """), params)
        logger.info(f"  Updated {len(updates)} records in database")

    return stats


def log_pipeline_run(conn, started, source_keys, stats, dry_run):
    """Log to pipeline_runs."""
    if dry_run:
        logger.info("  [dry-run] Would log to pipeline_runs")
        return

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "ear_backfill_computed_bills",
        "started": started,
        "count": stats.get("updated", 0),
        "notes": (
            f"sources={','.join(source_keys)}, "
            f"updated={stats.get('updated', 0)}, "
            f"skipped={stats.get('skipped_already_filled', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== eAR Bill Backfill (5/10/20 CCF) ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"Sources: {args.source_keys}")

    with engine.connect() as conn:
        logger.info(f"Schema: {SCHEMA}.rate_schedules")

        stats = backfill_bills(conn, args.source_keys, dry_run=args.dry_run)

        logger.info("\n--- Pipeline Logging ---")
        log_pipeline_run(conn, started, args.source_keys, stats, args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Backfill Complete ===")


if __name__ == "__main__":
    main()

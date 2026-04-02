#!/usr/bin/env python3
"""
EFC Comparability Migration — Confidence Recalibration

Purpose:
    Patch existing EFC records in rate_schedules to apply nuanced confidence
    levels consistent with the Duke-established confidence criteria.

    Primary change: downgrade 1-tier (uniform) records from "high" to "medium"
    confidence. The Duke audit (Sprint 28) established that high confidence
    requires tier_count >= 2.

    JSONB format audit (Sprint 28 EFC pilot) confirmed that EFC records
    already have clean JSONB — no extra keys, no contiguity gaps, no
    duplicate tiers. Bills are NOT recalculated because EFC curve-interpolated
    bills are more accurate than tier-recalculated bills (tiers are reverse-
    engineered from the bill curve and are intentionally lossy).

    Generalized for any/all EFC source_keys.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    # Dry-run on pilot 4 states:
    python scripts/migrate_efc_to_comparable.py --dry-run

    # Specific sources:
    python scripts/migrate_efc_to_comparable.py --source-keys efc_ar_2020 efc_ia_2023 --dry-run

    # All EFC sources:
    python scripts/migrate_efc_to_comparable.py --all-efc --dry-run

    # Execute (remove --dry-run):
    python scripts/migrate_efc_to_comparable.py

Notes:
    - Idempotent: safe to re-run (patches are deterministic)
    - Does NOT recalculate bills (curve-interpolated bills are ground truth)
    - Does NOT touch scraped_llm, water_rates, or any non-EFC records
    - JSONB structure already clean — no structural fixes needed
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Add project to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

SCHEMA = settings.utility_schema

# Default pilot states
PILOT_SOURCES = ["efc_ar_2020", "efc_ia_2023", "efc_wi_2016", "efc_ga_2019"]


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="EFC comparability migration — confidence recalibration"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    parser.add_argument(
        "--source-keys", nargs="+", default=None,
        help="Specific EFC source_key(s). Default: pilot 4 states.",
    )
    parser.add_argument(
        "--all-efc", action="store_true",
        help="Process all EFC sources (source_key LIKE 'efc_%%').",
    )
    return parser.parse_args()


def resolve_source_keys(args, conn) -> list[str]:
    """Determine which EFC source_keys to process."""
    if args.all_efc:
        result = conn.execute(text(f"""
            SELECT DISTINCT source_key
            FROM {SCHEMA}.rate_schedules
            WHERE source_key LIKE 'efc_%%'
            ORDER BY source_key
        """))
        keys = [r[0] for r in result]
        logger.info(f"Found {len(keys)} EFC sources in database")
        return keys
    elif args.source_keys:
        return args.source_keys
    else:
        return PILOT_SOURCES


def recalibrate_confidence(conn, source_keys: list[str], dry_run: bool) -> dict:
    """Apply nuanced confidence levels to EFC records.

    Confidence criteria (from Duke audit, Sprint 28):
      - high:   bill_10ccf in [10, 200] AND tier_count >= 2 AND no review flags
      - medium: bill_10ccf in [5, 500] OR (tier_count == 1 and was "high")
      - low:    bill_10ccf is NULL or outside [5, 500]

    Returns
    -------
    dict
        Stats: total processed, confidence changes per source.
    """
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)

    rows = conn.execute(text(f"""
        SELECT id, pwsid, source_key, bill_10ccf,
               tier_count, confidence, rate_structure_type,
               bill_5ccf, bill_20ccf, fixed_charges
        FROM {SCHEMA}.rate_schedules
        WHERE source_key IN ({sk_list})
        ORDER BY source_key, pwsid
    """)).fetchall()

    stats = {"total": len(rows), "confidence_changed": 0, "flagged_review": 0}
    per_source = {}
    updates = []

    for row in rows:
        source_key = row.source_key
        if source_key not in per_source:
            per_source[source_key] = {"total": 0, "changed": 0, "review": 0}
        per_source[source_key]["total"] += 1

        bill_10 = row.bill_10ccf
        tier_count = row.tier_count or 0
        has_fixed = row.fixed_charges is not None and len(row.fixed_charges) > 0

        review_reasons = []

        # Check for identical bills across volumes for tiered structures
        if (
            tier_count > 1
            and row.bill_5ccf is not None
            and bill_10 is not None
            and row.bill_20ccf is not None
            and row.bill_5ccf == bill_10 == row.bill_20ccf
            and row.rate_structure_type not in ("flat", "uniform")
        ):
            review_reasons.append(
                "increasing_block but identical bills at 5/10/20 CCF"
            )

        # Determine new confidence
        if (
            bill_10 is not None
            and 10 <= bill_10 <= 200
            and tier_count >= 2
        ):
            new_confidence = "high"
        elif bill_10 is not None and 5 <= bill_10 <= 500:
            new_confidence = "medium"
        elif bill_10 is None and has_fixed:
            new_confidence = "low"
        elif bill_10 is not None:
            new_confidence = "low"
        else:
            new_confidence = "low"

        # Uniform (1 tier) → cap at medium
        if tier_count <= 1 and new_confidence == "high":
            new_confidence = "medium"

        needs_review = bool(review_reasons)
        changed = new_confidence != row.confidence

        if needs_review:
            stats["flagged_review"] += 1
            per_source[source_key]["review"] += 1

        if changed or needs_review:
            stats["confidence_changed"] += 1 if changed else 0
            if changed:
                per_source[source_key]["changed"] += 1

            updates.append({
                "id": row.id,
                "confidence": new_confidence,
                "needs_review": needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
            })

    # Log summary
    logger.info(f"  Total records: {stats['total']}")
    logger.info(f"  Confidence changed: {stats['confidence_changed']}")
    logger.info(f"  Flagged for review: {stats['flagged_review']}")
    logger.info(f"  Records to update: {len(updates)}")

    logger.info(f"\n  --- Per-Source Breakdown ---")
    for sk in sorted(per_source.keys()):
        ps = per_source[sk]
        logger.info(
            f"  {sk}: total={ps['total']} "
            f"confidence_changed={ps['changed']} review={ps['review']}"
        )

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET confidence = :confidence,
                    needs_review = :needs_review,
                    review_reason = :review_reason
                WHERE id = :id
            """), u)
        logger.info(f"  Updated {len(updates)} records in database")

    return stats


def log_pipeline_run(
    conn, started: datetime, source_keys: list[str],
    stats: dict, dry_run: bool,
) -> None:
    """Log migration to pipeline_runs."""
    if dry_run:
        logger.info("  [dry-run] Would log to pipeline_runs")
        return

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "efc_confidence_recalibration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"sources={','.join(source_keys)}, "
            f"confidence_changed={stats.get('confidence_changed', 0)}, "
            f"flagged_review={stats.get('flagged_review', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== EFC Comparability Migration — Confidence Recalibration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        source_keys = resolve_source_keys(args, conn)
        logger.info(f"Target sources: {source_keys}")
        logger.info(f"Schema: {SCHEMA}.rate_schedules")

        # Recalibrate confidence
        logger.info("\n--- Confidence Recalibration ---")
        stats = recalibrate_confidence(conn, source_keys, args.dry_run)

        # Log pipeline run
        logger.info("\n--- Pipeline Logging ---")
        log_pipeline_run(conn, started, source_keys, stats, args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    main()

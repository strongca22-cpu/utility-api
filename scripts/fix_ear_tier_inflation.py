#!/usr/bin/env python3
"""
Fix eAR Tier Limit Inflation

Purpose:
    Corrects eAR records where tier limits are reported in gallons instead
    of CCF/HCF. The SWRCB eAR WRSFMetricUsage columns are supposed to be
    in HCF (hundred cubic feet = CCF), but many utilities report in gallons
    (748x inflation) or kgal (1.337x inflation, less common).

    Strategy: NULL out inflated tier structures (they're unreliable in wrong
    units) while preserving pre-computed bill amounts from the state filing
    (WR6/9/12/24HCFDWCharges) when those are reasonable. This is safer than
    applying a correction factor because the inflation factor varies across
    utilities (748x for gallons, 1000x for kgal, other factors).

    Records with inflated pre-computed bills (>$500/month at 12 CCF) also
    get their bill columns NULLed — the state's own calculation used the
    wrong tier limits.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    python scripts/fix_ear_tier_inflation.py              # Apply fixes
    python scripts/fix_ear_tier_inflation.py --dry-run     # Preview only

Notes:
    - Threshold: tier limit > 100 CCF considered suspect for residential
      (US median household uses ~5 CCF/month; 100 CCF/month is extreme)
    - Pre-computed bill threshold: $500/month at 12 CCF
    - Idempotent: can be re-run safely
    - Appends to parse_notes, does not overwrite
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


# Residential tier limits should never exceed this in CCF/month.
# US median household uses ~5 CCF/month. Even drought surcharge tiers
# rarely exceed 50 CCF. 100 CCF is a very generous threshold.
TIER_LIMIT_THRESHOLD = 100  # CCF
TIER_LIMIT_THRESHOLD_GAL = 100 * 748  # 74,800 gallons (rate_schedules uses gallons)

# Pre-computed bill amounts above this are clearly wrong
BILL_THRESHOLD = 500  # $/month at any standard CCF level


def run_fix(dry_run: bool = False) -> dict:
    """Fix eAR tier limit inflation.

    Parameters
    ----------
    dry_run : bool
        If True, report but don't modify the database.

    Returns
    -------
    dict
        Stats: tiers_nulled, bills_nulled, total_affected.
    """
    logger.info("=== Fix eAR Tier Limit Inflation ===")

    schema = settings.utility_schema
    now = datetime.now(timezone.utc).isoformat()

    with engine.connect() as conn:
        # Find all eAR records with inflated tier limits in rate_schedules
        # rate_schedules stores tiers in JSONB (gallons), so we check max_gal
        inflated = conn.execute(text(f"""
            SELECT rs.id, rs.pwsid, rs.source_key AS source, c.pws_name AS utility_name,
                   rs.volumetric_tiers,
                   rs.bill_6ccf, rs.bill_9ccf, rs.bill_12ccf, rs.bill_24ccf,
                   (rs.fixed_charges->0->>'amount')::float AS fixed_charge_monthly,
                   rs.parse_notes
            FROM {schema}.rate_schedules rs
            LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = rs.pwsid
            WHERE rs.source_key LIKE 'swrcb_ear_%'
              AND rs.volumetric_tiers IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM jsonb_array_elements(rs.volumetric_tiers) t
                  WHERE (t->>'max_gal')::float > :threshold_gal
              )
        """), {"threshold_gal": TIER_LIMIT_THRESHOLD_GAL}).fetchall()

        logger.info(f"Found {len(inflated)} eAR records with tier limits > {TIER_LIMIT_THRESHOLD} CCF ({TIER_LIMIT_THRESHOLD_GAL} gal)")

        stats = {
            "total_found": len(inflated),
            "tiers_nulled": 0,
            "bills_nulled": 0,
            "bills_preserved": 0,
            "pwsids_affected": len(set(r[1] for r in inflated)),
        }

        for row in inflated:
            rec_id = row[0]
            pwsid = row[1]
            source = row[2]
            name = row[3] or ""
            bill_6 = row[5]
            bill_9 = row[6]
            bill_12 = row[7]
            bill_24 = row[8]
            fixed = row[9]
            notes = row[10] or ""

            # Determine max tier limit for logging (convert back to CCF for display)
            import json as _json
            tiers = row[4] if isinstance(row[4], list) else _json.loads(row[4]) if row[4] else []
            max_gal = max((t.get("max_gal", 0) or 0) for t in tiers) if tiers else 0
            max_limit_ccf = max_gal / 748.0

            # Check if pre-computed bills are reasonable
            bills_ok = True
            for b in [bill_6, bill_9, bill_12, bill_24]:
                if b is not None and b > BILL_THRESHOLD:
                    bills_ok = False
                    break

            # Also check if bills are suspiciously low
            if bill_12 is not None and bill_12 < 2.0:
                bills_ok = False

            action_note = f"[FIX {now[:10]}] Tier limits inflated (max={max_limit_ccf:.0f} CCF, threshold={TIER_LIMIT_THRESHOLD}). Tiers NULLed."
            if not bills_ok:
                action_note += " Bills also NULLed (inflated/suspect)."
            else:
                action_note += " Pre-computed bills preserved (state-reported, reasonable)."

            new_notes = f"{notes}; {action_note}" if notes else action_note

            if dry_run:
                status = "bills_nulled" if not bills_ok else "tiers_only"
                logger.info(
                    f"  [DRY RUN] {pwsid} {source} {name[:30]:30s} "
                    f"max_limit={max_limit_ccf:.0f}CCF bill@12=${bill_12 or 0:.2f} → {status}"
                )
            else:
                if bills_ok:
                    # NULL tiers only, keep bills
                    conn.execute(text(f"""
                        UPDATE {schema}.rate_schedules
                        SET volumetric_tiers = NULL,
                            tier_count = 0,
                            conservation_signal = NULL,
                            parse_notes = :notes,
                            confidence = 'medium'
                        WHERE id = :id
                    """), {"id": rec_id, "notes": new_notes})
                    stats["bills_preserved"] += 1
                else:
                    # NULL tiers AND bills
                    conn.execute(text(f"""
                        UPDATE {schema}.rate_schedules
                        SET volumetric_tiers = NULL,
                            tier_count = 0,
                            conservation_signal = NULL,
                            bill_6ccf = NULL, bill_9ccf = NULL,
                            bill_12ccf = NULL, bill_24ccf = NULL,
                            parse_notes = :notes,
                            confidence = 'low'
                        WHERE id = :id
                    """), {"id": rec_id, "notes": new_notes})
                    stats["bills_nulled"] += 1

            stats["tiers_nulled"] += 1

        if not dry_run:
            conn.commit()

            # Log pipeline run
            conn.execute(text(f"""
                INSERT INTO {schema}.pipeline_runs (step_name, started_at, finished_at, row_count, status, notes)
                VALUES (:step, :started, :finished, :row_count, :status, :notes)
            """), {
                "step": "fix_ear_tier_inflation",
                "started": datetime.now(timezone.utc),
                "finished": datetime.now(timezone.utc),
                "row_count": stats["tiers_nulled"],
                "status": "success",
                "notes": (
                    f"Fixed {stats['tiers_nulled']} eAR records with inflated tier limits "
                    f"(>{TIER_LIMIT_THRESHOLD} CCF). {stats['bills_preserved']} bills preserved, "
                    f"{stats['bills_nulled']} bills also NULLed."
                ),
            })
            conn.commit()

    logger.info(f"Tiers NULLed: {stats['tiers_nulled']}")
    logger.info(f"Bills preserved (reasonable): {stats['bills_preserved']}")
    logger.info(f"Bills also NULLed (inflated): {stats['bills_nulled']}")
    logger.info(f"PWSIDs affected: {stats['pwsids_affected']}")
    logger.info("=== Fix Complete ===")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Fix eAR tier limit inflation")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying DB")
    args = parser.parse_args()
    run_fix(dry_run=args.dry_run)


if __name__ == "__main__":
    main()

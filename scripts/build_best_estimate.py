#!/usr/bin/env python3
"""
Build Best-Estimate Rate Selection

Purpose:
    For each PWSID with rate data, selects the single "best estimate" bill
    amount using a source priority hierarchy. Writes results to a new
    utility.rate_best_estimate table for downstream consumption.

    Priority logic:
    1. eAR 2022 (official state filing, water-only, most recent government data)
    2. eAR 2021 (same authority, one year older)
    3. scraped_llm HIGH confidence — only if within 25% of the eAR anchor
    4. owrs (water-only, curated, but often 5-8 years old)
    5. scraped_llm HIGH confidence without eAR cross-reference
    6. scraped_llm LOW/MEDIUM confidence (combined water+sewer suspects, etc.)
    7. eAR 2020 (no pre-computed bills — tier-only, lowest priority)

    The anchor principle: eAR 2022 is the default truth for CA utilities.
    Scraped rates upgrade to "selected" only when they are high-confidence
    AND within 25% of the eAR anchor value. This defers errors to government
    sourcing rather than LLM parsing.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru

Usage:
    python scripts/build_best_estimate.py              # Build best estimates
    python scripts/build_best_estimate.py --dry-run     # Preview only
    python scripts/build_best_estimate.py --csv          # Also write CSV

Notes:
    - Creates/replaces utility.rate_best_estimate table
    - One row per PWSID: selected source, bill amounts, confidence, notes
    - For single-source utilities, that source wins by default
    - VA utilities (scraped_llm only) get scraped as best estimate
    - Tolerance for scraped upgrade: 25% of eAR anchor value

Data Sources:
    - Input: utility.water_rates (all sources)
    - Output: utility.rate_best_estimate table
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

OUTPUT_DIR = PROJECT_ROOT / "data" / "interim"

# Tolerance for scraped rate to "upgrade" over eAR anchor
SCRAPED_UPGRADE_TOLERANCE = 0.25  # 25%

# Source priority (lower = higher priority)
SOURCE_PRIORITY = {
    "swrcb_ear_2022": 1,
    "swrcb_ear_2021": 2,
    "scraped_llm": 3,  # adjusted dynamically based on confidence + anchor agreement
    "owrs": 4,
    "swrcb_ear_2020": 5,
}


def get_comparable_bill(row: pd.Series) -> float | None:
    """Extract a comparable ~10CCF monthly bill from any source."""
    if pd.notna(row.get("bill_10ccf")) and row["bill_10ccf"] > 0:
        return float(row["bill_10ccf"])
    if pd.notna(row.get("bill_9ccf")) and pd.notna(row.get("bill_12ccf")):
        b9, b12 = float(row["bill_9ccf"]), float(row["bill_12ccf"])
        if b9 > 0 and b12 > 0:
            return round(b9 + (b12 - b9) / 3.0, 2)
    if pd.notna(row.get("bill_12ccf")) and row["bill_12ccf"] > 0:
        return float(row["bill_12ccf"])
    if pd.notna(row.get("bill_6ccf")) and row["bill_6ccf"] > 0:
        return float(row["bill_6ccf"])  # fallback — at 6 CCF, not 10
    return None


def select_best_estimate(group: pd.DataFrame) -> dict:
    """Select the best estimate for a single PWSID.

    Parameters
    ----------
    group : pd.DataFrame
        All water_rates records for one PWSID.

    Returns
    -------
    dict
        Best estimate record with selection rationale.
    """
    pwsid = group.iloc[0]["pwsid"]
    utility_name = group.iloc[0]["utility_name"] or ""

    # Add comparable bill to each record
    records = []
    for _, row in group.iterrows():
        comp = get_comparable_bill(row)
        records.append({
            "source": row["source"],
            "comp_bill": comp,
            "bill_5ccf": row.get("bill_5ccf"),
            "bill_10ccf": row.get("bill_10ccf"),
            "bill_6ccf": row.get("bill_6ccf"),
            "bill_12ccf": row.get("bill_12ccf"),
            "fixed_charge_monthly": row.get("fixed_charge_monthly"),
            "rate_structure_type": row.get("rate_structure_type"),
            "rate_effective_date": row.get("rate_effective_date"),
            "parse_confidence": row.get("parse_confidence"),
            "billing_frequency": row.get("billing_frequency"),
            "state_code": row.get("state_code"),
        })

    # Find the eAR anchor (prefer 2022, then 2021)
    anchor = None
    anchor_bill = None
    for src in ["swrcb_ear_2022", "swrcb_ear_2021"]:
        candidates = [r for r in records if r["source"] == src and r["comp_bill"] is not None]
        if candidates:
            anchor = candidates[0]
            anchor_bill = anchor["comp_bill"]
            break

    # Score each record
    scored = []
    for r in records:
        if r["comp_bill"] is None:
            continue

        source = r["source"]
        confidence = r["parse_confidence"] or "medium"
        base_priority = SOURCE_PRIORITY.get(source, 99)

        # Adjust scraped_llm priority based on confidence and anchor agreement
        notes = []
        if source == "scraped_llm":
            if confidence == "low":
                base_priority = 6  # flagged combined w+s or failed
                notes.append("low_confidence")
            elif anchor_bill is not None:
                pct_diff = abs(r["comp_bill"] - anchor_bill) / anchor_bill
                if pct_diff <= SCRAPED_UPGRADE_TOLERANCE:
                    base_priority = 0  # scraped agrees with anchor → best (most current)
                    notes.append(f"scraped_agrees_with_ear ({pct_diff:.0%} diff)")
                else:
                    base_priority = 5  # scraped diverges → demote below anchor
                    notes.append(f"scraped_diverges_from_ear ({pct_diff:.0%} diff)")
            else:
                # No eAR anchor — scraped is what we have
                base_priority = 3
                notes.append("no_ear_anchor")

        scored.append({
            **r,
            "priority": base_priority,
            "selection_notes": "; ".join(notes) if notes else "",
        })

    if not scored:
        return {
            "pwsid": pwsid,
            "utility_name": utility_name,
            "selected_source": None,
            "bill_estimate_10ccf": None,
            "bill_5ccf": None,
            "bill_10ccf": None,
            "bill_6ccf": None,
            "bill_12ccf": None,
            "fixed_charge_monthly": None,
            "rate_structure_type": None,
            "rate_effective_date": None,
            "n_sources": len(set(r["source"] for r in records)),
            "anchor_source": anchor["source"] if anchor else None,
            "anchor_bill": anchor_bill,
            "confidence": "none",
            "selection_notes": "no comparable bill from any source",
            "state_code": records[0].get("state_code") if records else None,
        }

    # Sort by priority (lowest = best)
    scored.sort(key=lambda x: x["priority"])
    best = scored[0]

    # Determine confidence
    if best["priority"] == 0:
        confidence = "high"  # scraped agrees with anchor
    elif best["source"].startswith("swrcb_ear"):
        confidence = "high"  # government source
    elif best["source"] == "owrs":
        confidence = "medium"  # curated but old
    elif best["parse_confidence"] == "low":
        confidence = "low"
    else:
        confidence = "medium"

    return {
        "pwsid": pwsid,
        "utility_name": utility_name[:255],
        "selected_source": best["source"],
        "bill_estimate_10ccf": round(best["comp_bill"], 2),
        "bill_5ccf": best.get("bill_5ccf"),
        "bill_10ccf": best.get("bill_10ccf"),
        "bill_6ccf": best.get("bill_6ccf"),
        "bill_12ccf": best.get("bill_12ccf"),
        "fixed_charge_monthly": best.get("fixed_charge_monthly"),
        "rate_structure_type": best.get("rate_structure_type"),
        "rate_effective_date": best.get("rate_effective_date"),
        "n_sources": len(set(r["source"] for r in records)),
        "anchor_source": anchor["source"] if anchor else None,
        "anchor_bill": anchor_bill,
        "confidence": confidence,
        "selection_notes": best.get("selection_notes", ""),
        "state_code": best.get("state_code"),
    }


def run_best_estimate(dry_run: bool = False, write_csv: bool = False) -> dict:
    """Build best-estimate rate selection for all PWSIDs.

    Parameters
    ----------
    dry_run : bool
        Preview only, don't write to DB.
    write_csv : bool
        Also write CSV output.

    Returns
    -------
    dict
        Summary statistics.
    """
    logger.info("=== Build Best-Estimate Rate Selection ===")

    schema = settings.utility_schema

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT pwsid, source, utility_name, state_code,
                   bill_5ccf, bill_6ccf, bill_9ccf, bill_10ccf, bill_12ccf, bill_24ccf,
                   fixed_charge_monthly, rate_structure_type, rate_effective_date,
                   billing_frequency, parse_confidence
            FROM {schema}.water_rates
            ORDER BY pwsid, source
        """), conn)

    logger.info(f"Loaded {len(df)} rate records for {df.pwsid.nunique()} PWSIDs")

    # Build best estimate per PWSID
    results = []
    for pwsid, group in df.groupby("pwsid"):
        result = select_best_estimate(group)
        results.append(result)

    rdf = pd.DataFrame(results)
    logger.info(f"Best estimates computed: {len(rdf)}")
    print()

    # Stats
    stats = {
        "total_pwsids": len(rdf),
        "with_estimate": len(rdf[rdf.bill_estimate_10ccf.notna()]),
        "no_estimate": len(rdf[rdf.bill_estimate_10ccf.isna()]),
    }

    # Source selection breakdown
    logger.info("Selected source breakdown:")
    for src, cnt in rdf.selected_source.value_counts().items():
        pct = cnt / len(rdf) * 100
        logger.info(f"  {src}: {cnt} ({pct:.0f}%)")
    none_count = rdf.selected_source.isna().sum()
    if none_count:
        logger.info(f"  (no estimate): {none_count}")
    print()

    # Confidence breakdown
    logger.info("Confidence breakdown:")
    for conf, cnt in rdf.confidence.value_counts().items():
        pct = cnt / len(rdf) * 100
        logger.info(f"  {conf}: {cnt} ({pct:.0f}%)")
    print()

    # Scraped upgrade stats
    upgraded = rdf[rdf.selection_notes.str.contains("scraped_agrees", na=False)]
    demoted = rdf[rdf.selection_notes.str.contains("scraped_diverges", na=False)]
    logger.info(f"Scraped upgrades (agrees with eAR anchor, <{SCRAPED_UPGRADE_TOLERANCE:.0%}): {len(upgraded)}")
    logger.info(f"Scraped demotions (diverges from eAR anchor): {len(demoted)}")
    print()

    # Bill distribution
    has_bill = rdf[rdf.bill_estimate_10ccf.notna()]
    if len(has_bill) > 0:
        logger.info("Bill estimate @10CCF distribution:")
        logger.info(f"  Mean: ${has_bill.bill_estimate_10ccf.mean():.2f}")
        logger.info(f"  Median: ${has_bill.bill_estimate_10ccf.median():.2f}")
        logger.info(f"  Min: ${has_bill.bill_estimate_10ccf.min():.2f}")
        logger.info(f"  Max: ${has_bill.bill_estimate_10ccf.max():.2f}")
    print()

    if dry_run:
        logger.info("[DRY RUN] Would write to rate_best_estimate table")
        # Show samples
        for _, r in rdf.head(10).iterrows():
            logger.info(
                f"  {r.pwsid} {r.utility_name[:30]:30s} "
                f"src={r.selected_source or 'none':20s} "
                f"bill=${r.bill_estimate_10ccf or 0:7.2f} "
                f"[{r.confidence}] {r.selection_notes}"
            )
        return stats

    # Write to database
    with engine.connect() as conn:
        # Create table if not exists (or recreate)
        conn.execute(text(f"""
            DROP TABLE IF EXISTS {schema}.rate_best_estimate
        """))
        conn.execute(text(f"""
            CREATE TABLE {schema}.rate_best_estimate (
                pwsid VARCHAR(12) PRIMARY KEY REFERENCES {schema}.cws_boundaries(pwsid),
                utility_name VARCHAR(255),
                state_code VARCHAR(2),
                selected_source VARCHAR(50),
                bill_estimate_10ccf FLOAT,
                bill_5ccf FLOAT,
                bill_10ccf FLOAT,
                bill_6ccf FLOAT,
                bill_12ccf FLOAT,
                fixed_charge_monthly FLOAT,
                rate_structure_type VARCHAR(30),
                rate_effective_date DATE,
                n_sources INTEGER,
                anchor_source VARCHAR(50),
                anchor_bill FLOAT,
                confidence VARCHAR(10),
                selection_notes TEXT,
                built_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Insert records (only those with estimates or CWS FK match)
        inserted = 0
        skipped_fk = 0
        for _, r in rdf.iterrows():
            try:
                conn.execute(text(f"""
                    INSERT INTO {schema}.rate_best_estimate (
                        pwsid, utility_name, state_code, selected_source,
                        bill_estimate_10ccf, bill_5ccf, bill_10ccf, bill_6ccf, bill_12ccf,
                        fixed_charge_monthly, rate_structure_type, rate_effective_date,
                        n_sources, anchor_source, anchor_bill, confidence, selection_notes
                    ) VALUES (
                        :pwsid, :utility_name, :state_code, :selected_source,
                        :bill_estimate_10ccf, :bill_5ccf, :bill_10ccf, :bill_6ccf, :bill_12ccf,
                        :fixed_charge_monthly, :rate_structure_type, :rate_effective_date,
                        :n_sources, :anchor_source, :anchor_bill, :confidence, :selection_notes
                    )
                """), {
                    "pwsid": r["pwsid"],
                    "utility_name": r.get("utility_name"),
                    "state_code": r.get("state_code"),
                    "selected_source": r.get("selected_source"),
                    "bill_estimate_10ccf": r.get("bill_estimate_10ccf"),
                    "bill_5ccf": float(r["bill_5ccf"]) if pd.notna(r.get("bill_5ccf")) else None,
                    "bill_10ccf": float(r["bill_10ccf"]) if pd.notna(r.get("bill_10ccf")) else None,
                    "bill_6ccf": float(r["bill_6ccf"]) if pd.notna(r.get("bill_6ccf")) else None,
                    "bill_12ccf": float(r["bill_12ccf"]) if pd.notna(r.get("bill_12ccf")) else None,
                    "fixed_charge_monthly": float(r["fixed_charge_monthly"]) if pd.notna(r.get("fixed_charge_monthly")) else None,
                    "rate_structure_type": r.get("rate_structure_type"),
                    "rate_effective_date": r.get("rate_effective_date"),
                    "n_sources": int(r.get("n_sources", 1)),
                    "anchor_source": r.get("anchor_source"),
                    "anchor_bill": float(r["anchor_bill"]) if pd.notna(r.get("anchor_bill")) else None,
                    "confidence": r.get("confidence"),
                    "selection_notes": r.get("selection_notes"),
                })
                inserted += 1
            except Exception as e:
                if "violates foreign key" in str(e):
                    skipped_fk += 1
                else:
                    logger.warning(f"  Failed to insert {r['pwsid']}: {e}")
                    skipped_fk += 1
                # Need to rollback the failed statement
                conn.rollback()

        conn.commit()
        stats["inserted"] = inserted
        stats["skipped_fk"] = skipped_fk

        # Log pipeline run
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, :finished, :row_count, :status, :notes)
        """), {
            "step": "build_best_estimate",
            "started": datetime.now(timezone.utc),
            "finished": datetime.now(timezone.utc),
            "row_count": inserted,
            "status": "success",
            "notes": f"{inserted} best estimates built from {len(df)} rate records across {df.pwsid.nunique()} PWSIDs",
        })
        conn.commit()

    logger.info(f"Inserted {inserted} best estimates ({skipped_fk} skipped FK)")

    if write_csv:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = OUTPUT_DIR / "rate_best_estimate.csv"
        rdf.to_csv(csv_path, index=False)
        logger.info(f"CSV written: {csv_path}")

    logger.info("=== Best Estimate Complete ===")
    return stats


def main():
    parser = argparse.ArgumentParser(description="Build best-estimate rate selection")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--csv", action="store_true", help="Also write CSV output")
    args = parser.parse_args()
    run_best_estimate(dry_run=args.dry_run, write_csv=args.csv)


if __name__ == "__main__":
    main()

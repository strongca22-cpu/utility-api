#!/usr/bin/env python3
"""
Cross-Year eAR Rate Change Analysis

Purpose:
    Compares CA utility water rates across eAR filing years (2020-2022)
    to identify rate change trends, outliers, and data quality issues.

    Uses fixed charges (all 3 years) and pre-computed bills (2021-2022 only;
    2020 lacks bill columns). Filters out NULLed/inflated records.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru

Usage:
    python scripts/analyze_ear_rate_changes.py              # Full analysis
    python scripts/analyze_ear_rate_changes.py --csv         # Also write CSV

Notes:
    - eAR 2020 has no pre-computed bill columns (WR6/9/12/24HCFDWCharges)
    - Records with NULLed bills (from tier inflation fix) are excluded
    - Outliers (>100% change) are likely eAR reporting corrections, not rate changes
    - Mean is heavily skewed by outliers; median is the better summary statistic

Data Sources:
    - Input: utility.water_rates (swrcb_ear_2020/2021/2022)
    - Output: stdout report + optional CSV
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

OUTPUT_DIR = PROJECT_ROOT / "data" / "interim"

# Outlier threshold — changes above this are flagged as likely data quality, not rate changes
OUTLIER_THRESHOLD_PCT = 50.0


def run_analysis(write_csv: bool = False) -> dict:
    """Run cross-year eAR rate change analysis."""
    logger.info("=== Cross-Year eAR Rate Change Analysis ===")

    schema = settings.utility_schema
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT wr.pwsid, wr.source, wr.utility_name,
                   wr.fixed_charge_monthly, wr.bill_6ccf, wr.bill_9ccf,
                   wr.bill_12ccf, wr.bill_24ccf, wr.rate_structure_type,
                   m.population
            FROM {schema}.water_rates wr
            LEFT JOIN {schema}.mdwd_financials m ON m.pwsid = wr.pwsid
            WHERE wr.source LIKE 'swrcb_ear_%'
            ORDER BY wr.pwsid, wr.source
        """), conn)

    logger.info(f"Total eAR records: {len(df)}")

    # --- Bill comparison: 2021 vs 2022 ---
    logger.info("\n--- Bill @12CCF: 2021 → 2022 ---")
    b21 = df[df.source == "swrcb_ear_2021"][["pwsid", "utility_name", "bill_12ccf", "population"]].rename(
        columns={"bill_12ccf": "bill_2021"})
    b22 = df[df.source == "swrcb_ear_2022"][["pwsid", "bill_12ccf"]].rename(
        columns={"bill_12ccf": "bill_2022"})

    bills = b21.merge(b22, on="pwsid")
    bills = bills[bills.bill_2021.notna() & bills.bill_2022.notna() &
                  (bills.bill_2021 > 2) & (bills.bill_2022 > 2)].copy()

    bills["change_pct"] = (bills.bill_2022 - bills.bill_2021) / bills.bill_2021 * 100
    bills["change_abs"] = bills.bill_2022 - bills.bill_2021

    # Separate clean from outliers
    clean = bills[bills.change_pct.abs() <= OUTLIER_THRESHOLD_PCT]
    outliers = bills[bills.change_pct.abs() > OUTLIER_THRESHOLD_PCT]

    logger.info(f"Utilities with clean bill data in both years: {len(bills)}")
    logger.info(f"  Clean (change ≤{OUTLIER_THRESHOLD_PCT}%): {len(clean)}")
    logger.info(f"  Outliers (change >{OUTLIER_THRESHOLD_PCT}%): {len(outliers)} (likely data corrections)")
    print()

    logger.info("Clean bill changes (excluding outliers):")
    logger.info(f"  Mean: {clean.change_pct.mean():+.1f}%")
    logger.info(f"  Median: {clean.change_pct.median():+.1f}%")
    logger.info(f"  Std dev: {clean.change_pct.std():.1f}%")
    logger.info(f"  Mean absolute: ${clean.change_abs.mean():+.2f}/mo")
    logger.info(f"  Median absolute: ${clean.change_abs.median():+.2f}/mo")
    print()

    # Distribution
    bins = [-100, -10, -2, 2, 5, 10, 20, 50]
    labels = ["<-10% decline", "-10 to -2%", "flat (±2%)", "+2 to +5%", "+5 to +10%", "+10 to +20%", "+20 to +50%"]
    clean["change_cat"] = pd.cut(clean.change_pct, bins=bins, labels=labels)
    logger.info("Distribution (clean utilities):")
    for cat in labels:
        count = len(clean[clean.change_cat == cat])
        if count > 0:
            pct = count / len(clean) * 100
            logger.info(f"  {cat}: {count} ({pct:.0f}%)")
    print()

    # --- Fixed charge: 2020 → 2022 ---
    logger.info("--- Fixed Charge: 2020 → 2022 ---")
    f20 = df[df.source == "swrcb_ear_2020"][["pwsid", "fixed_charge_monthly"]].rename(columns={"fixed_charge_monthly": "fixed_2020"})
    f21 = df[df.source == "swrcb_ear_2021"][["pwsid", "fixed_charge_monthly"]].rename(columns={"fixed_charge_monthly": "fixed_2021"})
    f22 = df[df.source == "swrcb_ear_2022"][["pwsid", "fixed_charge_monthly"]].rename(columns={"fixed_charge_monthly": "fixed_2022"})
    fixed = f20.merge(f21, on="pwsid").merge(f22, on="pwsid")
    fixed = fixed[fixed.fixed_2020.notna() & fixed.fixed_2022.notna() &
                  (fixed.fixed_2020 > 0) & (fixed.fixed_2022 > 0) &
                  (fixed.fixed_2020 < 200) & (fixed.fixed_2022 < 200)].copy()

    fixed["change_pct_20_22"] = (fixed.fixed_2022 - fixed.fixed_2020) / fixed.fixed_2020 * 100
    fixed_clean = fixed[fixed.change_pct_20_22.abs() <= OUTLIER_THRESHOLD_PCT]

    logger.info(f"Utilities with clean fixed charge in all 3 years: {len(fixed_clean)}")
    logger.info(f"  Mean 2020→2022 change: {fixed_clean.change_pct_20_22.mean():+.1f}%")
    logger.info(f"  Median 2020→2022 change: {fixed_clean.change_pct_20_22.median():+.1f}%")
    logger.info(f"  Annualized median: {fixed_clean.change_pct_20_22.median() / 2:+.1f}%/yr")
    print()

    # --- Summary statistics ---
    logger.info("--- Summary ---")
    logger.info(f"Annual bill increase (median, 2021→2022): {clean.change_pct.median():+.1f}%")
    logger.info(f"Annual fixed charge increase (median, 2020→2022): {fixed_clean.change_pct_20_22.median() / 2:+.1f}%/yr")
    logger.info(f"Proportion flat (±2% bill change): {len(clean[clean.change_cat == 'flat (±2%)']) / len(clean) * 100:.0f}%")
    logger.info(f"Proportion increasing (>2% bill change): {len(clean[clean.change_pct > 2]) / len(clean) * 100:.0f}%")

    # Outlier details
    if len(outliers) > 0:
        print()
        logger.info(f"--- Outliers (likely data corrections, n={len(outliers)}) ---")
        for _, r in outliers.sort_values("change_pct", ascending=False).head(10).iterrows():
            logger.info(
                f"  {r.pwsid} {r.utility_name[:30]:30s} "
                f"2021=${r.bill_2021:.2f} → 2022=${r.bill_2022:.2f} ({r.change_pct:+.0f}%)"
            )

    # Write CSV
    if write_csv:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = OUTPUT_DIR / "ear_rate_changes_2021_2022.csv"
        bills.to_csv(csv_path, index=False)
        logger.info(f"\nCSV written: {csv_path}")

    stats = {
        "bill_utilities_compared": len(bills),
        "bill_clean": len(clean),
        "bill_outliers": len(outliers),
        "bill_median_change_pct": round(clean.change_pct.median(), 1),
        "bill_mean_change_pct": round(clean.change_pct.mean(), 1),
        "fixed_utilities_compared": len(fixed_clean),
        "fixed_median_change_pct_2yr": round(fixed_clean.change_pct_20_22.median(), 1),
    }
    return stats


def main():
    parser = argparse.ArgumentParser(description="Cross-year eAR rate change analysis")
    parser.add_argument("--csv", action="store_true", help="Write CSV output")
    args = parser.parse_args()
    stats = run_analysis(write_csv=args.csv)
    print()
    logger.info("=== Stats ===")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")


if __name__ == "__main__":
    main()

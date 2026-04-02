#!/usr/bin/env python3
"""
Duke NIEPS QA & Cross-Reference Analysis

Purpose:
    Compare duke_nieps_10state vs scraped_llm records in rate_schedules.
    Produces statistics for Tasks 2-4 of the QA cross-reference analysis.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - pandas, numpy, sqlalchemy

Usage:
    python scripts/duke_qa_analysis.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

# Add project to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from utility_api.config import settings
from utility_api.db import engine

SCHEMA = settings.utility_schema


def load_overlap_data() -> pd.DataFrame:
    """Load PWSIDs that have both scraped_llm and duke_nieps_10state records."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            WITH duke AS (
                SELECT pwsid, bill_5ccf, bill_10ccf, bill_20ccf,
                       rate_structure_type, vintage_date, source_url,
                       confidence, tier_count, billing_frequency,
                       fixed_charges, volumetric_tiers
                FROM {SCHEMA}.rate_schedules
                WHERE source_key = 'duke_nieps_10state'
            ),
            scraped AS (
                SELECT pwsid, bill_5ccf, bill_10ccf, bill_20ccf,
                       rate_structure_type, vintage_date, source_url,
                       confidence, tier_count, billing_frequency,
                       fixed_charges, volumetric_tiers
                FROM {SCHEMA}.rate_schedules
                WHERE source_key = 'scraped_llm'
            )
            SELECT
                d.pwsid,
                SUBSTRING(d.pwsid, 1, 2) AS state_code,
                d.bill_5ccf AS duke_bill_5,
                d.bill_10ccf AS duke_bill_10,
                d.bill_20ccf AS duke_bill_20,
                d.rate_structure_type AS duke_structure,
                d.vintage_date AS duke_vintage,
                d.tier_count AS duke_tiers,
                d.billing_frequency AS duke_freq,
                d.fixed_charges AS duke_fixed,
                d.volumetric_tiers AS duke_vol_tiers,
                s.bill_5ccf AS scraped_bill_5,
                s.bill_10ccf AS scraped_bill_10,
                s.bill_20ccf AS scraped_bill_20,
                s.rate_structure_type AS scraped_structure,
                s.vintage_date AS scraped_vintage,
                s.source_url AS scraped_url,
                s.confidence AS scraped_confidence,
                s.tier_count AS scraped_tiers,
                s.billing_frequency AS scraped_freq,
                s.fixed_charges AS scraped_fixed,
                s.volumetric_tiers AS scraped_vol_tiers
            FROM duke d
            JOIN scraped s ON d.pwsid = s.pwsid
            ORDER BY d.pwsid
        """), conn)
    return df


def load_duke_only() -> pd.DataFrame:
    """Load all Duke records (for Task 4 issue identification)."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT pwsid,
                   SUBSTRING(pwsid, 1, 2) AS state_code,
                   bill_5ccf, bill_10ccf, bill_20ccf,
                   rate_structure_type, vintage_date, tier_count,
                   fixed_charges, volumetric_tiers, billing_frequency
            FROM {SCHEMA}.rate_schedules
            WHERE source_key = 'duke_nieps_10state'
            ORDER BY pwsid
        """), conn)
    return df


def load_source_counts() -> pd.DataFrame:
    """Load overall source coverage stats."""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT source_key,
                   COUNT(*) AS record_count,
                   COUNT(DISTINCT pwsid) AS pwsid_count,
                   COUNT(bill_10ccf) AS has_bill_10,
                   ROUND(AVG(bill_10ccf)::numeric, 2) AS avg_bill_10,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bill_10ccf)::numeric, 2) AS median_bill_10,
                   MIN(vintage_date) AS min_vintage,
                   MAX(vintage_date) AS max_vintage
            FROM {SCHEMA}.rate_schedules
            WHERE source_key IN ('duke_nieps_10state', 'scraped_llm')
            GROUP BY source_key
        """), conn)
    return df


def task2_comparison(df: pd.DataFrame) -> str:
    """Task 2: Head-to-head comparison."""
    lines = []
    lines.append("=" * 80)
    lines.append("TASK 2: HEAD-TO-HEAD COMPARISON")
    lines.append("=" * 80)

    # Filter to rows where both have bill_10ccf
    both = df.dropna(subset=["duke_bill_10", "scraped_bill_10"])
    both = both[(both["duke_bill_10"] > 0) & (both["scraped_bill_10"] > 0)].copy()

    lines.append(f"\nOverlap PWSIDs (total): {len(df)}")
    lines.append(f"Both have bill_10ccf > 0: {len(both)}")

    # Calculate % difference (scraped relative to duke)
    both["pct_diff"] = ((both["scraped_bill_10"] - both["duke_bill_10"]) / both["duke_bill_10"] * 100)
    both["abs_pct_diff"] = both["pct_diff"].abs()

    lines.append(f"\n--- % Difference Distribution (scraped vs duke) ---")
    lines.append(f"  Median:  {both['pct_diff'].median():+.1f}%")
    lines.append(f"  Mean:    {both['pct_diff'].mean():+.1f}%")
    lines.append(f"  P25:     {both['pct_diff'].quantile(0.25):+.1f}%")
    lines.append(f"  P75:     {both['pct_diff'].quantile(0.75):+.1f}%")
    lines.append(f"  Std Dev: {both['pct_diff'].std():.1f}%")

    lines.append(f"\n--- Absolute % Difference Distribution ---")
    lines.append(f"  Median:  {both['abs_pct_diff'].median():.1f}%")
    lines.append(f"  P25:     {both['abs_pct_diff'].quantile(0.25):.1f}%")
    lines.append(f"  P75:     {both['abs_pct_diff'].quantile(0.75):.1f}%")
    lines.append(f"  P90:     {both['abs_pct_diff'].quantile(0.90):.1f}%")
    lines.append(f"  P95:     {both['abs_pct_diff'].quantile(0.95):.1f}%")

    # Buckets
    lines.append(f"\n--- Agreement Buckets ---")
    buckets = [
        ("<10%", both["abs_pct_diff"] < 10),
        ("10-25%", (both["abs_pct_diff"] >= 10) & (both["abs_pct_diff"] < 25)),
        ("25-50%", (both["abs_pct_diff"] >= 25) & (both["abs_pct_diff"] < 50)),
        ("50-100%", (both["abs_pct_diff"] >= 50) & (both["abs_pct_diff"] < 100)),
        (">100%", both["abs_pct_diff"] >= 100),
    ]
    for label, mask in buckets:
        n = mask.sum()
        pct = n / len(both) * 100
        lines.append(f"  {label:10s}: {n:5d} ({pct:5.1f}%)")

    # Direction of disagreement for >50% diff
    big_diff = both[both["abs_pct_diff"] >= 50]
    if len(big_diff) > 0:
        scraped_higher = (big_diff["pct_diff"] > 0).sum()
        scraped_lower = (big_diff["pct_diff"] < 0).sum()
        lines.append(f"\n--- Direction for >50% disagreements (n={len(big_diff)}) ---")
        lines.append(f"  Scraped HIGHER than Duke: {scraped_higher} ({scraped_higher/len(big_diff)*100:.0f}%)")
        lines.append(f"  Scraped LOWER than Duke:  {scraped_lower} ({scraped_lower/len(big_diff)*100:.0f}%)")

    # State cross-tab
    lines.append(f"\n--- State-by-State Agreement ---")
    lines.append(f"{'State':>6s} {'N':>5s} {'Med%Diff':>9s} {'<10%':>6s} {'10-25%':>7s} {'25-50%':>7s} {'>50%':>6s} {'AvgDuke':>9s} {'AvgScr':>9s}")
    for state, sdf in both.groupby("state_code"):
        n = len(sdf)
        med = sdf["pct_diff"].median()
        lt10 = (sdf["abs_pct_diff"] < 10).sum()
        b1025 = ((sdf["abs_pct_diff"] >= 10) & (sdf["abs_pct_diff"] < 25)).sum()
        b2550 = ((sdf["abs_pct_diff"] >= 25) & (sdf["abs_pct_diff"] < 50)).sum()
        gt50 = (sdf["abs_pct_diff"] >= 50).sum()
        avg_d = sdf["duke_bill_10"].mean()
        avg_s = sdf["scraped_bill_10"].mean()
        lines.append(f"{state:>6s} {n:5d} {med:+8.1f}% {lt10:5d}  {b1025:5d}   {b2550:5d}   {gt50:5d}  ${avg_d:7.2f} ${avg_s:7.2f}")

    # Rate structure cross-tab
    lines.append(f"\n--- By Scraped Rate Structure Type ---")
    lines.append(f"{'Structure':>20s} {'N':>5s} {'Med%Diff':>9s} {'<10%':>6s} {'>50%':>6s}")
    for struct, sdf in both.groupby("scraped_structure"):
        n = len(sdf)
        if n < 3:
            continue
        med = sdf["pct_diff"].median()
        lt10 = (sdf["abs_pct_diff"] < 10).sum()
        gt50 = (sdf["abs_pct_diff"] >= 50).sum()
        lines.append(f"{str(struct):>20s} {n:5d} {med:+8.1f}% {lt10:5d}  {gt50:5d}")

    # Vintage gap analysis
    lines.append(f"\n--- Vintage Gap Analysis ---")
    vintage_both = both.dropna(subset=["duke_vintage", "scraped_vintage"])
    if len(vintage_both) > 0:
        vintage_both = vintage_both.copy()
        vintage_both["duke_year"] = pd.to_datetime(vintage_both["duke_vintage"]).dt.year
        vintage_both["scraped_year"] = pd.to_datetime(vintage_both["scraped_vintage"]).dt.year
        vintage_both["year_gap"] = vintage_both["scraped_year"] - vintage_both["duke_year"]
        lines.append(f"  Records with both vintages: {len(vintage_both)}")
        lines.append(f"  Median year gap (scraped - duke): {vintage_both['year_gap'].median():.0f} years")
        lines.append(f"  Mean year gap: {vintage_both['year_gap'].mean():.1f} years")

        # Correlation between year gap and bill difference
        lines.append(f"\n  Abs % diff by year gap:")
        for gap, gdf in vintage_both.groupby("year_gap"):
            if len(gdf) >= 5:
                lines.append(f"    Gap={int(gap):+d}yr: n={len(gdf):4d}, median abs %diff={gdf['abs_pct_diff'].median():.1f}%")
    else:
        lines.append("  (insufficient vintage data for gap analysis)")

    return "\n".join(lines), both


def task3_scraped_errors(df: pd.DataFrame, both: pd.DataFrame) -> str:
    """Task 3: Identify scraped data errors."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("TASK 3: SCRAPED DATA ERRORS (Duke as benchmark)")
    lines.append("=" * 80)

    both_valid = both.copy()

    # Category 1: 5x+ higher or lower
    cat1 = both_valid[both_valid["abs_pct_diff"] >= 400].copy()  # 5x = 400% diff
    lines.append(f"\n--- Cat 1: Bill 5x+ different (probable unit error) ---")
    lines.append(f"  Count: {len(cat1)}")
    if len(cat1) > 0:
        scraped_higher = cat1[cat1["pct_diff"] > 0]
        scraped_lower = cat1[cat1["pct_diff"] < 0]
        lines.append(f"  Scraped 5x+ HIGHER: {len(scraped_higher)}")
        lines.append(f"  Scraped 5x+ LOWER: {len(scraped_lower)}")
        lines.append(f"\n  Examples (top 10 by abs diff):")
        for _, r in cat1.nlargest(10, "abs_pct_diff").iterrows():
            lines.append(f"    {r['pwsid']} | scraped=${r['scraped_bill_10']:.2f} duke=${r['duke_bill_10']:.2f} | diff={r['pct_diff']:+.0f}% | {r.get('scraped_url', 'N/A')}")

    # Category 2: scraped bill = $0 or NULL where Duke has reasonable value
    all_overlap = df.copy()
    cat2_null = all_overlap[
        (all_overlap["scraped_bill_10"].isna() | (all_overlap["scraped_bill_10"] == 0)) &
        (all_overlap["duke_bill_10"].notna()) & (all_overlap["duke_bill_10"] > 5)
    ]
    lines.append(f"\n--- Cat 2: Scraped bill_10ccf = $0/NULL, Duke has value ---")
    lines.append(f"  Count: {len(cat2_null)}")
    if len(cat2_null) > 0:
        lines.append(f"  Duke bill range for these: ${cat2_null['duke_bill_10'].min():.2f}–${cat2_null['duke_bill_10'].max():.2f}")
        lines.append(f"  Examples (top 5):")
        for _, r in cat2_null.head(5).iterrows():
            lines.append(f"    {r['pwsid']} | scraped=${r['scraped_bill_10'] if pd.notna(r['scraped_bill_10']) else 'NULL'} duke=${r['duke_bill_10']:.2f} | {r.get('scraped_url', 'N/A')}")

    # Category 3: scraped > $500 where duke < $100
    cat3 = both_valid[
        (both_valid["scraped_bill_10"] > 500) & (both_valid["duke_bill_10"] < 100)
    ]
    lines.append(f"\n--- Cat 3: Scraped > $500, Duke < $100 (extreme outlier) ---")
    lines.append(f"  Count: {len(cat3)}")
    if len(cat3) > 0:
        lines.append(f"  Examples:")
        for _, r in cat3.head(10).iterrows():
            lines.append(f"    {r['pwsid']} | scraped=${r['scraped_bill_10']:.2f} duke=${r['duke_bill_10']:.2f} | {r.get('scraped_url', 'N/A')}")

    # Category 4: Rate structure misclassification
    # scraped says flat/uniform but duke bill varies significantly across 5/10/20 CCF
    cat4_candidates = both_valid[
        both_valid["scraped_structure"].isin(["flat", "uniform"])
    ].copy()
    if len(cat4_candidates) > 0:
        # Check if duke bills vary across volume (indicating tiered)
        duke_varies = cat4_candidates.dropna(subset=["duke_bill_5", "duke_bill_10", "duke_bill_20"])
        if len(duke_varies) > 0:
            duke_varies = duke_varies.copy()
            duke_varies["duke_vol_range"] = duke_varies["duke_bill_20"] - duke_varies["duke_bill_5"]
            duke_varies["duke_vol_ratio"] = duke_varies["duke_bill_20"] / duke_varies["duke_bill_5"].clip(lower=1)
            misclass = duke_varies[duke_varies["duke_vol_ratio"] > 2.5]  # Duke shows significant tiering
            lines.append(f"\n--- Cat 4: Scraped=flat/uniform but Duke shows tiered billing ---")
            lines.append(f"  Scraped flat/uniform records: {len(cat4_candidates)}")
            lines.append(f"  Duke shows significant volume variation: {len(misclass)}")
            if len(misclass) > 0:
                lines.append(f"  Examples:")
                for _, r in misclass.head(5).iterrows():
                    lines.append(f"    {r['pwsid']} | scraped_struct={r['scraped_structure']} | duke 5/10/20ccf: ${r['duke_bill_5']:.2f}/${r['duke_bill_10']:.2f}/${r['duke_bill_20']:.2f}")

    # Category 5: Scraped bill_10ccf > $200 (general high outliers)
    cat5 = both_valid[both_valid["scraped_bill_10"] > 200].copy()
    lines.append(f"\n--- Cat 5: Scraped bill_10ccf > $200 ---")
    lines.append(f"  Count: {len(cat5)}")
    if len(cat5) > 0:
        cat5_agree = cat5[cat5["abs_pct_diff"] < 25]
        cat5_disagree = cat5[cat5["abs_pct_diff"] >= 50]
        lines.append(f"  Agree with Duke (<25% diff): {len(cat5_agree)}")
        lines.append(f"  Disagree with Duke (>50% diff): {len(cat5_disagree)}")

    # Summary counts by category
    lines.append(f"\n--- Scraped Error Summary ---")
    lines.append(f"  5x+ different:           {len(cat1)}")
    lines.append(f"  $0/NULL extraction fail:  {len(cat2_null)}")
    lines.append(f"  Extreme outlier (>$500):  {len(cat3)}")

    return "\n".join(lines)


def task4_duke_issues(df: pd.DataFrame, duke_all: pd.DataFrame, both: pd.DataFrame) -> str:
    """Task 4: Identify Duke data issues."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("TASK 4: DUKE DATA ISSUES")
    lines.append("=" * 80)

    # Issue 1: Duke bills that are $0, NULL, or negative
    duke_zero = duke_all[
        (duke_all["bill_10ccf"].isna()) | (duke_all["bill_10ccf"] <= 0)
    ]
    lines.append(f"\n--- Issue 1: Duke bill_10ccf = $0/NULL/negative ---")
    lines.append(f"  Total Duke records: {len(duke_all)}")
    lines.append(f"  $0/NULL/negative: {len(duke_zero)} ({len(duke_zero)/len(duke_all)*100:.1f}%)")
    if len(duke_zero) > 0:
        by_state = duke_zero.groupby("state_code").size()
        lines.append(f"  By state:")
        for state, cnt in by_state.items():
            lines.append(f"    {state}: {cnt}")

    # Issue 2: Duke bills identical across 5/10/20 CCF for non-flat
    duke_nonflat = duke_all[~duke_all["rate_structure_type"].isin(["flat", "uniform"])].copy()
    duke_nonflat_valid = duke_nonflat.dropna(subset=["bill_5ccf", "bill_10ccf", "bill_20ccf"])
    if len(duke_nonflat_valid) > 0:
        identical = duke_nonflat_valid[
            (duke_nonflat_valid["bill_5ccf"] == duke_nonflat_valid["bill_10ccf"]) &
            (duke_nonflat_valid["bill_10ccf"] == duke_nonflat_valid["bill_20ccf"])
        ]
        lines.append(f"\n--- Issue 2: Duke non-flat but identical bills across volumes ---")
        lines.append(f"  Non-flat Duke records with all 3 bills: {len(duke_nonflat_valid)}")
        lines.append(f"  Identical 5/10/20 CCF bills: {len(identical)}")
        if len(identical) > 0:
            lines.append(f"  Examples:")
            for _, r in identical.head(5).iterrows():
                lines.append(f"    {r['pwsid']} | struct={r['rate_structure_type']} | 5/10/20: ${r['bill_5ccf']:.2f}/${r['bill_10ccf']:.2f}/${r['bill_20ccf']:.2f}")

    # Issue 3: States where Duke systematically over/understates
    lines.append(f"\n--- Issue 3: Systematic state-level bias ---")
    lines.append(f"{'State':>6s} {'N':>5s} {'MedDiff':>9s} {'Direction':>12s} {'Systematic?':>12s}")
    for state, sdf in both.groupby("state_code"):
        n = len(sdf)
        if n < 5:
            continue
        med = sdf["pct_diff"].median()
        pct_positive = (sdf["pct_diff"] > 0).sum() / n * 100
        direction = "scraped higher" if med > 0 else "duke higher"
        systematic = "YES" if abs(med) > 20 and (pct_positive > 70 or pct_positive < 30) else "no"
        lines.append(f"{state:>6s} {n:5d} {med:+8.1f}% {direction:>12s} {systematic:>12s}")

    # Issue 4: Stale Duke data (>5 years old)
    duke_with_vintage = duke_all.dropna(subset=["vintage_date"]).copy()
    if len(duke_with_vintage) > 0:
        duke_with_vintage["vintage_year"] = pd.to_datetime(duke_with_vintage["vintage_date"]).dt.year
        stale = duke_with_vintage[duke_with_vintage["vintage_year"] < 2021]
        lines.append(f"\n--- Issue 4: Stale Duke data (vintage < 2021) ---")
        lines.append(f"  Duke records with vintage: {len(duke_with_vintage)}")
        lines.append(f"  Vintage < 2021: {len(stale)} ({len(stale)/len(duke_with_vintage)*100:.1f}%)")
        if len(stale) > 0:
            lines.append(f"  Vintage year distribution:")
            for yr, cnt in stale.groupby("vintage_year").size().items():
                lines.append(f"    {int(yr)}: {cnt}")

    # Issue 5: Duke vintage distribution overall
    lines.append(f"\n--- Duke Vintage Distribution ---")
    if len(duke_with_vintage) > 0:
        for yr, cnt in duke_with_vintage.groupby("vintage_year").size().items():
            pct = cnt / len(duke_with_vintage) * 100
            lines.append(f"    {int(yr)}: {cnt:5d} ({pct:.1f}%)")

    return "\n".join(lines)


def main():
    print("Loading data...")
    source_counts = load_source_counts()
    print("\n=== SOURCE OVERVIEW ===")
    print(source_counts.to_string(index=False))

    df = load_overlap_data()
    duke_all = load_duke_only()

    print(f"\nOverlap PWSIDs: {len(df)}")
    print(f"Total Duke records: {len(duke_all)}")

    # Task 2
    t2_text, both = task2_comparison(df)
    print(t2_text)

    # Task 3
    t3_text = task3_scraped_errors(df, both)
    print(t3_text)

    # Task 4
    t4_text = task4_duke_issues(df, duke_all, both)
    print(t4_text)

    # Write full output to file for reference
    output_path = Path(__file__).parents[1] / "data" / "interim" / "duke_qa_analysis_output.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("=== SOURCE OVERVIEW ===\n")
        f.write(source_counts.to_string(index=False) + "\n\n")
        f.write(t2_text + "\n")
        f.write(t3_text + "\n")
        f.write(t4_text + "\n")
    print(f"\nFull output saved to: {output_path}")


if __name__ == "__main__":
    main()

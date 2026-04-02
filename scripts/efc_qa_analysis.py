#!/usr/bin/env python3
"""
EFC Bulk Source QA & Cross-Reference Analysis

Purpose:
    Compare EFC state survey records vs scraped_llm records in rate_schedules.
    Generalized for any EFC source_key(s). Produces statistics for:
      - Task 2: Head-to-head bill comparison
      - Task 3: JSONB storage format audit
      - Task 4: EFC-side data issues
    Adapted from duke_qa_analysis.py template (Sprint 28).

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - pandas, numpy, sqlalchemy

Usage:
    # All 4 pilot states:
    python scripts/efc_qa_analysis.py

    # Specific source(s):
    python scripts/efc_qa_analysis.py --source-keys efc_ar_2020 efc_ia_2023

    # All EFC sources:
    python scripts/efc_qa_analysis.py --all-efc

Notes:
    - Overlap with scraped_llm is currently small (~29 across pilot states)
    - Results are indicative, not statistically conclusive at low N
    - JSONB format audit runs on ALL EFC records (not just overlap)
    - Output saved to data/interim/efc_qa_analysis_output.txt
"""

import argparse
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

# Default pilot states (4 largest EFC sources)
PILOT_SOURCES = ["efc_ar_2020", "efc_ia_2023", "efc_wi_2016", "efc_ga_2019"]


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="EFC QA & cross-reference analysis")
    parser.add_argument(
        "--source-keys", nargs="+", default=None,
        help="Specific EFC source_key(s) to analyze. Default: pilot 4 states.",
    )
    parser.add_argument(
        "--all-efc", action="store_true",
        help="Analyze all EFC sources (source_key LIKE 'efc_%%').",
    )
    return parser.parse_args()


def resolve_source_keys(args) -> list[str]:
    """Determine which EFC source_keys to analyze."""
    if args.all_efc:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT DISTINCT source_key
                FROM {SCHEMA}.rate_schedules
                WHERE source_key LIKE 'efc_%%'
                ORDER BY source_key
            """))
            keys = [r[0] for r in result]
        print(f"Found {len(keys)} EFC sources in database")
        return keys
    elif args.source_keys:
        return args.source_keys
    else:
        return PILOT_SOURCES


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_overlap_data(source_keys: list[str]) -> pd.DataFrame:
    """Load PWSIDs that have both an EFC record and a scraped_llm record."""
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            WITH efc AS (
                SELECT pwsid, source_key AS efc_source,
                       bill_5ccf, bill_10ccf, bill_20ccf,
                       rate_structure_type, vintage_date,
                       confidence, tier_count, billing_frequency,
                       fixed_charges, volumetric_tiers
                FROM {SCHEMA}.rate_schedules
                WHERE source_key IN ({sk_list})
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
                e.pwsid,
                SUBSTRING(e.pwsid, 1, 2) AS state_code,
                e.efc_source,
                e.bill_5ccf        AS efc_bill_5,
                e.bill_10ccf       AS efc_bill_10,
                e.bill_20ccf       AS efc_bill_20,
                e.rate_structure_type AS efc_structure,
                e.vintage_date     AS efc_vintage,
                e.tier_count       AS efc_tiers,
                e.billing_frequency AS efc_freq,
                e.fixed_charges    AS efc_fixed,
                e.volumetric_tiers AS efc_vol_tiers,
                s.bill_5ccf        AS scraped_bill_5,
                s.bill_10ccf       AS scraped_bill_10,
                s.bill_20ccf       AS scraped_bill_20,
                s.rate_structure_type AS scraped_structure,
                s.vintage_date     AS scraped_vintage,
                s.source_url       AS scraped_url,
                s.confidence       AS scraped_confidence,
                s.tier_count       AS scraped_tiers,
                s.billing_frequency AS scraped_freq,
                s.fixed_charges    AS scraped_fixed,
                s.volumetric_tiers AS scraped_vol_tiers
            FROM efc e
            JOIN scraped s ON e.pwsid = s.pwsid
            ORDER BY e.pwsid
        """), conn)
    return df


def load_efc_all(source_keys: list[str]) -> pd.DataFrame:
    """Load all EFC records for issue identification and JSONB audit."""
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT id, pwsid, source_key,
                   SUBSTRING(pwsid, 1, 2) AS state_code,
                   bill_5ccf, bill_10ccf, bill_20ccf,
                   rate_structure_type, vintage_date, tier_count,
                   fixed_charges, volumetric_tiers, billing_frequency,
                   confidence, conservation_signal
            FROM {SCHEMA}.rate_schedules
            WHERE source_key IN ({sk_list})
            ORDER BY pwsid
        """), conn)
    return df


def load_source_counts(source_keys: list[str]) -> pd.DataFrame:
    """Load overall source coverage stats."""
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT source_key,
                   COUNT(*) AS record_count,
                   COUNT(DISTINCT pwsid) AS pwsid_count,
                   COUNT(bill_10ccf) AS has_bill_10,
                   ROUND(AVG(bill_10ccf)::numeric, 2) AS avg_bill_10,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                         (ORDER BY bill_10ccf)::numeric, 2) AS median_bill_10,
                   MIN(vintage_date) AS min_vintage,
                   MAX(vintage_date) AS max_vintage
            FROM {SCHEMA}.rate_schedules
            WHERE source_key IN ({sk_list})
               OR source_key = 'scraped_llm'
            GROUP BY source_key
            ORDER BY source_key
        """), conn)
    return df


# ---------------------------------------------------------------------------
# Task 2: Head-to-head comparison
# ---------------------------------------------------------------------------

def task2_comparison(df: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    """Head-to-head bill comparison: EFC vs scraped_llm."""
    lines = []
    lines.append("=" * 80)
    lines.append("TASK 2: HEAD-TO-HEAD COMPARISON  (EFC vs scraped_llm)")
    lines.append("=" * 80)

    # Filter to rows where both have bill_10ccf
    both = df.dropna(subset=["efc_bill_10", "scraped_bill_10"])
    both = both[(both["efc_bill_10"] > 0) & (both["scraped_bill_10"] > 0)].copy()

    lines.append(f"\nOverlap PWSIDs (total): {len(df)}")
    lines.append(f"Both have bill_10ccf > 0: {len(both)}")

    if len(both) == 0:
        lines.append("\n** No valid bill pairs — cannot compute comparison. **")
        return "\n".join(lines), both

    # % difference (scraped relative to EFC)
    both["pct_diff"] = (
        (both["scraped_bill_10"] - both["efc_bill_10"])
        / both["efc_bill_10"] * 100
    )
    both["abs_pct_diff"] = both["pct_diff"].abs()

    # Small-N warning
    if len(both) < 30:
        lines.append(f"\n** WARNING: N={len(both)} is small. Results are indicative, not conclusive. **")

    lines.append(f"\n--- % Difference Distribution (scraped vs EFC) ---")
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

    # Agreement buckets
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

    # Direction for large disagreements
    big_diff = both[both["abs_pct_diff"] >= 50]
    if len(big_diff) > 0:
        scraped_higher = (big_diff["pct_diff"] > 0).sum()
        scraped_lower = (big_diff["pct_diff"] < 0).sum()
        lines.append(f"\n--- Direction for >50% disagreements (n={len(big_diff)}) ---")
        lines.append(f"  Scraped HIGHER than EFC: {scraped_higher} ({scraped_higher/len(big_diff)*100:.0f}%)")
        lines.append(f"  Scraped LOWER than EFC:  {scraped_lower} ({scraped_lower/len(big_diff)*100:.0f}%)")

    # Per-EFC-source breakdown
    lines.append(f"\n--- By EFC Source ---")
    lines.append(
        f"{'Source':>20s} {'N':>5s} {'Med%Diff':>9s} {'<10%':>6s} "
        f"{'10-25%':>7s} {'25-50%':>7s} {'>50%':>6s} {'AvgEFC':>9s} {'AvgScr':>9s}"
    )
    for src, sdf in both.groupby("efc_source"):
        n = len(sdf)
        med = sdf["pct_diff"].median()
        lt10 = (sdf["abs_pct_diff"] < 10).sum()
        b1025 = ((sdf["abs_pct_diff"] >= 10) & (sdf["abs_pct_diff"] < 25)).sum()
        b2550 = ((sdf["abs_pct_diff"] >= 25) & (sdf["abs_pct_diff"] < 50)).sum()
        gt50 = (sdf["abs_pct_diff"] >= 50).sum()
        avg_e = sdf["efc_bill_10"].mean()
        avg_s = sdf["scraped_bill_10"].mean()
        lines.append(
            f"{src:>20s} {n:5d} {med:+8.1f}% {lt10:5d}  "
            f"{b1025:5d}   {b2550:5d}   {gt50:5d}  ${avg_e:7.2f} ${avg_s:7.2f}"
        )

    # State cross-tab
    lines.append(f"\n--- State-by-State Agreement ---")
    lines.append(
        f"{'State':>6s} {'N':>5s} {'Med%Diff':>9s} {'<10%':>6s} "
        f"{'10-25%':>7s} {'25-50%':>7s} {'>50%':>6s} {'AvgEFC':>9s} {'AvgScr':>9s}"
    )
    for state, sdf in both.groupby("state_code"):
        n = len(sdf)
        med = sdf["pct_diff"].median()
        lt10 = (sdf["abs_pct_diff"] < 10).sum()
        b1025 = ((sdf["abs_pct_diff"] >= 10) & (sdf["abs_pct_diff"] < 25)).sum()
        b2550 = ((sdf["abs_pct_diff"] >= 25) & (sdf["abs_pct_diff"] < 50)).sum()
        gt50 = (sdf["abs_pct_diff"] >= 50).sum()
        avg_e = sdf["efc_bill_10"].mean()
        avg_s = sdf["scraped_bill_10"].mean()
        lines.append(
            f"{state:>6s} {n:5d} {med:+8.1f}% {lt10:5d}  "
            f"{b1025:5d}   {b2550:5d}   {gt50:5d}  ${avg_e:7.2f} ${avg_s:7.2f}"
        )

    # Rate structure cross-tab
    lines.append(f"\n--- By Scraped Rate Structure Type ---")
    lines.append(f"{'Structure':>20s} {'N':>5s} {'Med%Diff':>9s} {'<10%':>6s} {'>50%':>6s}")
    for struct, sdf in both.groupby("scraped_structure"):
        n = len(sdf)
        if n < 2:
            continue
        med = sdf["pct_diff"].median()
        lt10 = (sdf["abs_pct_diff"] < 10).sum()
        gt50 = (sdf["abs_pct_diff"] >= 50).sum()
        lines.append(f"{str(struct):>20s} {n:5d} {med:+8.1f}% {lt10:5d}  {gt50:5d}")

    # Vintage gap analysis
    lines.append(f"\n--- Vintage Gap Analysis ---")
    vintage_both = both.dropna(subset=["efc_vintage", "scraped_vintage"])
    if len(vintage_both) > 0:
        vintage_both = vintage_both.copy()
        vintage_both["efc_year"] = pd.to_datetime(vintage_both["efc_vintage"]).dt.year
        vintage_both["scraped_year"] = pd.to_datetime(vintage_both["scraped_vintage"]).dt.year
        vintage_both["year_gap"] = vintage_both["scraped_year"] - vintage_both["efc_year"]
        lines.append(f"  Records with both vintages: {len(vintage_both)}")
        lines.append(f"  Median year gap (scraped - EFC): {vintage_both['year_gap'].median():.0f} years")
        lines.append(f"  Mean year gap: {vintage_both['year_gap'].mean():.1f} years")

        lines.append(f"\n  Abs % diff by year gap:")
        for gap, gdf in vintage_both.groupby("year_gap"):
            lines.append(
                f"    Gap={int(gap):+d}yr: n={len(gdf):4d}, "
                f"median abs %diff={gdf['abs_pct_diff'].median():.1f}%"
            )
    else:
        lines.append("  (insufficient vintage data for gap analysis)")

    # Detail listing (all pairs, since N is small)
    if len(both) <= 50:
        lines.append(f"\n--- Full Detail Listing (N={len(both)}) ---")
        lines.append(
            f"{'PWSID':>16s} {'EFC_src':>16s} {'EFC$10':>8s} {'Scr$10':>8s} "
            f"{'%Diff':>8s} {'EFC_struct':>18s} {'Scr_struct':>18s} {'EFC_vint':>10s} {'Scr_vint':>10s}"
        )
        for _, r in both.iterrows():
            efc_v = str(r["efc_vintage"])[:10] if pd.notna(r["efc_vintage"]) else "N/A"
            scr_v = str(r["scraped_vintage"])[:10] if pd.notna(r["scraped_vintage"]) else "N/A"
            lines.append(
                f"{r['pwsid']:>16s} {r['efc_source']:>16s} "
                f"${r['efc_bill_10']:7.2f} ${r['scraped_bill_10']:7.2f} "
                f"{r['pct_diff']:+7.1f}% "
                f"{str(r['efc_structure']):>18s} {str(r['scraped_structure']):>18s} "
                f"{efc_v:>10s} {scr_v:>10s}"
            )

    return "\n".join(lines), both


# ---------------------------------------------------------------------------
# Task 3: JSONB storage format audit (all EFC records, not just overlap)
# ---------------------------------------------------------------------------

def task3_jsonb_audit(efc_all: pd.DataFrame) -> str:
    """Audit JSONB storage format comparability with scraped_llm canonical format."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("TASK 3: JSONB STORAGE FORMAT AUDIT")
    lines.append("=" * 80)
    lines.append(f"\nTotal EFC records analyzed: {len(efc_all)}")

    # --- fixed_charges structure ---
    lines.append(f"\n--- fixed_charges Structure ---")
    has_fixed = efc_all[efc_all["fixed_charges"].notna()].copy()
    no_fixed = efc_all[efc_all["fixed_charges"].isna()]
    lines.append(f"  Has fixed_charges: {len(has_fixed)} ({len(has_fixed)/len(efc_all)*100:.1f}%)")
    lines.append(f"  NULL fixed_charges: {len(no_fixed)} ({len(no_fixed)/len(efc_all)*100:.1f}%)")

    # Canonical keys: {name, amount, meter_size}
    # Check for extra or missing keys
    extra_keys_count = 0
    missing_keys_count = 0
    canonical_fc_keys = {"name", "amount", "meter_size"}
    extra_key_examples = {}

    for _, row in has_fixed.iterrows():
        fc_list = row["fixed_charges"]
        if not isinstance(fc_list, list):
            continue
        for fc in fc_list:
            if not isinstance(fc, dict):
                continue
            keys = set(fc.keys())
            extra = keys - canonical_fc_keys
            missing = canonical_fc_keys - keys
            if extra:
                extra_keys_count += 1
                for k in extra:
                    extra_key_examples.setdefault(k, 0)
                    extra_key_examples[k] += 1
            if missing:
                missing_keys_count += 1

    lines.append(f"  Records with extra keys in fixed_charges: {extra_keys_count}")
    if extra_key_examples:
        lines.append(f"  Extra key breakdown:")
        for k, cnt in sorted(extra_key_examples.items(), key=lambda x: -x[1]):
            lines.append(f"    '{k}': {cnt} records")
    lines.append(f"  Records with missing canonical keys: {missing_keys_count}")

    # --- volumetric_tiers structure ---
    lines.append(f"\n--- volumetric_tiers Structure ---")
    has_tiers = efc_all[efc_all["volumetric_tiers"].notna()].copy()
    no_tiers = efc_all[efc_all["volumetric_tiers"].isna()]
    lines.append(f"  Has volumetric_tiers: {len(has_tiers)} ({len(has_tiers)/len(efc_all)*100:.1f}%)")
    lines.append(f"  NULL volumetric_tiers: {len(no_tiers)} ({len(no_tiers)/len(efc_all)*100:.1f}%)")

    # Canonical keys: {tier, min_gal, max_gal, rate_per_1000_gal}
    canonical_vt_keys = {"tier", "min_gal", "max_gal", "rate_per_1000_gal"}
    extra_vt_count = 0
    missing_vt_count = 0
    extra_vt_examples = {}
    contiguity_issues = 0
    duplicate_tier_count = 0
    tier_count_dist = {}

    for _, row in has_tiers.iterrows():
        tier_list = row["volumetric_tiers"]
        if not isinstance(tier_list, list) or len(tier_list) == 0:
            continue

        # Track tier count distribution
        tc = len(tier_list)
        tier_count_dist[tc] = tier_count_dist.get(tc, 0) + 1

        # Check keys
        for t in tier_list:
            if not isinstance(t, dict):
                continue
            keys = set(t.keys())
            extra = keys - canonical_vt_keys
            missing = canonical_vt_keys - keys
            if extra:
                extra_vt_count += 1
                for k in extra:
                    extra_vt_examples.setdefault(k, 0)
                    extra_vt_examples[k] += 1
            if missing:
                missing_vt_count += 1

        # Check contiguity
        sorted_tiers = sorted(tier_list, key=lambda t: t.get("tier", 0))
        for i in range(1, len(sorted_tiers)):
            prev_max = sorted_tiers[i - 1].get("max_gal")
            curr_min = sorted_tiers[i].get("min_gal")
            if prev_max is not None and curr_min is not None and prev_max != curr_min:
                contiguity_issues += 1
                break  # Count per record, not per gap

        # Check duplicate tiers
        seen = set()
        has_dup = False
        for t in tier_list:
            key = (t.get("min_gal"), t.get("max_gal"), t.get("rate_per_1000_gal"))
            if key in seen:
                has_dup = True
                break
            seen.add(key)
        if has_dup:
            duplicate_tier_count += 1

    lines.append(f"  Records with extra keys in volumetric_tiers: {extra_vt_count}")
    if extra_vt_examples:
        lines.append(f"  Extra key breakdown:")
        for k, cnt in sorted(extra_vt_examples.items(), key=lambda x: -x[1]):
            lines.append(f"    '{k}': {cnt} tier entries")
    lines.append(f"  Records with missing canonical keys: {missing_vt_count}")
    lines.append(f"  Records with tier contiguity gaps: {contiguity_issues}")
    lines.append(f"  Records with duplicate tiers: {duplicate_tier_count}")

    lines.append(f"\n--- Tier Count Distribution ---")
    for tc in sorted(tier_count_dist.keys()):
        cnt = tier_count_dist[tc]
        pct = cnt / len(has_tiers) * 100
        lines.append(f"  {tc} tier(s): {cnt:5d} ({pct:5.1f}%)")

    # --- rate_structure_type values ---
    lines.append(f"\n--- rate_structure_type Distribution ---")
    for rst, cnt in efc_all["rate_structure_type"].value_counts().items():
        pct = cnt / len(efc_all) * 100
        lines.append(f"  {str(rst):>20s}: {cnt:5d} ({pct:5.1f}%)")

    # --- billing_frequency distribution ---
    lines.append(f"\n--- billing_frequency Distribution ---")
    for bf, cnt in efc_all["billing_frequency"].value_counts(dropna=False).items():
        pct = cnt / len(efc_all) * 100
        label = str(bf) if pd.notna(bf) else "NULL"
        lines.append(f"  {label:>15s}: {cnt:5d} ({pct:5.1f}%)")

    # --- confidence distribution ---
    lines.append(f"\n--- Confidence Distribution ---")
    for conf, cnt in efc_all["confidence"].value_counts(dropna=False).items():
        pct = cnt / len(efc_all) * 100
        label = str(conf) if pd.notna(conf) else "NULL"
        lines.append(f"  {label:>10s}: {cnt:5d} ({pct:5.1f}%)")

    # Summary of fixable issues
    lines.append(f"\n--- JSONB Fix Summary ---")
    lines.append(f"  Extra keys in fixed_charges (strip):    {extra_keys_count}")
    lines.append(f"  Extra keys in volumetric_tiers (strip): {extra_vt_count}")
    lines.append(f"  Tier contiguity gaps (fix boundaries):  {contiguity_issues}")
    lines.append(f"  Duplicate tiers (deduplicate):          {duplicate_tier_count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Task 4: EFC-side data issues
# ---------------------------------------------------------------------------

def task4_efc_issues(
    df: pd.DataFrame, efc_all: pd.DataFrame, both: pd.DataFrame
) -> str:
    """Identify EFC-side data quality issues."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("TASK 4: EFC DATA ISSUES")
    lines.append("=" * 80)

    # Issue 1: NULL / zero / negative bills
    efc_zero = efc_all[
        (efc_all["bill_10ccf"].isna()) | (efc_all["bill_10ccf"] <= 0)
    ]
    lines.append(f"\n--- Issue 1: EFC bill_10ccf = $0/NULL/negative ---")
    lines.append(f"  Total EFC records: {len(efc_all)}")
    lines.append(f"  $0/NULL/negative: {len(efc_zero)} ({len(efc_zero)/len(efc_all)*100:.1f}%)")
    if len(efc_zero) > 0:
        by_source = efc_zero.groupby("source_key").size()
        lines.append(f"  By source:")
        for src, cnt in by_source.items():
            lines.append(f"    {src}: {cnt}")

    # Issue 2: Non-flat structure but identical bills at 5/10/20 CCF
    efc_nonflat = efc_all[~efc_all["rate_structure_type"].isin(["flat", "uniform"])].copy()
    efc_nonflat_valid = efc_nonflat.dropna(subset=["bill_5ccf", "bill_10ccf", "bill_20ccf"])
    if len(efc_nonflat_valid) > 0:
        identical = efc_nonflat_valid[
            (efc_nonflat_valid["bill_5ccf"] == efc_nonflat_valid["bill_10ccf"])
            & (efc_nonflat_valid["bill_10ccf"] == efc_nonflat_valid["bill_20ccf"])
        ]
        lines.append(f"\n--- Issue 2: Non-flat but identical bills across 5/10/20 CCF ---")
        lines.append(f"  Non-flat EFC records with all 3 bills: {len(efc_nonflat_valid)}")
        lines.append(f"  Identical 5/10/20 CCF: {len(identical)}")
        if len(identical) > 0:
            lines.append(f"  Examples:")
            for _, r in identical.head(5).iterrows():
                lines.append(
                    f"    {r['pwsid']} ({r['source_key']}) | "
                    f"struct={r['rate_structure_type']} | "
                    f"5/10/20: ${r['bill_5ccf']:.2f}/${r['bill_10ccf']:.2f}/${r['bill_20ccf']:.2f}"
                )

    # Issue 3: Bill outliers — unusually high or low
    efc_with_bill = efc_all[efc_all["bill_10ccf"].notna() & (efc_all["bill_10ccf"] > 0)].copy()
    if len(efc_with_bill) > 0:
        very_low = efc_with_bill[efc_with_bill["bill_10ccf"] < 5]
        very_high = efc_with_bill[efc_with_bill["bill_10ccf"] > 500]
        lines.append(f"\n--- Issue 3: EFC bill outliers ---")
        lines.append(f"  EFC records with bill_10ccf > 0: {len(efc_with_bill)}")
        lines.append(f"  Bill < $5 (suspicious low):  {len(very_low)}")
        lines.append(f"  Bill > $500 (suspicious high): {len(very_high)}")
        if len(very_high) > 0:
            lines.append(f"  High outlier examples:")
            for _, r in very_high.nlargest(5, "bill_10ccf").iterrows():
                lines.append(
                    f"    {r['pwsid']} ({r['source_key']}) | "
                    f"bill_10ccf=${r['bill_10ccf']:.2f} | struct={r['rate_structure_type']}"
                )

    # Issue 4: Stale vintage
    efc_with_vintage = efc_all.dropna(subset=["vintage_date"]).copy()
    if len(efc_with_vintage) > 0:
        efc_with_vintage["vintage_year"] = pd.to_datetime(
            efc_with_vintage["vintage_date"]
        ).dt.year
        stale = efc_with_vintage[efc_with_vintage["vintage_year"] < 2018]
        lines.append(f"\n--- Issue 4: Stale EFC vintage (< 2018) ---")
        lines.append(f"  EFC records with vintage: {len(efc_with_vintage)}")
        lines.append(f"  Vintage < 2018: {len(stale)} ({len(stale)/len(efc_with_vintage)*100:.1f}%)")

        lines.append(f"\n  Vintage year distribution:")
        for yr, cnt in efc_with_vintage.groupby("vintage_year").size().items():
            pct = cnt / len(efc_with_vintage) * 100
            lines.append(f"    {int(yr)}: {cnt:5d} ({pct:.1f}%)")
    else:
        lines.append(f"\n--- Issue 4: No vintage data available ---")

    # Issue 5: Systematic bias in overlap (if enough data)
    if len(both) >= 5:
        lines.append(f"\n--- Issue 5: Systematic state-level bias (overlap only) ---")
        lines.append(
            f"{'State':>6s} {'N':>5s} {'MedDiff':>9s} {'Direction':>14s} {'Systematic?':>12s}"
        )
        for state, sdf in both.groupby("state_code"):
            n = len(sdf)
            if n < 2:
                continue
            med = sdf["pct_diff"].median()
            pct_positive = (sdf["pct_diff"] > 0).sum() / n * 100
            direction = "scraped higher" if med > 0 else "EFC higher"
            systematic = (
                "YES" if abs(med) > 20 and (pct_positive > 70 or pct_positive < 30)
                else "no"
            )
            lines.append(
                f"{state:>6s} {n:5d} {med:+8.1f}% {direction:>14s} {systematic:>12s}"
            )

    # Issue 6: Scraped error flagging (EFC as benchmark)
    if len(both) > 0:
        lines.append(f"\n--- Issue 6: Scraped errors flagged by EFC benchmark ---")
        # 5x+ difference
        cat1 = both[both["abs_pct_diff"] >= 400]
        lines.append(f"  Bill 5x+ different: {len(cat1)}")
        if len(cat1) > 0:
            for _, r in cat1.iterrows():
                lines.append(
                    f"    {r['pwsid']} | EFC=${r['efc_bill_10']:.2f} "
                    f"scraped=${r['scraped_bill_10']:.2f} | diff={r['pct_diff']:+.0f}%"
                )

        # Scraped NULL where EFC has value
        cat2 = df[
            (df["scraped_bill_10"].isna() | (df["scraped_bill_10"] == 0))
            & df["efc_bill_10"].notna()
            & (df["efc_bill_10"] > 5)
        ]
        lines.append(f"  Scraped NULL/$0 where EFC has value: {len(cat2)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    source_keys = resolve_source_keys(args)

    print(f"Analyzing EFC sources: {source_keys}")
    print(f"Number of sources: {len(source_keys)}")

    # Source overview
    print("\nLoading source counts...")
    source_counts = load_source_counts(source_keys)
    print("\n=== SOURCE OVERVIEW ===")
    print(source_counts.to_string(index=False))

    # Load data
    print("\nLoading overlap data...")
    df = load_overlap_data(source_keys)
    print(f"Overlap PWSIDs: {len(df)}")

    print("Loading all EFC records...")
    efc_all = load_efc_all(source_keys)
    print(f"Total EFC records: {len(efc_all)}")

    # Task 2: Head-to-head
    t2_text, both = task2_comparison(df)
    print(t2_text)

    # Task 3: JSONB format audit
    t3_text = task3_jsonb_audit(efc_all)
    print(t3_text)

    # Task 4: EFC issues
    t4_text = task4_efc_issues(df, efc_all, both)
    print(t4_text)

    # Write full output
    output_path = (
        Path(__file__).parents[1] / "data" / "interim" / "efc_qa_analysis_output.txt"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(f"EFC QA Analysis — Sources: {source_keys}\n")
        f.write(f"{'=' * 80}\n\n")
        f.write("=== SOURCE OVERVIEW ===\n")
        f.write(source_counts.to_string(index=False) + "\n\n")
        f.write(t2_text + "\n")
        f.write(t3_text + "\n")
        f.write(t4_text + "\n")
    print(f"\nFull output saved to: {output_path}")


if __name__ == "__main__":
    main()

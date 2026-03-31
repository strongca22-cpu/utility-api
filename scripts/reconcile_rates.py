#!/usr/bin/env python3
"""
Rate Reconciliation Diagnostic

Purpose:
    Identifies pseudo-duplicates (same PWSID, different sources), measures
    cross-source variance, categorizes divergence causes, and flags data
    quality issues. Outputs a diagnostic report for human review.

    This is a diagnostic tool — it flags issues but does not resolve them.
    Resolution methodology requires additional context (vintage priority,
    water-only vs combined, data quality thresholds).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru

Usage:
    python scripts/reconcile_rates.py                 # Full diagnostic
    python scripts/reconcile_rates.py --csv            # Also write CSV report
    python scripts/reconcile_rates.py --threshold 25   # Flag CV > 25% (default)

Notes:
    - Uses bill_10ccf (scraped/OWRS) or interpolated 10CCF from eAR bill_9ccf/bill_12ccf
    - eAR tier limit inflation (1000x) is a known systematic issue in the state filing
    - Scraped rates may include combined water+sewer (flagged by eAR comparison)
    - Vintage differences (2013 OWRS vs 2022 eAR vs 2024 scraped) are expected divergence

Data Sources:
    - Input: utility.water_rates (all sources)
    - Output: stdout report + optional CSV at data/interim/rate_reconciliation.csv
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

OUTPUT_DIR = PROJECT_ROOT / "data" / "interim"

# eAR tier limit threshold — limits above this are almost certainly
# in wrong units (gallons vs CCF, or per-billing-period vs monthly)
EAR_TIER_LIMIT_SUSPECT = 500  # CCF — no residential tier should exceed this

# Bill amount thresholds for flagging
BILL_SUSPECT_LOW = 2.0    # $/month — below this is likely data error
BILL_SUSPECT_HIGH = 500.0  # $/month at 10 CCF — above this is likely inflated tiers


def get_comparable_bill(row: pd.Series) -> tuple[float | None, str | None]:
    """Extract a comparable ~10CCF bill amount from any source.

    Parameters
    ----------
    row : pd.Series
        A water_rates record.

    Returns
    -------
    tuple[float | None, str | None]
        (bill_amount, method) — method is '10ccf', 'interp_10', or '12ccf'.
    """
    if pd.notna(row.get("bill_10ccf")) and row["bill_10ccf"] > 0:
        return float(row["bill_10ccf"]), "10ccf"
    if pd.notna(row.get("bill_9ccf")) and pd.notna(row.get("bill_12ccf")):
        b9 = float(row["bill_9ccf"])
        b12 = float(row["bill_12ccf"])
        if b9 > 0 and b12 > 0:
            return round(b9 + (b12 - b9) / 3.0, 2), "interp_10"
    if pd.notna(row.get("bill_12ccf")) and row["bill_12ccf"] > 0:
        return float(row["bill_12ccf"]), "12ccf"
    return None, None


def flag_issues(row: pd.Series) -> list[str]:
    """Flag data quality issues on a single rate record.

    Parameters
    ----------
    row : pd.Series
        A water_rates record.

    Returns
    -------
    list[str]
        List of issue flags (may be empty).
    """
    flags = []
    source = row.get("source", "")

    # eAR tier limit inflation
    if "ear" in source:
        for col in ["tier_1_limit_ccf", "tier_2_limit_ccf", "tier_3_limit_ccf", "tier_4_limit_ccf"]:
            val = row.get(col)
            if pd.notna(val) and float(val) > EAR_TIER_LIMIT_SUSPECT:
                flags.append(f"ear_tier_limit_inflated ({col}={val:.0f})")
                break

    # Suspiciously low bills
    comp_bill, _ = get_comparable_bill(row)
    if comp_bill is not None and comp_bill < BILL_SUSPECT_LOW:
        flags.append(f"bill_suspiciously_low (${comp_bill:.2f})")

    # Suspiciously high bills (likely inflated tier limits or combined charges)
    if comp_bill is not None and comp_bill > BILL_SUSPECT_HIGH:
        flags.append(f"bill_suspiciously_high (${comp_bill:.0f})")

    # Fixed charge anomalies
    fixed = row.get("fixed_charge_monthly")
    if pd.notna(fixed):
        if float(fixed) > 200:
            flags.append(f"fixed_charge_high (${fixed:.0f})")
        elif float(fixed) < 0:
            flags.append("fixed_charge_negative")

    # Stale data (effective date before 2015)
    date = row.get("rate_effective_date")
    if pd.notna(date):
        try:
            d = pd.to_datetime(date)
            if d.year < 2015:
                flags.append(f"stale_vintage ({d.year})")
        except Exception:
            pass

    return flags


def run_reconciliation(cv_threshold: float = 25.0, write_csv: bool = False) -> dict:
    """Run the full reconciliation diagnostic.

    Parameters
    ----------
    cv_threshold : float
        CV% threshold for flagging divergent utilities.
    write_csv : bool
        If True, write detailed CSV report.

    Returns
    -------
    dict
        Summary statistics.
    """
    logger.info("=== Rate Reconciliation Diagnostic ===")

    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT rs.pwsid, rs.source_key AS source, c.pws_name AS utility_name,
                   rs.bill_5ccf, rs.bill_6ccf, rs.bill_9ccf, rs.bill_10ccf, rs.bill_12ccf, rs.bill_24ccf,
                   (rs.fixed_charges->0->>'amount')::float AS fixed_charge_monthly,
                   (rs.volumetric_tiers->0->>'rate_per_1000_gal')::float AS tier_1_rate,
                   (rs.volumetric_tiers->0->>'max_gal')::float AS tier_1_limit_ccf,
                   (rs.volumetric_tiers->1->>'rate_per_1000_gal')::float AS tier_2_rate,
                   (rs.volumetric_tiers->1->>'max_gal')::float AS tier_2_limit_ccf,
                   (rs.volumetric_tiers->2->>'rate_per_1000_gal')::float AS tier_3_rate,
                   (rs.volumetric_tiers->2->>'max_gal')::float AS tier_3_limit_ccf,
                   (rs.volumetric_tiers->3->>'rate_per_1000_gal')::float AS tier_4_rate,
                   (rs.volumetric_tiers->3->>'max_gal')::float AS tier_4_limit_ccf,
                   rs.vintage_date AS rate_effective_date, rs.rate_structure_type,
                   rs.confidence AS parse_confidence
            FROM utility.rate_schedules rs
            LEFT JOIN utility.cws_boundaries c ON c.pwsid = rs.pwsid
            ORDER BY rs.pwsid, rs.source_key
        """), conn)

    logger.info(f"Total rate records: {len(df)}")
    logger.info(f"Unique PWSIDs: {df.pwsid.nunique()}")
    logger.info(f"Sources: {dict(df.source.value_counts())}")
    print()

    # --- 1. Per-record quality flags ---
    logger.info("--- Per-Record Quality Flags ---")
    df["quality_flags"] = df.apply(flag_issues, axis=1)
    df["n_quality_flags"] = df.quality_flags.apply(len)
    flagged = df[df.n_quality_flags > 0]
    logger.info(f"Records with quality flags: {len(flagged)}/{len(df)}")

    # Count flag types
    all_flags = []
    for flags in flagged.quality_flags:
        for f in flags:
            flag_type = f.split(" (")[0]
            all_flags.append(flag_type)
    flag_counts = pd.Series(all_flags).value_counts()
    for flag, count in flag_counts.items():
        logger.info(f"  {flag}: {count}")
    print()

    # --- 2. Cross-source variance ---
    logger.info("--- Cross-Source Variance ---")
    df["comp_bill"], df["comp_method"] = zip(*df.apply(get_comparable_bill, axis=1))

    # Filter to utilities with multiple sources AND comparable bills
    has_comp = df[df.comp_bill.notna()]
    multi = has_comp.groupby("pwsid").filter(lambda g: g.source.nunique() > 1)

    variance_rows = []
    for pwsid, group in multi.groupby("pwsid"):
        values = group.comp_bill.values
        if len(values) < 2:
            continue

        mean_v = values.mean()
        spread = values.max() - values.min()
        cv = values.std() / mean_v * 100 if mean_v > 0 else 0
        sources = sorted(group.source.unique())
        dates = sorted(group.rate_effective_date.dropna().unique())
        any_flagged = any(group.n_quality_flags > 0)

        # Classify the divergence cause
        causes = []
        if any(group.quality_flags.apply(lambda f: any("ear_tier_limit_inflated" in x for x in f))):
            causes.append("eAR_tier_inflation")
        if any(group.quality_flags.apply(lambda f: any("bill_suspiciously_high" in x for x in f))):
            causes.append("possible_combined_charges")
        if any(group.quality_flags.apply(lambda f: any("stale_vintage" in x for x in f))):
            causes.append("stale_vintage")
        if any(group.quality_flags.apply(lambda f: any("bill_suspiciously_low" in x for x in f))):
            causes.append("data_quality_low")
        if len(dates) >= 2:
            try:
                date_range = (pd.to_datetime(dates[-1]) - pd.to_datetime(dates[0])).days / 365.25
                if date_range > 3:
                    causes.append(f"vintage_gap_{date_range:.0f}yr")
            except Exception:
                pass
        if not causes and cv > cv_threshold:
            causes.append("unexplained")

        variance_rows.append({
            "pwsid": pwsid,
            "utility_name": group.utility_name.iloc[0] or "",
            "n_sources": len(sources),
            "sources": ", ".join(sources),
            "n_records": len(group),
            "mean_bill_10ccf": round(mean_v, 2),
            "min_bill": round(values.min(), 2),
            "max_bill": round(values.max(), 2),
            "spread": round(spread, 2),
            "cv_pct": round(cv, 1),
            "has_quality_flags": any_flagged,
            "divergence_causes": "; ".join(causes) if causes else "within_tolerance",
        })

    vdf = pd.DataFrame(variance_rows)

    logger.info(f"Multi-source utilities with comparable bills: {len(vdf)}")
    print()

    # --- 3. Variance summary ---
    logger.info("--- Variance Summary ---")
    if len(vdf) > 0:
        logger.info(f"Mean CV: {vdf.cv_pct.mean():.1f}%")
        logger.info(f"Median CV: {vdf.cv_pct.median():.1f}%")
        logger.info(f"Mean spread: ${vdf.spread.mean():.2f}")
        logger.info(f"Median spread: ${vdf.spread.median():.2f}")
        print()

        # Category breakdown
        vdf["var_cat"] = pd.cut(
            vdf.cv_pct,
            bins=[0, 10, 25, 50, 100, float("inf")],
            labels=["<10% agree", "10-25% moderate", "25-50% divergent", "50-100% major", ">100% conflict"],
        )
        logger.info("Variance categories:")
        for cat, cnt in vdf.var_cat.value_counts().sort_index().items():
            pct = cnt / len(vdf) * 100
            logger.info(f"  {cat}: {cnt} ({pct:.0f}%)")
        print()

        # Divergence cause breakdown
        logger.info("Divergence cause breakdown (for CV > threshold):")
        divergent = vdf[vdf.cv_pct > cv_threshold]
        cause_counts = {}
        for causes in divergent.divergence_causes:
            for c in causes.split("; "):
                c = c.strip()
                if c and c != "within_tolerance":
                    cause_counts[c] = cause_counts.get(c, 0) + 1
        for cause, cnt in sorted(cause_counts.items(), key=lambda x: -x[1]):
            logger.info(f"  {cause}: {cnt}")
        print()

    # --- 4. Specific problem categories ---
    logger.info("--- Specific Problem Categories ---")

    # eAR tier inflation
    ear_inflated = df[df.quality_flags.apply(lambda f: any("ear_tier_limit_inflated" in x for x in f))]
    logger.info(f"eAR tier limit inflation: {len(ear_inflated)} records across {ear_inflated.pwsid.nunique()} utilities")
    if len(ear_inflated) > 0:
        for _, r in ear_inflated.head(5).iterrows():
            logger.info(f"  {r.pwsid} {r.source} tier1_limit={r.tier_1_limit_ccf}")

    # Possible combined water+sewer (scraped much higher than eAR/OWRS)
    combined_suspect = []
    for pwsid, group in multi.groupby("pwsid"):
        scraped = group[group.source == "scraped_llm"]
        others = group[group.source != "scraped_llm"]
        if len(scraped) == 0 or len(others) == 0:
            continue
        scraped_bill = scraped.comp_bill.iloc[0]
        other_bills = others[others.comp_bill.notna()].comp_bill.values
        if len(other_bills) == 0:
            continue
        # Filter out obviously broken eAR values
        reasonable_others = other_bills[other_bills < BILL_SUSPECT_HIGH]
        if len(reasonable_others) == 0:
            continue
        other_median = pd.Series(reasonable_others).median()
        if scraped_bill > other_median * 1.5 and scraped_bill > 40:
            combined_suspect.append({
                "pwsid": pwsid,
                "name": group.utility_name.iloc[0] or "",
                "scraped_bill": round(scraped_bill, 2),
                "other_median": round(other_median, 2),
                "ratio": round(scraped_bill / other_median, 2),
            })

    print()
    logger.info(f"Suspected combined water+sewer scrapes: {len(combined_suspect)}")
    for cs in sorted(combined_suspect, key=lambda x: -x["ratio"])[:10]:
        logger.info(
            f"  {cs['pwsid']} {cs['name'][:35]:35s} "
            f"scraped=${cs['scraped_bill']:.0f} vs others=${cs['other_median']:.0f} "
            f"({cs['ratio']:.1f}x)"
        )
    print()

    # Stale data
    stale = df[df.quality_flags.apply(lambda f: any("stale_vintage" in x for x in f))]
    logger.info(f"Stale vintage records (pre-2015): {len(stale)} across {stale.pwsid.nunique()} utilities")

    # --- 5. Coverage summary ---
    print()
    logger.info("--- Coverage Summary ---")
    total_unique_pwsids = df.pwsid.nunique()
    single_source = df.groupby("pwsid").filter(lambda g: g.source.nunique() == 1).pwsid.nunique()
    multi_source = df.groupby("pwsid").filter(lambda g: g.source.nunique() > 1).pwsid.nunique()
    logger.info(f"Total unique PWSIDs with rate data: {total_unique_pwsids}")
    logger.info(f"  Single source only: {single_source}")
    logger.info(f"  Multi-source (can cross-validate): {multi_source}")

    # Source-specific unique counts
    for source in sorted(df.source.unique()):
        n = df[df.source == source].pwsid.nunique()
        logger.info(f"  {source}: {n} unique PWSIDs")

    # --- Write CSV ---
    if write_csv and len(vdf) > 0:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = OUTPUT_DIR / "rate_reconciliation.csv"
        vdf.to_csv(csv_path, index=False)
        logger.info(f"\nCSV report written: {csv_path}")

    # --- Summary stats ---
    stats = {
        "total_records": len(df),
        "unique_pwsids": total_unique_pwsids,
        "multi_source_pwsids": multi_source,
        "flagged_records": len(flagged),
        "ear_tier_inflated": len(ear_inflated),
        "suspected_combined": len(combined_suspect),
        "stale_records": len(stale),
        "mean_cv": round(vdf.cv_pct.mean(), 1) if len(vdf) > 0 else 0,
        "median_cv": round(vdf.cv_pct.median(), 1) if len(vdf) > 0 else 0,
    }
    return stats


def main():
    parser = argparse.ArgumentParser(description="Rate reconciliation diagnostic")
    parser.add_argument("--csv", action="store_true", help="Write CSV report")
    parser.add_argument("--threshold", type=float, default=25.0, help="CV%% threshold for flagging")
    args = parser.parse_args()

    stats = run_reconciliation(cv_threshold=args.threshold, write_csv=args.csv)

    print()
    logger.info("=== Reconciliation Summary ===")
    for k, v in stats.items():
        logger.info(f"  {k}: {v}")


if __name__ == "__main__":
    main()

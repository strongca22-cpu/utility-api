#!/usr/bin/env python3
"""
Generalized Best-Estimate Rate Selection

Purpose:
    For each PWSID with rate data, selects the single "best estimate" bill
    amount using a config-driven source priority hierarchy. Writes results
    to utility.rate_best_estimate table via ORM.

    Generalized from the Sprint 6 CA-only version (scripts/build_best_estimate.py)
    to handle all states. For multi-source states (CA), government data serves
    as a QA cross-reference — divergence is flagged but does not demote scrape.

    Priority logic (configurable via config/source_priority.yaml):
    1. LLM-scraped rates — primary source (most current, full structure)
    2. Government bulk data (eAR, EFC) — fallback + QA cross-reference
    3. Curated third-party (OWRS) — fallback
    4. Reference datasets (Duke, TML) — last resort
    5. Oldest government vintage — lowest priority

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru
    - pyyaml

Usage:
    ua-ops build-best-estimate              # Build all states
    ua-ops build-best-estimate --state CA   # CA only
    ua-ops build-best-estimate --dry-run    # Preview

Notes:
    - Replaces scripts/build_best_estimate.py (which remains as legacy reference)
    - Uses ORM model RateBestEstimate instead of raw SQL table creation
    - Source priority loaded from config/source_priority.yaml
    - Anchor logic only applies when state has anchor_sources configured
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# --- Config Loading ---

def load_source_priority() -> dict:
    """Load source priority configuration.

    Returns
    -------
    dict
        Priority config with 'default' key and optional state overrides.
    """
    config_path = PROJECT_ROOT / "config" / "source_priority.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_source_base_priority(config: dict) -> dict[str, int]:
    """Build source_key → base_priority mapping from config.

    Returns
    -------
    dict[str, int]
        Mapping of source keys to their base priority numbers.
    """
    priorities = {}
    for entry in config.get("default", {}).get("priority_order", []):
        priorities[entry["source"]] = entry["priority"]
    return priorities


def resolve_source_priority(
    source: str,
    base_priorities: dict[str, int],
    config: dict,
) -> int:
    """Resolve priority for a source, including pattern-based fallbacks.

    Parameters
    ----------
    source : str
        The source_key to look up.
    base_priorities : dict
        Exact-match priority mapping from get_source_base_priority().
    config : dict
        Full config (for pattern overrides and fallback_priority).

    Returns
    -------
    int
        Priority number (lower = higher priority).
    """
    # 1. Exact match
    if source in base_priorities:
        return base_priorities[source]

    # 2. Pattern-based overrides
    defaults = config.get("default", {})
    for pattern_entry in defaults.get("source_patterns", []):
        pattern = pattern_entry.get("pattern", "")
        if pattern and source.startswith(pattern):
            return pattern_entry["priority"]

    # 3. Fallback
    return defaults.get("fallback_priority", 99)


def get_source_display_tiers(config: dict) -> dict[str, str]:
    """Build source_key → display_tier mapping from config.

    Returns
    -------
    dict[str, str]
        Mapping of source keys to their display tier (premium, free, reference).
    """
    tiers = {}
    for entry in config.get("default", {}).get("priority_order", []):
        if "display_tier" in entry:
            tiers[entry["source"]] = entry["display_tier"]
    return tiers


def resolve_display_tier(source: str, config: dict) -> str:
    """Resolve display_tier for a source, including pattern-based fallbacks.

    Parameters
    ----------
    source : str
        The source_key to look up.
    config : dict
        Full source priority config.

    Returns
    -------
    str
        Display tier: 'premium', 'free', or 'reference'.
    """
    # 1. Exact match from priority_order entries
    for entry in config.get("default", {}).get("priority_order", []):
        if entry.get("source") == source and "display_tier" in entry:
            return entry["display_tier"]

    # 2. Pattern-based overrides
    defaults = config.get("default", {})
    for pattern_entry in defaults.get("source_patterns", []):
        pattern = pattern_entry.get("pattern", "")
        if pattern and source.startswith(pattern) and "display_tier" in pattern_entry:
            return pattern_entry["display_tier"]

    # 3. Fallback
    return defaults.get("fallback_display_tier", "free")


# --- Bill Extraction ---

def get_comparable_bill(row: pd.Series) -> float | None:
    """Extract a comparable ~10CCF monthly bill from any source.

    Tries bill_10ccf first, then interpolates from eAR snapshots,
    then falls back to whatever is available.
    """
    if pd.notna(row.get("bill_10ccf")) and row["bill_10ccf"] > 0:
        return float(row["bill_10ccf"])
    # Interpolate from eAR 9+12 CCF snapshots
    if pd.notna(row.get("bill_9ccf")) and pd.notna(row.get("bill_12ccf")):
        b9, b12 = float(row["bill_9ccf"]), float(row["bill_12ccf"])
        if b9 > 0 and b12 > 0:
            return round(b9 + (b12 - b9) / 3.0, 2)
    if pd.notna(row.get("bill_12ccf")) and row["bill_12ccf"] > 0:
        return float(row["bill_12ccf"])
    if pd.notna(row.get("bill_6ccf")) and row["bill_6ccf"] > 0:
        return float(row["bill_6ccf"])
    return None


# --- Per-PWSID Selection ---

def select_best_estimate(group: pd.DataFrame, config: dict, base_priorities: dict) -> dict:
    """Select the best estimate for a single PWSID.

    Parameters
    ----------
    group : pd.DataFrame
        All rate_schedules records for one PWSID.
    config : dict
        Full source priority config.
    base_priorities : dict
        source_key → base priority mapping.

    Returns
    -------
    dict
        Best estimate record with selection rationale.
    """
    pwsid = group.iloc[0]["pwsid"]
    utility_name = group.iloc[0]["utility_name"] or ""
    state_code = group.iloc[0].get("state_code", pwsid[:2])

    # Get state-specific config (if any)
    state_config = config.get(state_code, {})
    anchor_sources = state_config.get("anchor_sources", [])
    anchor_tolerance = state_config.get("anchor_tolerance", 0.25)
    fallback_priority = config.get("default", {}).get("fallback_priority", 99)

    # Build records with comparable bills
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
            "state_code": state_code,
            "source_url": row.get("source_url"),
        })

    # Find anchor (if this state has anchor sources configured)
    anchor = None
    anchor_bill = None
    for anchor_src in anchor_sources:
        candidates = [r for r in records if r["source"] == anchor_src and r["comp_bill"] is not None]
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
        base_priority = resolve_source_priority(source, base_priorities, config)

        notes = []

        # Dynamic priority adjustment for scraped_llm
        if source == "scraped_llm":
            if confidence == "low":
                base_priority = max(base_priority, 6)
                notes.append("low_confidence")
            elif anchor_bill is not None:
                pct_diff = abs(r["comp_bill"] - anchor_bill) / anchor_bill
                if pct_diff <= anchor_tolerance:
                    notes.append(f"anchor_agrees ({pct_diff:.0%} diff)")
                else:
                    # Scraped diverges from anchor — flag for QA but do NOT demote.
                    # Scraped data from actual rate pages is the primary source;
                    # bulk data serves as cross-reference, not gate.
                    notes.append(f"anchor_diverges_qa_flag ({pct_diff:.0%} diff)")
            else:
                # No anchor available — use base priority
                notes.append("no_anchor_available")

        scored.append({
            **r,
            "priority": base_priority,
            "selection_notes": "; ".join(notes) if notes else "",
        })

    # Handle no-data case
    if not scored:
        return {
            "pwsid": pwsid,
            "utility_name": utility_name,
            "state_code": state_code,
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
            "source_url": None,
        }

    # Select best (lowest priority number)
    scored.sort(key=lambda x: x["priority"])
    best = scored[0]

    # Determine confidence
    if best["priority"] == 0:
        confidence = "high"  # scraped agrees with anchor
    elif best["source"].startswith("swrcb_ear") or best["source"].startswith("efc_"):
        confidence = "high"  # government source
    elif best["source"] == "owrs":
        confidence = "medium"  # curated but old
    elif best.get("parse_confidence") == "high":
        confidence = "medium"  # scraped high-confidence, no anchor
    elif best.get("parse_confidence") == "low":
        confidence = "low"
    else:
        confidence = "medium"

    return {
        "pwsid": pwsid,
        "utility_name": utility_name[:255] if utility_name else None,
        "state_code": state_code,
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
        "source_url": best.get("source_url"),
    }


# --- Main Entry Point ---

def run_best_estimate(
    state_filter: str | None = None,
    dry_run: bool = False,
    write_csv: bool = False,
    snapshot: bool = True,
) -> dict:
    """Build best-estimate rate selection for all PWSIDs.

    Parameters
    ----------
    state_filter : str, optional
        Limit to a single state code (e.g., "CA", "NC").
    dry_run : bool
        Preview only, don't write to DB.
    write_csv : bool
        Also write CSV output.
    snapshot : bool
        If True (default), take a Tier 2 pre-write snapshot of the critical
        tables before TRUNCATE/DELETE. Set False in tests to skip.

    Returns
    -------
    dict
        Summary statistics.
    """
    logger.info("=== Build Best-Estimate Rate Selection (Generalized) ===")

    # Load config
    config = load_source_priority()
    base_priorities = get_source_base_priority(config)
    logger.info(f"Source priority: {base_priorities}")

    schema = settings.utility_schema

    # Load rate data from rate_schedules (sole source after Phase 3/4 deprecation)
    where_clause = ""
    params = {}
    if state_filter:
        where_clause = "WHERE c.state_code = :state"
        params["state"] = state_filter
        logger.info(f"Filtering to state: {state_filter}")

    with engine.connect() as conn:
        state_join = f"{'JOIN' if state_filter else 'LEFT JOIN'} {schema}.cws_boundaries c ON rs.pwsid = c.pwsid"

        df = pd.read_sql(text(f"""
            SELECT rs.pwsid,
                   rs.source_key AS source,
                   c.pws_name AS utility_name,
                   c.state_code,
                   rs.bill_5ccf,
                   rs.bill_6ccf,
                   rs.bill_9ccf,
                   rs.bill_10ccf,
                   rs.bill_12ccf,
                   rs.bill_24ccf,
                   (rs.fixed_charges->0->>'amount')::float AS fixed_charge_monthly,
                   rs.rate_structure_type,
                   rs.vintage_date AS rate_effective_date,
                   rs.billing_frequency,
                   rs.confidence AS parse_confidence,
                   rs.source_url
            FROM {schema}.rate_schedules rs
            {state_join}
            {where_clause}
            ORDER BY rs.pwsid, rs.source_key
        """), conn, params=params)

    if len(df) == 0:
        logger.warning("No rate records found")
        return {"total_pwsids": 0}

    logger.info(f"Loaded {len(df)} rate records for {df.pwsid.nunique()} PWSIDs")

    # Build best estimate per PWSID
    results = []
    for pwsid, group in df.groupby("pwsid"):
        result = select_best_estimate(group, config, base_priorities)
        results.append(result)

    rdf = pd.DataFrame(results)
    logger.info(f"Best estimates computed: {len(rdf)}")

    # --- Statistics ---
    stats = {
        "total_pwsids": len(rdf),
        "with_estimate": int(rdf.bill_estimate_10ccf.notna().sum()),
        "no_estimate": int(rdf.bill_estimate_10ccf.isna().sum()),
    }

    # State breakdown
    logger.info("\nCoverage by state:")
    for state, sdf in rdf.groupby("state_code"):
        with_est = sdf.bill_estimate_10ccf.notna().sum()
        logger.info(f"  {state}: {len(sdf)} PWSIDs, {with_est} with estimates")

    # Source selection breakdown
    logger.info("\nSelected source breakdown:")
    for src, cnt in rdf.selected_source.value_counts().items():
        pct = cnt / len(rdf) * 100
        logger.info(f"  {src}: {cnt} ({pct:.0f}%)")
    none_count = rdf.selected_source.isna().sum()
    if none_count:
        logger.info(f"  (no estimate): {none_count}")

    # Confidence breakdown
    logger.info("\nConfidence breakdown:")
    for conf, cnt in rdf.confidence.value_counts().items():
        pct = cnt / len(rdf) * 100
        logger.info(f"  {conf}: {cnt} ({pct:.0f}%)")

    # Bill distribution
    has_bill = rdf[rdf.bill_estimate_10ccf.notna()]
    if len(has_bill) > 0:
        logger.info(f"\nBill estimate @10CCF: "
                     f"median=${has_bill.bill_estimate_10ccf.median():.2f}, "
                     f"mean=${has_bill.bill_estimate_10ccf.mean():.2f}, "
                     f"range=[${has_bill.bill_estimate_10ccf.min():.2f}–"
                     f"${has_bill.bill_estimate_10ccf.max():.2f}]")

    if dry_run:
        logger.info("\n[DRY RUN] Would write to rate_best_estimate table")
        for _, r in rdf.head(10).iterrows():
            logger.info(
                f"  {r.pwsid} {(r.utility_name or '')[:30]:30s} "
                f"src={r.selected_source or 'none':20s} "
                f"bill=${r.bill_estimate_10ccf or 0:7.2f} "
                f"[{r.confidence}] {r.selection_notes}"
            )
        return stats

    # --- Tier 2 pre-write snapshot ---
    # Take a gzipped CSV snapshot of the critical tables BEFORE the destructive
    # TRUNCATE/DELETE below. Failure here MUST NOT block the rebuild — log and
    # continue. See src/utility_api/ops/snapshot.py and docs/backup_system.md.
    if snapshot:
        try:
            from utility_api.ops.snapshot import snapshot_critical_tables

            snap_paths = snapshot_critical_tables(reason="best_estimate_rebuild")
            for tbl, p in snap_paths.items():
                logger.info(f"  snapshot[{tbl}] = {p}")
        except Exception as e:
            logger.warning(f"Tier 2 snapshot failed (continuing): {e}")

    # --- Write to database ---
    with engine.connect() as conn:
        # Truncate existing best estimates (for filtered state or all)
        if state_filter:
            conn.execute(text(f"""
                DELETE FROM {schema}.rate_best_estimate
                WHERE state_code = :state
            """), {"state": state_filter})
        else:
            conn.execute(text(f"TRUNCATE TABLE {schema}.rate_best_estimate"))

        # Insert records
        inserted = 0
        skipped_fk = 0
        for _, r in rdf.iterrows():
            try:
                conn.execute(text(f"""
                    INSERT INTO {schema}.rate_best_estimate (
                        pwsid, utility_name, state_code, selected_source,
                        bill_estimate_10ccf, bill_5ccf, bill_10ccf, bill_6ccf, bill_12ccf,
                        fixed_charge_monthly, rate_structure_type, rate_effective_date,
                        n_sources, anchor_source, anchor_bill, confidence, selection_notes,
                        source_url
                    ) VALUES (
                        :pwsid, :utility_name, :state_code, :selected_source,
                        :bill_estimate_10ccf, :bill_5ccf, :bill_10ccf, :bill_6ccf, :bill_12ccf,
                        :fixed_charge_monthly, :rate_structure_type, :rate_effective_date,
                        :n_sources, :anchor_source, :anchor_bill, :confidence, :selection_notes,
                        :source_url
                    )
                """), {
                    "pwsid": r["pwsid"],
                    "utility_name": r["utility_name"] if pd.notna(r.get("utility_name")) else None,
                    "state_code": r.get("state_code"),
                    "selected_source": r["selected_source"] if pd.notna(r.get("selected_source")) else None,
                    "bill_estimate_10ccf": float(r["bill_estimate_10ccf"]) if pd.notna(r.get("bill_estimate_10ccf")) else None,
                    "bill_5ccf": float(r["bill_5ccf"]) if pd.notna(r.get("bill_5ccf")) else None,
                    "bill_10ccf": float(r["bill_10ccf"]) if pd.notna(r.get("bill_10ccf")) else None,
                    "bill_6ccf": float(r["bill_6ccf"]) if pd.notna(r.get("bill_6ccf")) else None,
                    "bill_12ccf": float(r["bill_12ccf"]) if pd.notna(r.get("bill_12ccf")) else None,
                    "fixed_charge_monthly": float(r["fixed_charge_monthly"]) if pd.notna(r.get("fixed_charge_monthly")) else None,
                    "rate_structure_type": r["rate_structure_type"] if pd.notna(r.get("rate_structure_type")) else None,
                    "rate_effective_date": r.get("rate_effective_date"),
                    "n_sources": int(r.get("n_sources", 1)),
                    "anchor_source": r["anchor_source"] if pd.notna(r.get("anchor_source")) else None,
                    "anchor_bill": float(r["anchor_bill"]) if pd.notna(r.get("anchor_bill")) else None,
                    "confidence": r["confidence"] if pd.notna(r.get("confidence")) else None,
                    "selection_notes": r["selection_notes"] if pd.notna(r.get("selection_notes")) else None,
                    "source_url": r["source_url"] if pd.notna(r.get("source_url")) else None,
                })
                inserted += 1
            except Exception as e:
                if "violates foreign key" in str(e):
                    skipped_fk += 1
                else:
                    logger.warning(f"  Failed to insert {r['pwsid']}: {e}")
                    skipped_fk += 1
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
            "notes": (f"{inserted} best estimates built from {len(df)} rate records "
                      f"across {df.pwsid.nunique()} PWSIDs"
                      + (f" (state={state_filter})" if state_filter else " (all states)")),
        })
        conn.commit()

    logger.info(f"\nInserted {inserted} best estimates ({skipped_fk} skipped FK)")

    if write_csv:
        output_dir = PROJECT_ROOT / "data" / "interim"
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "rate_best_estimate.csv"
        rdf.to_csv(csv_path, index=False)
        logger.info(f"CSV written: {csv_path}")

    logger.info("=== Best Estimate Complete ===")
    return stats

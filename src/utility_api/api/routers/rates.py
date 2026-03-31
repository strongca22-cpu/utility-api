#!/usr/bin/env python3
"""
/rates Endpoint

Purpose:
    Serve parsed water rate data by PWSID. Returns tier structure,
    fixed charges, computed bills, and parse provenance metadata.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - fastapi
    - sqlalchemy

Usage:
    GET /rates/VA4760100           # Single utility rate data
    GET /rates?state=VA            # All parsed rates for a state
    GET /rates?min_confidence=high # Filter by parse confidence
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.config import settings

router = APIRouter(tags=["rates"])

SCHEMA = settings.utility_schema


@router.get("/rates/best-estimate")
def list_best_estimates(
    state: str | None = Query(None, description="Filter by state code (e.g., CA)"),
    min_confidence: str = Query(None, description="Minimum confidence: high, medium, low"),
    min_bill: float | None = Query(None, description="Minimum bill @10CCF"),
    max_bill: float | None = Query(None, description="Maximum bill @10CCF"),
    db: Session = Depends(get_db),
):
    """List best-estimate rates for all utilities.

    Returns one row per PWSID with the selected best-estimate source,
    bill amount, and confidence. Use for comparison and ranking.

    Source priority: eAR 2022 (government anchor) > scraped (if agrees) > OWRS > scraped (diverges).
    """
    clauses = []
    params = {}

    if state:
        clauses.append("be.state_code = :state")
        params["state"] = state.upper()
    if min_confidence:
        conf_map = {"high": ["high"], "medium": ["high", "medium"], "low": ["high", "medium", "low"]}
        conf_list = conf_map.get(min_confidence.lower(), ["high", "medium", "low"])
        placeholders = ", ".join(f"'{c}'" for c in conf_list)
        clauses.append(f"be.confidence IN ({placeholders})")
    if min_bill is not None:
        clauses.append("be.bill_estimate_10ccf >= :min_bill")
        params["min_bill"] = min_bill
    if max_bill is not None:
        clauses.append("be.bill_estimate_10ccf <= :max_bill")
        params["max_bill"] = max_bill

    where = "WHERE " + " AND ".join(clauses) if clauses else ""

    rows = db.execute(text(f"""
        SELECT
            be.pwsid, be.utility_name, be.state_code,
            be.selected_source, be.bill_estimate_10ccf,
            be.bill_5ccf, be.bill_10ccf, be.bill_6ccf, be.bill_12ccf,
            be.fixed_charge_monthly, be.rate_structure_type,
            be.rate_effective_date, be.n_sources,
            be.anchor_source, be.anchor_bill,
            be.confidence, be.selection_notes,
            m.population
        FROM {SCHEMA}.rate_best_estimate be
        LEFT JOIN {SCHEMA}.mdwd_financials m ON m.pwsid = be.pwsid
        {where}
        ORDER BY m.population DESC NULLS LAST, be.bill_estimate_10ccf DESC NULLS LAST
    """), params).mappings().all()

    results = []
    for row in rows:
        results.append({
            "pwsid": row["pwsid"],
            "utility_name": row["utility_name"],
            "state_code": row["state_code"],
            "selected_source": row["selected_source"],
            "bill_estimate_10ccf": row["bill_estimate_10ccf"],
            "bill_5ccf": row["bill_5ccf"],
            "bill_10ccf": row["bill_10ccf"],
            "bill_6ccf": row["bill_6ccf"],
            "bill_12ccf": row["bill_12ccf"],
            "fixed_charge_monthly": row["fixed_charge_monthly"],
            "rate_structure": row["rate_structure_type"],
            "rate_effective_date": str(row["rate_effective_date"]) if row["rate_effective_date"] else None,
            "n_sources": row["n_sources"],
            "anchor_source": row["anchor_source"],
            "anchor_bill": row["anchor_bill"],
            "confidence": row["confidence"],
            "selection_notes": row["selection_notes"],
            "population_served": row["population"],
        })

    return {
        "total_results": len(results),
        "filters": {"state": state, "min_confidence": min_confidence, "min_bill": min_bill, "max_bill": max_bill},
        "rates": results,
    }


@router.get("/rates/{pwsid}")
def get_rate(pwsid: str, db: Session = Depends(get_db)):
    """Get parsed water rate data for a specific utility.

    Reads from rate_schedules (canonical JSONB schema) when available,
    falls back to water_rates (legacy fixed columns).

    Parameters
    ----------
    pwsid : str
        EPA Public Water System ID (e.g., VA4760100).

    Returns
    -------
    dict
        Rate structure, tier data, computed bills, and provenance.
    """
    pwsid_upper = pwsid.upper()

    # Try rate_schedules first (canonical)
    row = db.execute(text(f"""
        SELECT
            rs.pwsid, c.pws_name AS utility_name, c.state_code, c.county_served AS county,
            rs.vintage_date AS rate_effective_date, rs.rate_structure_type,
            rs.customer_class AS rate_class,
            rs.billing_frequency,
            rs.fixed_charges, rs.volumetric_tiers, rs.surcharges,
            rs.bill_5ccf, rs.bill_10ccf, rs.bill_20ccf,
            rs.conservation_signal, rs.tier_count,
            rs.source_key, rs.source_url, rs.confidence AS parse_confidence,
            rs.parse_model, rs.parse_notes, rs.scrape_timestamp,
            rs.needs_review, rs.review_reason
        FROM {SCHEMA}.rate_schedules rs
        JOIN {SCHEMA}.cws_boundaries c ON c.pwsid = rs.pwsid
        WHERE rs.pwsid = :pwsid
        AND rs.confidence IN ('high', 'medium')
        ORDER BY rs.vintage_date DESC NULLS LAST
        LIMIT 1
    """), {"pwsid": pwsid_upper}).mappings().first()

    if row:
        # Serve from canonical schema
        fixed_charges = row["fixed_charges"] or []
        volumetric_tiers = row["volumetric_tiers"] or []
        fixed_monthly = sum(fc.get("amount", 0) for fc in fixed_charges) if fixed_charges else None

        return {
            "pwsid": row["pwsid"],
            "utility_name": row["utility_name"],
            "state_code": row["state_code"],
            "county": row["county"],
            "rate_effective_date": str(row["rate_effective_date"]) if row["rate_effective_date"] else None,
            "rate_structure": row["rate_structure_type"],
            "rate_class": row["rate_class"],
            "billing_frequency": row["billing_frequency"],
            "fixed_charge_monthly": fixed_monthly,
            "fixed_charges": fixed_charges,
            "tiers": volumetric_tiers,
            "surcharges": row["surcharges"] or [],
            "bill_5ccf": row["bill_5ccf"],
            "bill_10ccf": row["bill_10ccf"],
            "bill_20ccf": row["bill_20ccf"],
            "conservation_signal": row["conservation_signal"],
            "tier_count": row["tier_count"],
            "needs_review": row["needs_review"],
            "review_reason": row["review_reason"],
            "provenance": {
                "source_key": row["source_key"],
                "source_url": row["source_url"],
                "parse_confidence": row["parse_confidence"],
                "parse_model": row["parse_model"],
                "parse_notes": row["parse_notes"],
                "scraped_at": row["scrape_timestamp"].isoformat() if row["scrape_timestamp"] else None,
            },
        }

    # Phase 4: water_rates fallback removed — rate_schedules is sole source
    raise HTTPException(
        status_code=404,
        detail=f"No parsed rate data found for PWSID {pwsid}"
    )


@router.get("/rates")
def list_rates(
    state: str | None = Query(None, description="Filter by state code (e.g., VA)"),
    min_confidence: str = Query("medium", description="Minimum parse confidence: high, medium"),
    db: Session = Depends(get_db),
):
    """List all parsed water rates, optionally filtered by state.

    Returns summary records (no full tier data) for listing/comparison.
    Use /rates/{pwsid} for full tier detail.
    """
    confidence_list = ["high"] if min_confidence == "high" else ["high", "medium"]
    placeholders = ", ".join(f"'{c}'" for c in confidence_list)

    state_clause = ""
    params = {}
    if state:
        state_clause = "AND c.state_code = :state"
        params["state"] = state.upper()

    rows = db.execute(text(f"""
        SELECT
            rs.pwsid, c.pws_name AS utility_name, c.state_code, c.county,
            rs.rate_structure_type,
            (rs.fixed_charges->0->>'amount')::float AS fixed_charge_monthly,
            rs.bill_5ccf, rs.bill_10ccf,
            rs.confidence AS parse_confidence, rs.vintage_date AS rate_effective_date,
            m.population
        FROM {SCHEMA}.rate_schedules rs
        LEFT JOIN {SCHEMA}.cws_boundaries c ON c.pwsid = rs.pwsid
        LEFT JOIN {SCHEMA}.mdwd_financials m ON m.pwsid = rs.pwsid
        WHERE rs.confidence IN ({placeholders})
        {state_clause}
        ORDER BY m.population DESC NULLS LAST
    """), params).mappings().all()

    results = []
    for row in rows:
        results.append({
            "pwsid": row["pwsid"],
            "utility_name": row["utility_name"],
            "state_code": row["state_code"],
            "county": row["county"],
            "rate_structure": row["rate_structure_type"],
            "fixed_charge_monthly": row["fixed_charge_monthly"],
            "bill_5ccf": row["bill_5ccf"],
            "bill_10ccf": row["bill_10ccf"],
            "parse_confidence": row["parse_confidence"],
            "rate_effective_date": str(row["rate_effective_date"]) if row["rate_effective_date"] else None,
            "population_served": row["population"],
        })

    return {
        "total_results": len(results),
        "state_filter": state,
        "min_confidence": min_confidence,
        "rates": results,
    }

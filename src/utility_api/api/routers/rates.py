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


@router.get("/rates/{pwsid}")
def get_rate(pwsid: str, db: Session = Depends(get_db)):
    """Get parsed water rate data for a specific utility.

    Parameters
    ----------
    pwsid : str
        EPA Public Water System ID (e.g., VA4760100).

    Returns
    -------
    dict
        Rate structure, tier data, computed bills, and provenance.
    """
    row = db.execute(text(f"""
        SELECT
            w.pwsid, w.utility_name, w.state_code, w.county,
            w.rate_effective_date, w.rate_structure_type, w.rate_class,
            w.billing_frequency, w.fixed_charge_monthly, w.meter_size_inches,
            w.tier_1_limit_ccf, w.tier_1_rate,
            w.tier_2_limit_ccf, w.tier_2_rate,
            w.tier_3_limit_ccf, w.tier_3_rate,
            w.tier_4_limit_ccf, w.tier_4_rate,
            w.bill_5ccf, w.bill_10ccf,
            w.source_url, w.parse_confidence, w.parse_model,
            w.parse_notes, w.scraped_at, w.parsed_at
        FROM {SCHEMA}.water_rates w
        WHERE w.pwsid = :pwsid
        AND w.parse_confidence IN ('high', 'medium')
        ORDER BY w.rate_effective_date DESC NULLS LAST
        LIMIT 1
    """), {"pwsid": pwsid.upper()}).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No parsed rate data found for PWSID {pwsid}"
        )

    # Build tier list (non-null tiers only)
    tiers = []
    for i in range(1, 5):
        rate = row[f"tier_{i}_rate"]
        if rate is not None:
            tiers.append({
                "tier": i,
                "limit_ccf": row[f"tier_{i}_limit_ccf"],
                "rate_per_ccf": rate,
            })

    return {
        "pwsid": row["pwsid"],
        "utility_name": row["utility_name"],
        "state_code": row["state_code"],
        "county": row["county"],
        "rate_effective_date": str(row["rate_effective_date"]) if row["rate_effective_date"] else None,
        "rate_structure": row["rate_structure_type"],
        "rate_class": row["rate_class"],
        "billing_frequency": row["billing_frequency"],
        "fixed_charge_monthly": row["fixed_charge_monthly"],
        "meter_size_inches": row["meter_size_inches"],
        "tiers": tiers,
        "bill_5ccf": row["bill_5ccf"],
        "bill_10ccf": row["bill_10ccf"],
        "provenance": {
            "source_url": row["source_url"],
            "parse_confidence": row["parse_confidence"],
            "parse_model": row["parse_model"],
            "parse_notes": row["parse_notes"],
            "scraped_at": row["scraped_at"].isoformat() if row["scraped_at"] else None,
            "parsed_at": row["parsed_at"] if row["parsed_at"] else None,
        },
    }


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
        state_clause = "AND w.state_code = :state"
        params["state"] = state.upper()

    rows = db.execute(text(f"""
        SELECT
            w.pwsid, w.utility_name, w.state_code, w.county,
            w.rate_structure_type, w.fixed_charge_monthly,
            w.bill_5ccf, w.bill_10ccf,
            w.parse_confidence, w.rate_effective_date,
            m.population
        FROM {SCHEMA}.water_rates w
        LEFT JOIN {SCHEMA}.mdwd_financials m ON m.pwsid = w.pwsid
        WHERE w.parse_confidence IN ({placeholders})
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

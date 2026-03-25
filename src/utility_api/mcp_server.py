#!/usr/bin/env python3
"""
Utility Intelligence MCP Server

Purpose:
    Model Context Protocol server that exposes utility data lookup tools
    for MCP-compatible agents (Claude Desktop, etc.). Wraps the /resolve
    and /utility/{pwsid} functionality as MCP tools with direct database
    access (no HTTP intermediary).

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - mcp (>=1.0)
    - sqlalchemy
    - geoalchemy2

Usage:
    python -m utility_api.mcp_server          # Run as stdio MCP server
    ua-mcp                                     # Via CLI entry point

    Claude Desktop config (~/.claude/claude_desktop_config.json):
    {
        "mcpServers": {
            "utility-api": {
                "command": "python",
                "args": ["-m", "utility_api.mcp_server"]
            }
        }
    }

Notes:
    - Direct DB access (same SQLAlchemy engine as FastAPI app)
    - No auth — running locally as a subprocess of the MCP client
    - Two tools: resolve_water_utility, get_utility_details
"""

from mcp.server.fastmcp import FastMCP
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine

mcp = FastMCP("Utility Intelligence")

SCHEMA = settings.utility_schema


@mcp.tool()
def resolve_water_utility(latitude: float, longitude: float) -> dict:
    """Given a latitude and longitude, return the water utility that serves
    that location with rate data, SDWIS metadata, and water stress risk.

    Parameters
    ----------
    latitude : float
        WGS84 latitude (-90 to 90).
    longitude : float
        WGS84 longitude (-180 to 180).

    Returns
    -------
    dict
        Utility identity, SDWIS attributes, rate data, and Aqueduct risk scores.
        Returns {"error": "..."} if no utility found at that location.
    """
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            WITH cws_match AS (
                SELECT pwsid, pws_name, state_code, county_served,
                       population_served, source_type
                FROM {SCHEMA}.cws_boundaries
                WHERE ST_Contains(geom, ST_SetSRID(ST_Point(:lng, :lat), 4326))
                LIMIT 1
            ),
            sdwis AS (
                SELECT pws_name, owner_type_code, population_served_count,
                       primary_source_code, violation_count_5yr
                FROM {SCHEMA}.sdwis_systems
                WHERE pwsid = (SELECT pwsid FROM cws_match)
            ),
            best_rate AS (
                SELECT selected_source, bill_estimate_10ccf, fixed_charge_monthly,
                       rate_structure_type, confidence, rate_effective_date
                FROM {SCHEMA}.rate_best_estimate
                WHERE pwsid = (SELECT pwsid FROM cws_match)
            ),
            risk AS (
                SELECT bws_label AS water_stress, bwd_score AS water_depletion,
                       drr_score AS drought_risk
                FROM {SCHEMA}.aqueduct_polygons
                WHERE ST_Contains(geom, ST_SetSRID(ST_Point(:lng, :lat), 4326))
                LIMIT 1
            )
            SELECT
                c.pwsid, c.pws_name, c.state_code, c.county_served, c.population_served,
                s.owner_type_code, s.primary_source_code, s.violation_count_5yr,
                b.selected_source, b.bill_estimate_10ccf, b.fixed_charge_monthly,
                b.rate_structure_type, b.confidence, b.rate_effective_date,
                r.water_stress, r.water_depletion, r.drought_risk
            FROM (SELECT 1) dummy
            LEFT JOIN cws_match c ON TRUE
            LEFT JOIN sdwis s ON TRUE
            LEFT JOIN best_rate b ON TRUE
            LEFT JOIN risk r ON TRUE
        """), {"lat": latitude, "lng": longitude}).mappings().first()

    if not row or not row["pwsid"]:
        return {"error": f"No water utility found at ({latitude}, {longitude})"}

    result = {
        "pwsid": row["pwsid"],
        "utility_name": row["pws_name"],
        "state": row["state_code"],
        "county": row["county_served"],
        "population_served": row["population_served"],
        "owner_type": row["owner_type_code"],
        "water_source": row["primary_source_code"],
        "violations_5yr": row["violation_count_5yr"],
    }

    if row["bill_estimate_10ccf"]:
        result["rate_data"] = {
            "bill_10ccf": float(row["bill_estimate_10ccf"]),
            "fixed_charge_monthly": float(row["fixed_charge_monthly"]) if row["fixed_charge_monthly"] else None,
            "rate_structure": row["rate_structure_type"],
            "confidence": row["confidence"],
            "source": row["selected_source"],
            "effective_date": str(row["rate_effective_date"]) if row["rate_effective_date"] else None,
        }
    else:
        result["rate_data"] = None

    if row["water_stress"]:
        result["water_risk"] = {
            "water_stress": row["water_stress"],
            "water_depletion": float(row["water_depletion"]) if row["water_depletion"] else None,
            "drought_risk": float(row["drought_risk"]) if row["drought_risk"] else None,
        }
    else:
        result["water_risk"] = None

    return result


@mcp.tool()
def get_utility_details(pwsid: str) -> dict:
    """Given a PWSID, return full utility details including rate schedule,
    rate structure type, and SDWIS metadata.

    Parameters
    ----------
    pwsid : str
        EPA Public Water System ID (e.g., VA4760100, CA1910001).

    Returns
    -------
    dict
        Full utility details: identity, SDWIS attributes, rate tiers,
        bill benchmarks, and provenance. Returns {"error": "..."} if not found.
    """
    pwsid = pwsid.upper().strip()

    with engine.connect() as conn:
        # Get SDWIS + CWS basics
        meta = conn.execute(text(f"""
            SELECT
                s.pwsid, s.pws_name, s.state_code, s.population_served_count,
                s.owner_type_code, s.primary_source_code, s.pws_type_code,
                s.violation_count_5yr, s.health_violation_count_5yr,
                c.county_served
            FROM {SCHEMA}.sdwis_systems s
            LEFT JOIN {SCHEMA}.cws_boundaries c ON c.pwsid = s.pwsid
            WHERE s.pwsid = :pwsid
        """), {"pwsid": pwsid}).mappings().first()

        if not meta:
            return {"error": f"PWSID {pwsid} not found in SDWIS database"}

        # Get rate schedule (canonical)
        rate = conn.execute(text(f"""
            SELECT
                vintage_date, rate_structure_type, customer_class,
                billing_frequency, fixed_charges, volumetric_tiers,
                surcharges, bill_5ccf, bill_10ccf, bill_20ccf,
                conservation_signal, tier_count,
                source_key, source_url, confidence, parse_model
            FROM {SCHEMA}.rate_schedules
            WHERE pwsid = :pwsid
            AND confidence IN ('high', 'medium')
            ORDER BY vintage_date DESC NULLS LAST
            LIMIT 1
        """), {"pwsid": pwsid}).mappings().first()

        # Get best estimate
        best = conn.execute(text(f"""
            SELECT selected_source, bill_estimate_10ccf, confidence,
                   rate_structure_type, rate_effective_date
            FROM {SCHEMA}.rate_best_estimate
            WHERE pwsid = :pwsid
        """), {"pwsid": pwsid}).mappings().first()

    result = {
        "pwsid": meta["pwsid"],
        "utility_name": meta["pws_name"],
        "state": meta["state_code"],
        "county": meta["county_served"],
        "population_served": meta["population_served_count"],
        "system_type": meta["pws_type_code"],
        "owner_type": meta["owner_type_code"],
        "water_source": meta["primary_source_code"],
        "violations_5yr": meta["violation_count_5yr"],
        "health_violations_5yr": meta["health_violation_count_5yr"],
    }

    if rate:
        result["rate_schedule"] = {
            "effective_date": str(rate["vintage_date"]) if rate["vintage_date"] else None,
            "rate_structure": rate["rate_structure_type"],
            "customer_class": rate["customer_class"],
            "billing_frequency": rate["billing_frequency"],
            "fixed_charges": rate["fixed_charges"],
            "volumetric_tiers": rate["volumetric_tiers"],
            "surcharges": rate["surcharges"],
            "bill_5ccf": float(rate["bill_5ccf"]) if rate["bill_5ccf"] else None,
            "bill_10ccf": float(rate["bill_10ccf"]) if rate["bill_10ccf"] else None,
            "bill_20ccf": float(rate["bill_20ccf"]) if rate["bill_20ccf"] else None,
            "conservation_signal": rate["conservation_signal"],
            "tier_count": rate["tier_count"],
            "source": rate["source_key"],
            "source_url": rate["source_url"],
            "confidence": rate["confidence"],
        }
    else:
        result["rate_schedule"] = None

    if best:
        result["best_estimate"] = {
            "bill_10ccf": float(best["bill_estimate_10ccf"]) if best["bill_estimate_10ccf"] else None,
            "source": best["selected_source"],
            "confidence": best["confidence"],
            "rate_structure": best["rate_structure_type"],
            "effective_date": str(best["rate_effective_date"]) if best["rate_effective_date"] else None,
        }
    else:
        result["best_estimate"] = None

    return result


def main():
    """Run the MCP server via stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

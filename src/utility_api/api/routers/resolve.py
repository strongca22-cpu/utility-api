#!/usr/bin/env python3
"""
/resolve Endpoint

Purpose:
    Given a lat/lng coordinate, resolve the enclosing water utility (CWS),
    enriched with SDWIS attributes, MDWD financial data, and Aqueduct
    water risk scores. Single PostGIS spatial query.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - fastapi
    - sqlalchemy
    - geoalchemy2

Usage:
    GET /resolve?lat=38.8951&lng=-77.0364
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.api.schemas import ResolveResponse
from utility_api.config import settings

router = APIRouter(tags=["resolve"])

# ANCHOR: resolve_spatial_query
RESOLVE_QUERY = text("""
WITH cws_match AS (
    SELECT
        c.pwsid,
        c.pws_name,
        c.state_code,
        c.county_served,
        c.population_served,
        c.source_type
    FROM utility.cws_boundaries c
    WHERE ST_Contains(c.geom, ST_SetSRID(ST_Point(:lng, :lat), 4326))
    LIMIT 1
),
aqueduct_match AS (
    SELECT
        a.string_id AS aqueduct_id,
        a.bws_score AS water_stress_score,
        a.bws_label AS water_stress_label,
        a.bwd_score AS water_depletion_score,
        a.drr_score AS drought_risk_score,
        a.iav_score AS interannual_variability,
        a.sev_score AS seasonal_variability,
        a.w_awr_def_tot_score AS overall_water_risk
    FROM utility.aqueduct_polygons a
    WHERE ST_Contains(a.geom, ST_SetSRID(ST_Point(:lng, :lat), 4326))
    LIMIT 1
),
sdwis_match AS (
    SELECT
        s.pws_name AS sdwis_pws_name,
        s.pws_type_code AS system_type,
        s.primary_source_code AS water_source,
        s.owner_type_code AS owner_type,
        s.service_connections_count AS service_connections,
        s.is_wholesaler_ind AS is_wholesaler,
        s.activity_status_cd AS activity_status,
        s.violation_count_5yr,
        s.health_violation_count_5yr,
        s.last_violation_date
    FROM utility.sdwis_systems s
    WHERE s.pwsid = (SELECT pwsid FROM cws_match)
),
mdwd_match AS (
    SELECT
        m.year AS mdwd_year,
        m.median_household_income,
        m.pct_below_poverty,
        m.water_utility_revenue,
        m.water_utility_expenditure,
        m.water_utility_debt,
        m.population AS mdwd_population
    FROM utility.mdwd_financials m
    WHERE m.pwsid = (SELECT pwsid FROM cws_match)
    ORDER BY m.year DESC
    LIMIT 1
),
rate_match AS (
    SELECT TRUE AS has_rate_data
    FROM utility.water_rates r
    WHERE r.pwsid = (SELECT pwsid FROM cws_match)
    LIMIT 1
)
SELECT
    -- CWS
    c.pwsid, c.pws_name, c.state_code, c.county_served, c.population_served,
    -- SDWIS
    s.system_type, s.water_source, s.owner_type, s.service_connections,
    s.is_wholesaler, s.activity_status,
    s.violation_count_5yr, s.health_violation_count_5yr, s.last_violation_date,
    -- MDWD
    m.mdwd_year, m.median_household_income, m.pct_below_poverty,
    m.water_utility_revenue, m.water_utility_expenditure,
    m.water_utility_debt, m.mdwd_population,
    -- Rate data
    COALESCE(r.has_rate_data, FALSE) AS has_rate_data,
    -- Aqueduct
    a.aqueduct_id, a.water_stress_score, a.water_stress_label,
    a.water_depletion_score, a.drought_risk_score,
    a.interannual_variability, a.seasonal_variability, a.overall_water_risk
FROM
    (SELECT 1) AS dummy
    LEFT JOIN cws_match c ON TRUE
    LEFT JOIN sdwis_match s ON TRUE
    LEFT JOIN mdwd_match m ON TRUE
    LEFT JOIN rate_match r ON TRUE
    LEFT JOIN aqueduct_match a ON TRUE
""")


@router.get("/resolve", response_model=ResolveResponse)
def resolve(
    lat: float = Query(..., ge=-90, le=90, description="Latitude (WGS84)"),
    lng: float = Query(..., ge=-180, le=180, description="Longitude (WGS84)"),
    db: Session = Depends(get_db),
):
    """Resolve a geographic coordinate to water utility and risk context.

    Returns the enclosing Community Water System (CWS) with SDWIS attributes,
    MDWD financial data, and Aqueduct water risk scores.
    """
    result = db.execute(RESOLVE_QUERY, {"lat": lat, "lng": lng}).mappings().first()

    if result is None:
        raise HTTPException(status_code=500, detail="Query returned no result")

    # Build response
    cws_match = result["pwsid"] is not None
    aqueduct_match = result["aqueduct_id"] is not None
    mdwd_available = result["mdwd_year"] is not None

    return ResolveResponse(
        lat=lat,
        lng=lng,
        cws_match=cws_match,
        aqueduct_match=aqueduct_match,
        # CWS
        pwsid=result["pwsid"],
        pws_name=result["pws_name"],
        state_code=result["state_code"],
        county_served=result["county_served"],
        population_served=result["population_served"],
        # SDWIS
        system_type=result["system_type"],
        water_source=result["water_source"],
        owner_type=result["owner_type"],
        service_connections=result["service_connections"],
        is_wholesaler=result["is_wholesaler"],
        activity_status=result["activity_status"],
        violation_count_5yr=result["violation_count_5yr"],
        health_violation_count_5yr=result["health_violation_count_5yr"],
        last_violation_date=result["last_violation_date"],
        # MDWD
        mdwd_available=mdwd_available,
        mdwd_year=result["mdwd_year"],
        has_rate_data=result["has_rate_data"],
        median_household_income=result["median_household_income"],
        pct_below_poverty=result["pct_below_poverty"],
        water_utility_revenue=result["water_utility_revenue"],
        water_utility_expenditure=result["water_utility_expenditure"],
        water_utility_debt=result["water_utility_debt"],
        mdwd_population=result["mdwd_population"],
        # Aqueduct
        aqueduct_id=result["aqueduct_id"],
        water_stress_score=result["water_stress_score"],
        water_stress_label=result["water_stress_label"],
        water_depletion_score=result["water_depletion_score"],
        drought_risk_score=result["drought_risk_score"],
        interannual_variability=result["interannual_variability"],
        seasonal_variability=result["seasonal_variability"],
        overall_water_risk=result["overall_water_risk"],
    )

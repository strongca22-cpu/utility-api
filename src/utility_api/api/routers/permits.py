#!/usr/bin/env python3
"""
/permits Endpoint

Purpose:
    Spatial radius query for state regulatory permits. Given lat/lng and
    a search radius, returns all permits within that distance, ordered
    by proximity. Supports filtering by category_group and source.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - fastapi
    - sqlalchemy
    - geoalchemy2

Usage:
    GET /permits?lat=38.8951&lng=-77.0364&radius_km=10
    GET /permits?lat=38.8951&lng=-77.0364&radius_km=10&category_group=industrial
    GET /permits?lat=38.8951&lng=-77.0364&radius_km=10&source=va_deq_vwp
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.api.schemas import PermitRecord, PermitsResponse
from utility_api.config import settings

router = APIRouter(tags=["permits"])

# Base spatial query — ST_DWithin uses meters on geography type,
# so we cast to geography for accurate distance calculation.
# Results ordered by distance ascending.
PERMITS_QUERY_TEMPLATE = """
SELECT
    source,
    permit_number,
    facility_name,
    source_category,
    category_group,
    use_codes,
    status,
    state_code,
    county,
    issued_date,
    expiration_date,
    face_value_amount,
    face_value_units,
    max_diversion_rate,
    max_diversion_units,
    max_diversion_rate_gpd,
    ST_Y(geom) AS lat,
    ST_X(geom) AS lng,
    ST_Distance(
        geom::geography,
        ST_SetSRID(ST_Point(:lng, :lat), 4326)::geography
    ) / 1000.0 AS distance_km
FROM {schema}.permits
WHERE geom IS NOT NULL
  AND ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_Point(:lng, :lat), 4326)::geography,
        :radius_m
  )
  {filters}
ORDER BY distance_km ASC
LIMIT :max_results
"""


@router.get("/permits", response_model=PermitsResponse)
def permits(
    lat: float = Query(..., ge=-90, le=90, description="Latitude (WGS84)"),
    lng: float = Query(..., ge=-180, le=180, description="Longitude (WGS84)"),
    radius_km: float = Query(10.0, ge=0.1, le=200, description="Search radius in km"),
    category_group: str | None = Query(
        None,
        description="Filter by category group (industrial, energy, municipal, etc.)",
    ),
    source: str | None = Query(
        None,
        description="Filter by data source (va_deq_vwp, va_deq_vpdes, ca_swrcb_ewrims)",
    ),
    max_results: int = Query(200, ge=1, le=1000, description="Maximum results to return"),
    db: Session = Depends(get_db),
):
    """Find permits within a radius of a geographic point.

    Returns all state regulatory permits within the specified radius,
    ordered by distance from the query point. Supports filtering by
    category_group and data source.
    """
    # Build dynamic filter clauses
    filters = []
    params = {
        "lat": lat,
        "lng": lng,
        "radius_m": radius_km * 1000,
        "max_results": max_results,
    }

    if category_group:
        filters.append("AND category_group = :category_group")
        params["category_group"] = category_group

    if source:
        filters.append("AND source = :source")
        params["source"] = source

    filter_clause = " ".join(filters)
    query = text(
        PERMITS_QUERY_TEMPLATE.format(
            schema=settings.utility_schema,
            filters=filter_clause,
        )
    )

    rows = db.execute(query, params).mappings().all()

    permit_records = []
    for row in rows:
        permit_records.append(PermitRecord(
            source=row["source"],
            permit_number=row["permit_number"],
            facility_name=row["facility_name"],
            source_category=row["source_category"],
            category_group=row["category_group"],
            use_codes=row["use_codes"],
            status=row["status"],
            state_code=row["state_code"],
            county=row["county"],
            issued_date=row["issued_date"],
            expiration_date=row["expiration_date"],
            face_value_amount=row["face_value_amount"],
            face_value_units=row["face_value_units"],
            max_diversion_rate=row["max_diversion_rate"],
            max_diversion_units=row["max_diversion_units"],
            max_diversion_rate_gpd=row["max_diversion_rate_gpd"],
            lat=row["lat"],
            lng=row["lng"],
            distance_km=round(row["distance_km"], 3),
        ))

    return PermitsResponse(
        query_lat=lat,
        query_lng=lng,
        radius_km=radius_km,
        total_results=len(permit_records),
        permits=permit_records,
    )

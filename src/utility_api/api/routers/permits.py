#!/usr/bin/env python3
"""
Permits Endpoints

Purpose:
    Spatial radius query for state regulatory permits, plus facility-linked
    permit lookup. Supports filtering by category_group and source.

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
    GET /facility/{facility_id}/permits
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.api.schemas import (
    FacilityPermitsResponse,
    PermitRecord,
    PermitXrefRecord,
    PermitsResponse,
)
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


# --- Facility-linked permit lookup ---

FACILITY_PERMITS_QUERY = """
SELECT
    p.source,
    p.permit_number,
    p.facility_name,
    p.source_category,
    p.category_group,
    p.use_codes,
    p.status,
    p.state_code,
    p.county,
    p.issued_date,
    p.expiration_date,
    p.face_value_amount,
    p.face_value_units,
    p.max_diversion_rate,
    p.max_diversion_units,
    p.max_diversion_rate_gpd,
    ST_Y(p.geom) AS lat,
    ST_X(p.geom) AS lng,
    x.match_type,
    x.match_distance_km,
    x.match_confidence
FROM {schema}.permit_facility_xref x
JOIN {schema}.permits p ON p.id = x.permit_id
WHERE x.facility_id = :facility_id
ORDER BY x.match_distance_km ASC NULLS LAST
"""

FACILITY_NEARBY_QUERY = """
SELECT
    p.source,
    p.permit_number,
    p.facility_name,
    p.source_category,
    p.category_group,
    p.use_codes,
    p.status,
    p.state_code,
    p.county,
    p.issued_date,
    p.expiration_date,
    p.face_value_amount,
    p.face_value_units,
    p.max_diversion_rate,
    p.max_diversion_units,
    p.max_diversion_rate_gpd,
    ST_Y(p.geom) AS lat,
    ST_X(p.geom) AS lng,
    ST_Distance(
        p.geom::geography,
        f.geom::geography
    ) / 1000.0 AS distance_km
FROM public.facilities f
JOIN {schema}.permits p
  ON p.geom IS NOT NULL
  AND ST_DWithin(p.geom::geography, f.geom::geography, :radius_m)
WHERE f.facility_id = :facility_id
  {filters}
ORDER BY distance_km ASC
LIMIT :max_results
"""


@router.get("/facility/{facility_id}/permits", response_model=FacilityPermitsResponse)
def facility_permits(
    facility_id: str = Path(..., description="SS facility ID (e.g., SS-US-VA-0001)"),
    radius_km: float = Query(10.0, ge=0.1, le=200, description="Radius for nearby permits (beyond linked)"),
    category_group: str | None = Query(None, description="Filter nearby permits by category group"),
    include_nearby: bool = Query(True, description="Include nearby unlinked permits"),
    max_results: int = Query(100, ge=1, le=500, description="Max nearby permits"),
    db: Session = Depends(get_db),
):
    """Get permits linked to a facility, plus nearby permits.

    Returns two sets:
    1. **Linked permits** — directly cross-referenced via permit_facility_xref
       (matched by spatial proximity to known DC permits)
    2. **Nearby permits** — all permits within radius_km of the facility
       (includes discharge, stormwater, wetland, etc.)

    The linked permits include match metadata (distance, confidence).
    """
    schema = settings.utility_schema

    # Check facility exists
    fac_check = db.execute(
        text("SELECT facility_id, name, operator_name, region FROM public.facilities WHERE facility_id = :fid"),
        {"fid": facility_id},
    ).mappings().first()

    if fac_check is None:
        raise HTTPException(status_code=404, detail=f"Facility {facility_id} not found")

    # 1. Linked permits from xref table
    linked_rows = db.execute(
        text(FACILITY_PERMITS_QUERY.format(schema=schema)),
        {"facility_id": facility_id},
    ).mappings().all()

    linked_permits = []
    for row in linked_rows:
        linked_permits.append(PermitXrefRecord(
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
            match_type=row["match_type"],
            match_distance_km=round(row["match_distance_km"], 3) if row["match_distance_km"] else None,
            match_confidence=row["match_confidence"],
        ))

    # 2. Nearby permits (spatial radius from facility)
    nearby_permits = []
    if include_nearby:
        filters = []
        params = {
            "facility_id": facility_id,
            "radius_m": radius_km * 1000,
            "max_results": max_results,
        }
        if category_group:
            filters.append("AND p.category_group = :category_group")
            params["category_group"] = category_group

        filter_clause = " ".join(filters)
        nearby_rows = db.execute(
            text(FACILITY_NEARBY_QUERY.format(schema=schema, filters=filter_clause)),
            params,
        ).mappings().all()

        # Exclude permits already in linked set
        linked_permit_nums = {p.permit_number for p in linked_permits}
        for row in nearby_rows:
            if row["permit_number"] in linked_permit_nums:
                continue
            nearby_permits.append(PermitRecord(
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

    return FacilityPermitsResponse(
        facility_id=facility_id,
        facility_name=fac_check["name"],
        operator=fac_check["operator_name"],
        state_code=fac_check["region"],
        linked_permits=linked_permits,
        nearby_permits=nearby_permits,
        radius_km=radius_km,
    )

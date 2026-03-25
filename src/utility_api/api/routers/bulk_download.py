#!/usr/bin/env python3
"""
/bulk-download Endpoint

Purpose:
    Dataset export for Product A customers. Returns rate_best_estimate
    joined with CWS boundary centroids and SDWIS metadata as CSV or
    GeoJSON. One row per PWSID.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - fastapi
    - sqlalchemy
    - geoalchemy2

Usage:
    GET /bulk-download?format=csv
    GET /bulk-download?state=VA&format=csv
    GET /bulk-download?format=geojson

Notes:
    - Rate-limited more aggressively (1/hour per key) via header hint
    - CSV streams as text/csv with Content-Disposition attachment
    - GeoJSON streams as application/geo+json
"""

import csv
import io
import json

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.config import settings

router = APIRouter(tags=["bulk-download"])

SCHEMA = settings.utility_schema

BULK_QUERY = text(f"""
    SELECT
        be.pwsid,
        COALESCE(s.pws_name, be.utility_name) AS utility_name,
        be.state_code,
        c.county_served AS county,
        s.population_served_count AS population,
        be.rate_structure_type,
        be.bill_5ccf,
        be.bill_10ccf,
        be.bill_estimate_10ccf AS bill_20ccf,
        be.fixed_charge_monthly,
        be.confidence,
        be.selected_source AS source,
        be.rate_effective_date AS last_verified,
        ST_Y(ST_Centroid(c.geom)) AS lat,
        ST_X(ST_Centroid(c.geom)) AS lng
    FROM {SCHEMA}.rate_best_estimate be
    LEFT JOIN {SCHEMA}.cws_boundaries c ON c.pwsid = be.pwsid
    LEFT JOIN {SCHEMA}.sdwis_systems s ON s.pwsid = be.pwsid
    WHERE (:state IS NULL OR be.state_code = :state)
    ORDER BY be.state_code, s.population_served_count DESC NULLS LAST
""")

CSV_COLUMNS = [
    "pwsid", "utility_name", "state_code", "county", "population",
    "rate_structure_type", "bill_5ccf", "bill_10ccf", "bill_20ccf",
    "fixed_charge_monthly", "confidence", "source", "last_verified",
    "lat", "lng",
]


@router.get("/bulk-download")
def bulk_download(
    state: str | None = Query(None, description="Filter by state code (e.g., VA)"),
    format: str = Query("csv", description="Output format: csv or geojson"),
    db: Session = Depends(get_db),
):
    """Download the full rate dataset as CSV or GeoJSON.

    Returns one row per PWSID with: utility identity, rate structure,
    bill benchmarks (5/10/20 CCF), confidence, source, and centroid
    coordinates.

    **Intended for bulk data consumers (Product A).** Rate-limited to
    1 download per hour per API key.

    Parameters
    ----------
    state : str, optional
        Filter to a single state code.
    format : str
        Output format: "csv" or "geojson". Default: csv.
    """
    params = {"state": state.upper() if state else None}
    rows = db.execute(BULK_QUERY, params).mappings().all()

    if format.lower() == "geojson":
        return _stream_geojson(rows, state)
    else:
        return _stream_csv(rows, state)


def _stream_csv(rows, state: str | None) -> StreamingResponse:
    """Stream results as CSV with Content-Disposition header."""
    filename = f"utility_rates_{state.lower() if state else 'national'}.csv"

    def generate():
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        yield output.getvalue()
        output.truncate(0)
        output.seek(0)

        for row in rows:
            writer.writerow({
                "pwsid": row["pwsid"],
                "utility_name": row["utility_name"],
                "state_code": row["state_code"],
                "county": row["county"],
                "population": row["population"],
                "rate_structure_type": row["rate_structure_type"],
                "bill_5ccf": row["bill_5ccf"],
                "bill_10ccf": row["bill_10ccf"],
                "bill_20ccf": row["bill_20ccf"],
                "fixed_charge_monthly": row["fixed_charge_monthly"],
                "confidence": row["confidence"],
                "source": row["source"],
                "last_verified": str(row["last_verified"]) if row["last_verified"] else None,
                "lat": row["lat"],
                "lng": row["lng"],
            })
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _stream_geojson(rows, state: str | None) -> StreamingResponse:
    """Stream results as GeoJSON FeatureCollection."""
    features = []
    for row in rows:
        lat = row["lat"]
        lng = row["lng"]
        geometry = None
        if lat is not None and lng is not None:
            geometry = {"type": "Point", "coordinates": [lng, lat]}

        properties = {
            "pwsid": row["pwsid"],
            "utility_name": row["utility_name"],
            "state_code": row["state_code"],
            "county": row["county"],
            "population": row["population"],
            "rate_structure_type": row["rate_structure_type"],
            "bill_5ccf": float(row["bill_5ccf"]) if row["bill_5ccf"] else None,
            "bill_10ccf": float(row["bill_10ccf"]) if row["bill_10ccf"] else None,
            "bill_20ccf": float(row["bill_20ccf"]) if row["bill_20ccf"] else None,
            "fixed_charge_monthly": float(row["fixed_charge_monthly"]) if row["fixed_charge_monthly"] else None,
            "confidence": row["confidence"],
            "source": row["source"],
            "last_verified": str(row["last_verified"]) if row["last_verified"] else None,
        }

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": properties,
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    content = json.dumps(geojson, indent=2, default=str)
    filename = f"utility_rates_{state.lower() if state else 'national'}.geojson"

    return StreamingResponse(
        iter([content]),
        media_type="application/geo+json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

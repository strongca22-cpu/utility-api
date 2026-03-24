#!/usr/bin/env python3
"""
VA DEQ Permit Ingest

Purpose:
    Download and load Virginia DEQ permit layers from the EDMA ArcGIS
    MapServer into the utility.permits table. Covers three layers:
      - VWP Individual Permits (Layer 192): water protection, incl. data centers
      - VWP General Permits (Layer 193): smaller-scale wetland/stream impacts
      - VPDES Outfalls (Layer 119): discharge permits

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests
    - pyyaml
    - sqlalchemy

Usage:
    ua-ingest va-deq

Notes:
    - ArcGIS MapServer with max 1000 records per request
    - Coordinates served in Web Mercator (EPSG:3857), reprojected to 4326
    - Truncate-and-reload per source (idempotent)
    - No volume data exposed by DEQ GIS layers
    - VWP_ACTIVITY_TYPE "Residential" excluded from IP ingest
    - VWP GP: Linear Transportation excluded; only Industrial, Commercial,
      Municipal, Mining, Agricultural, Other retained
    - Raw attributes stored in raw_attrs JSONB column

Data Sources:
    - Input: https://gisdata.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer
    - Output: utility.permits table
"""

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# EDMA MapServer base URL
EDMA_BASE = "https://gisdata.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer"

# Layer IDs
VWP_IP_LAYER = 192   # VWP Individual Permits
VWP_GP_LAYER = 193   # VWP General Permits
VPDES_LAYER = 119    # VPDES Permit Outfalls

PAGE_SIZE = 1000     # ArcGIS MapServer max
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "va_deq"

# VWP GP activity codes to include (exclude transportation, residential)
VWP_GP_INCLUDE = {"Industrial", "Commercial", "Municipal", "Mining", "Agricultural", "Other"}

# VWP IP activity types to exclude
VWP_IP_EXCLUDE = {"Residential"}


def _load_category_map() -> dict[str, dict[str, str]]:
    """Load category mapping from YAML config.

    Returns
    -------
    dict[str, dict[str, str]]
        Mapping sections from category_mapping.yaml.
    """
    config_path = PROJECT_ROOT / "config" / "category_mapping.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def _reproject_3857_to_4326(x: float, y: float) -> tuple[float, float]:
    """Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326).

    Parameters
    ----------
    x : float
        Easting in meters (Web Mercator).
    y : float
        Northing in meters (Web Mercator).

    Returns
    -------
    tuple[float, float]
        (longitude, latitude) in EPSG:4326.
    """
    lng = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lng, lat


def _query_layer(layer_id: int, where: str = "1=1", out_sr: int = 4326) -> list[dict]:
    """Paginated query of an EDMA MapServer layer.

    Parameters
    ----------
    layer_id : int
        ArcGIS MapServer layer ID.
    where : str
        SQL WHERE clause for filtering.
    out_sr : int
        Output spatial reference (default 4326).

    Returns
    -------
    list[dict]
        All feature records with attributes and geometry.
    """
    url = f"{EDMA_BASE}/{layer_id}/query"
    all_features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "outSR": out_sr,
            "f": "json",
            "resultRecordCount": PAGE_SIZE,
            "resultOffset": offset,
        }

        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=120)
                r.raise_for_status()
                data = r.json()

                if "error" in data:
                    raise ValueError(f"ArcGIS error: {data['error']}")

                features = data.get("features", [])
                all_features.extend(features)

                logger.debug(
                    f"Layer {layer_id}: fetched {len(features)} features "
                    f"(offset={offset}, total so far={len(all_features)})"
                )
                break

            except Exception as e:
                if attempt < 2:
                    wait = 5 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

        # Check if more pages exist
        if data.get("exceededTransferLimit", False) and len(features) == PAGE_SIZE:
            offset += PAGE_SIZE
            time.sleep(0.5)  # Polite pause
        else:
            break

    return all_features


def _cache_features(features: list[dict], cache_name: str) -> Path:
    """Cache raw features to JSON file.

    Parameters
    ----------
    features : list[dict]
        Feature records from ArcGIS query.
    cache_name : str
        Name for the cache file (without extension).

    Returns
    -------
    Path
        Path to the cache file.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / f"{cache_name}.json"
    with open(cache_path, "w") as f:
        json.dump(features, f)
    logger.info(f"Cached {len(features)} features to {cache_path.name}")
    return cache_path


def _build_vwp_ip_rows(
    features: list[dict], category_map: dict[str, str]
) -> list[dict]:
    """Convert VWP Individual Permit features to permit table rows.

    Parameters
    ----------
    features : list[dict]
        Raw ArcGIS features from Layer 192.
    category_map : dict[str, str]
        VWP_ACTIVITY_TYPE → category_group mapping.

    Returns
    -------
    list[dict]
        Rows ready for database insert.
    """
    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        activity_type = attrs.get("VWP_ACTIVITY_TYPE", "")

        # Skip excluded activity types
        if activity_type in VWP_IP_EXCLUDE:
            continue

        # Get coordinates (already in 4326 from outSR param)
        lng = geom.get("x")
        lat = geom.get("y")

        # Clean classification to status
        classification = attrs.get("VWP_CLASSIFICATION", "")
        status = classification.lower() if classification else None

        rows.append({
            "source": "va_deq_vwp",
            "permit_number": attrs.get("PERMIT_NO", ""),
            "facility_name": attrs.get("FAC_NAME"),
            "source_category": activity_type or None,
            "category_group": category_map.get(activity_type, "other"),
            "use_codes": None,
            "status": status,
            "state_code": "VA",
            "county": attrs.get("FIC_DESCRIPTION"),
            "issued_date": None,
            "expiration_date": None,
            "face_value_amount": None,
            "face_value_units": None,
            "max_diversion_rate": None,
            "max_diversion_units": None,
            "geom_wkt": f"SRID=4326;POINT({lng} {lat})" if lng and lat else None,
            "raw_attrs": attrs,
        })

    return rows


def _build_vwp_gp_rows(
    features: list[dict], category_map: dict[str, str]
) -> list[dict]:
    """Convert VWP General Permit features to permit table rows.

    Parameters
    ----------
    features : list[dict]
        Raw ArcGIS features from Layer 193.
    category_map : dict[str, str]
        VWT_ACTIV_CODE → category_group mapping.

    Returns
    -------
    list[dict]
        Rows ready for database insert.
    """
    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        activity_code = attrs.get("VWT_ACTIV_CODE", "")

        # Only include targeted activity codes
        if activity_code not in VWP_GP_INCLUDE:
            continue

        lng = geom.get("x")
        lat = geom.get("y")

        classification = attrs.get("VWP_GP_CLASSIFICATION", "")
        status = classification.lower() if classification else None

        rows.append({
            "source": "va_deq_vwp_gp",
            "permit_number": attrs.get("PERMIT_NO", ""),
            "facility_name": attrs.get("FAC_NAME"),
            "source_category": activity_code or None,
            "category_group": category_map.get(activity_code, "other"),
            "use_codes": None,
            "status": status,
            "state_code": "VA",
            "county": attrs.get("FIC_DESCRIPTION"),
            "issued_date": None,
            "expiration_date": None,
            "face_value_amount": None,
            "face_value_units": None,
            "max_diversion_rate": None,
            "max_diversion_units": None,
            "geom_wkt": f"SRID=4326;POINT({lng} {lat})" if lng and lat else None,
            "raw_attrs": attrs,
        })

    return rows


def _build_vpdes_rows(
    features: list[dict], category_map: dict[str, str]
) -> list[dict]:
    """Convert VPDES Outfall features to permit table rows.

    Parameters
    ----------
    features : list[dict]
        Raw ArcGIS features from Layer 119.
    category_map : dict[str, str]
        VAP_TYPE → category_group mapping.

    Returns
    -------
    list[dict]
        Rows ready for database insert.
    """
    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        vap_type = attrs.get("VAP_TYPE", "")

        lng = geom.get("x")
        lat = geom.get("y")

        rows.append({
            "source": "va_deq_vpdes",
            "permit_number": attrs.get("VAP_PMT_NO", ""),
            "facility_name": attrs.get("FAC_NAME"),
            "source_category": vap_type or None,
            "category_group": category_map.get(vap_type, "other"),
            "use_codes": None,
            "status": None,  # VPDES layer doesn't expose status
            "state_code": "VA",
            "county": None,  # VPDES layer doesn't expose county
            "issued_date": None,
            "expiration_date": None,
            "face_value_amount": None,
            "face_value_units": None,
            "max_diversion_rate": None,
            "max_diversion_units": None,
            "geom_wkt": f"SRID=4326;POINT({lng} {lat})" if lng and lat else None,
            "raw_attrs": attrs,
        })

    return rows


def _insert_rows(rows: list[dict]) -> int:
    """Insert permit rows into the database.

    Parameters
    ----------
    rows : list[dict]
        Rows with geom_wkt field for geometry.

    Returns
    -------
    int
        Number of rows inserted.
    """
    if not rows:
        return 0

    schema = settings.utility_schema
    insert_sql = text(f"""
        INSERT INTO {schema}.permits (
            source, permit_number, facility_name, source_category,
            category_group, use_codes, status, state_code, county,
            issued_date, expiration_date, face_value_amount, face_value_units,
            max_diversion_rate, max_diversion_units, geom, raw_attrs
        ) VALUES (
            :source, :permit_number, :facility_name, :source_category,
            :category_group, :use_codes, :status, :state_code, :county,
            :issued_date, :expiration_date, :face_value_amount, :face_value_units,
            :max_diversion_rate, :max_diversion_units,
            ST_GeomFromEWKT(CAST(:geom_wkt AS text)),
            :raw_attrs
        )
    """)

    with engine.connect() as conn:
        # Convert raw_attrs dicts to JSON strings for psycopg
        params = []
        for row in rows:
            p = dict(row)
            p["raw_attrs"] = json.dumps(p["raw_attrs"]) if p["raw_attrs"] else None
            p["use_codes"] = json.dumps(p["use_codes"]) if p["use_codes"] else None
            params.append(p)

        # Batch insert in chunks
        chunk_size = 500
        for i in range(0, len(params), chunk_size):
            chunk = params[i : i + chunk_size]
            for p in chunk:
                conn.execute(insert_sql, p)

        conn.commit()

    return len(rows)


def run_va_deq_ingest() -> None:
    """Download and load VA DEQ permits into PostGIS.

    Queries three EDMA MapServer layers (VWP IP, VWP GP, VPDES),
    maps categories, and loads into utility.permits. Truncate-and-reload
    for va_deq_* sources only (preserves CA data).
    """
    started = datetime.now(timezone.utc)
    logger.info("=== VA DEQ Permit Ingest Starting ===")

    # Load category mappings
    config = _load_category_map()
    vwp_ip_map = config.get("va_deq_vwp_activity_type", {})
    vwp_gp_map = config.get("va_deq_vwp_gp_activity_code", {})
    vpdes_map = config.get("va_deq_vpdes_type", {})

    # --- VWP Individual Permits ---
    logger.info("Fetching VWP Individual Permits (Layer 192)...")
    vwp_ip_features = _query_layer(VWP_IP_LAYER)
    _cache_features(vwp_ip_features, "vwp_individual_permits")
    vwp_ip_rows = _build_vwp_ip_rows(vwp_ip_features, vwp_ip_map)
    logger.info(f"VWP IP: {len(vwp_ip_features)} features → {len(vwp_ip_rows)} rows (after filtering)")

    # --- VWP General Permits ---
    logger.info("Fetching VWP General Permits (Layer 193)...")
    vwp_gp_features = _query_layer(VWP_GP_LAYER)
    _cache_features(vwp_gp_features, "vwp_general_permits")
    vwp_gp_rows = _build_vwp_gp_rows(vwp_gp_features, vwp_gp_map)
    logger.info(f"VWP GP: {len(vwp_gp_features)} features → {len(vwp_gp_rows)} rows (after filtering)")

    # --- VPDES Outfalls ---
    logger.info("Fetching VPDES Outfalls (Layer 119)...")
    vpdes_features = _query_layer(VPDES_LAYER)
    _cache_features(vpdes_features, "vpdes_outfalls")
    vpdes_rows = _build_vpdes_rows(vpdes_features, vpdes_map)
    logger.info(f"VPDES: {len(vpdes_features)} features → {len(vpdes_rows)} rows")

    # --- Truncate VA DEQ rows and reload ---
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(
            f"DELETE FROM {schema}.permits WHERE source LIKE 'va_deq_%'"
        ))
        conn.commit()
    logger.info("Cleared existing VA DEQ permit rows")

    # Insert all rows
    all_rows = vwp_ip_rows + vwp_gp_rows + vpdes_rows
    inserted = _insert_rows(all_rows)
    logger.info(f"Inserted {inserted} total VA DEQ permit rows")

    # Log pipeline run
    with engine.connect() as conn:
        conn.execute(
            text(
                f"INSERT INTO {schema}.pipeline_runs "
                f"(step_name, started_at, finished_at, row_count, status, notes) "
                f"VALUES (:step, :started, NOW(), :count, 'success', :notes)"
            ),
            {
                "step": "va_deq",
                "started": started,
                "count": inserted,
                "notes": (
                    f"VWP IP: {len(vwp_ip_rows)}, "
                    f"VWP GP: {len(vwp_gp_rows)}, "
                    f"VPDES: {len(vpdes_rows)}"
                ),
            },
        )
        conn.commit()

    logger.info(f"=== VA DEQ Ingest Complete: {inserted} permits loaded ===")


if __name__ == "__main__":
    run_va_deq_ingest()

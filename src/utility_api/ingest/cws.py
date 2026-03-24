#!/usr/bin/env python3
"""
EPA CWS Boundaries Ingest

Purpose:
    Download and load EPA Community Water System service area boundaries
    into the utility.cws_boundaries table. Uses the ArcGIS Feature Service
    REST API with paginated OID-range queries (~44K polygons).

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests
    - geopandas
    - sqlalchemy, geoalchemy2

Usage:
    ua-ingest cws

Notes:
    - Feature service max record count: 2,000 per page
    - Total: ~44,656 features (OBJECTID 1..44656)
    - Downloads as GeoJSON in EPSG:4326
    - Truncate-and-reload pattern (idempotent)
    - Raw GeoJSON pages cached in data/raw/epa_cws/

Data Sources:
    - Input: ArcGIS Feature Service (Water_System_Boundaries/FeatureServer/0)
    - Output: utility.cws_boundaries table
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
from loguru import logger
from shapely.geometry import MultiPolygon
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine

# ArcGIS Feature Service endpoint
FEATURE_SERVICE_URL = (
    "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/"
    "Water_System_Boundaries/FeatureServer/0/query"
)

PAGE_SIZE = 2000  # Max records per request
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "epa_cws"

# Field mapping: ArcGIS field name → DB column name
FIELD_MAP = {
    "PWSID": "pwsid",
    "PWS_Name": "pws_name",
    "Primacy_Agency": "state_code",
    "Population_Served_Count": "population_served",
    "Service_Area_Type": "source_type",
}


def _get_oid_range() -> tuple[int, int]:
    """Query the feature service for the OBJECTID range.

    Returns
    -------
    tuple[int, int]
        (min_oid, max_oid)
    """
    import requests

    params = {
        "where": "1=1",
        "returnIdsOnly": "true",
        "f": "json",
    }
    r = requests.get(FEATURE_SERVICE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    oids = data.get("objectIds", [])
    if not oids:
        raise ValueError("No OBJECTIDs returned from feature service")
    return min(oids), max(oids)


def _download_page(oid_start: int, oid_end: int, cache_dir: Path) -> Path:
    """Download one page of CWS boundaries as GeoJSON.

    Parameters
    ----------
    oid_start : int
        Start OBJECTID (exclusive).
    oid_end : int
        End OBJECTID (inclusive).
    cache_dir : Path
        Directory to cache downloaded pages.

    Returns
    -------
    Path
        Path to the cached GeoJSON file.
    """
    import requests

    cache_file = cache_dir / f"cws_{oid_start}_{oid_end}.geojson"
    if cache_file.exists():
        logger.debug(f"Using cached page: {cache_file.name}")
        return cache_file

    params = {
        "where": f"OBJECTID > {oid_start} AND OBJECTID <= {oid_end}",
        "outFields": ",".join(FIELD_MAP.keys()),
        "outSR": "4326",
        "f": "geojson",
    }

    for attempt in range(3):
        try:
            r = requests.get(FEATURE_SERVICE_URL, params=params, timeout=300)
            r.raise_for_status()
            data = r.json()

            # Check for ArcGIS error response
            if "error" in data:
                raise ValueError(f"ArcGIS error: {data['error']}")

            with open(cache_file, "w") as f:
                json.dump(data, f)

            feature_count = len(data.get("features", []))
            logger.debug(f"Downloaded {feature_count} features (OID {oid_start+1}-{oid_end})")
            return cache_file

        except Exception as e:
            if attempt < 2:
                wait = 5 * (attempt + 1)
                logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def _download_all_pages() -> list[Path]:
    """Download all CWS boundary pages from the feature service.

    Returns
    -------
    list[Path]
        List of cached GeoJSON file paths.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    min_oid, max_oid = _get_oid_range()
    logger.info(f"OBJECTID range: {min_oid}–{max_oid} ({max_oid - min_oid + 1} features)")

    pages = []
    oid_start = min_oid - 1  # Start before first OID for > comparison
    total_pages = (max_oid - min_oid) // PAGE_SIZE + 1

    for page_num in range(total_pages):
        oid_end = min(oid_start + PAGE_SIZE, max_oid)
        page_file = _download_page(oid_start, oid_end, RAW_DIR)
        pages.append(page_file)
        oid_start = oid_end

        if (page_num + 1) % 5 == 0:
            logger.info(f"Downloaded page {page_num + 1}/{total_pages}")

        # Brief pause to be polite to the server
        time.sleep(0.5)

        if oid_start >= max_oid:
            break

    logger.info(f"Downloaded {len(pages)} pages total")
    return pages


def _load_geojson_pages(pages: list[Path]) -> gpd.GeoDataFrame:
    """Load and merge all cached GeoJSON pages into a single GeoDataFrame.

    Parameters
    ----------
    pages : list[Path]
        List of GeoJSON file paths.

    Returns
    -------
    gpd.GeoDataFrame
        Merged CWS boundaries.
    """
    gdfs = []
    for page in pages:
        try:
            gdf = gpd.read_file(page)
            if len(gdf) > 0:
                gdfs.append(gdf)
        except Exception as e:
            logger.warning(f"Failed to read {page.name}: {e}")

    if not gdfs:
        raise ValueError("No valid GeoJSON pages loaded")

    merged = pd.concat(gdfs, ignore_index=True)
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    logger.info(f"Merged {len(merged)} CWS boundaries from {len(gdfs)} pages")
    return merged


def _prepare_for_postgis(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Rename columns and prepare geometry for PostGIS insert.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Raw merged CWS data.

    Returns
    -------
    gpd.GeoDataFrame
        Cleaned data with DB column names.
    """
    # Rename fields
    rename = {k: v for k, v in FIELD_MAP.items() if k in gdf.columns}
    out = gdf.rename(columns=rename)

    # Extract state code from Primacy_Agency (first 2 chars) if needed
    if "state_code" in out.columns:
        out["state_code"] = out["state_code"].str[:2]

    # Keep only the columns we need
    target_cols = list(FIELD_MAP.values()) + ["geometry"]
    available = [c for c in target_cols if c in out.columns]
    out = out[available].copy()

    # Add missing columns
    for col in FIELD_MAP.values():
        if col not in out.columns:
            out[col] = None

    # Convert population_served to integer (comes as float from GeoJSON)
    if "population_served" in out.columns:
        out["population_served"] = (
            pd.to_numeric(out["population_served"], errors="coerce")
            .round()
            .astype("Int64")
        )

    # Ensure MultiPolygon geometry
    out["geometry"] = out["geometry"].apply(
        lambda g: MultiPolygon([g]) if g is not None and g.geom_type == "Polygon" else g
    )

    # Drop rows without PWSID or geometry
    out = out.dropna(subset=["pwsid"])
    out = out[out.geometry.notna()]

    # Deduplicate on PWSID (keep first occurrence)
    out = out.drop_duplicates(subset=["pwsid"], keep="first")

    # Rename geometry to match DB column
    out = out.rename_geometry("geom")

    logger.info(f"Prepared {len(out)} CWS boundaries for PostGIS")
    return out


def run_cws_ingest() -> None:
    """Download and load EPA CWS boundaries into PostGIS.

    Downloads from ArcGIS Feature Service (paginated), caches raw GeoJSON,
    then loads into utility.cws_boundaries. Truncate-and-reload pattern.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== CWS Boundaries Ingest Starting ===")

    # Download
    pages = _download_all_pages()

    # Load and merge
    gdf = _load_geojson_pages(pages)

    # Prepare
    gdf = _prepare_for_postgis(gdf)

    # Truncate and load
    schema = settings.utility_schema
    table = "cws_boundaries"
    logger.info(f"Truncating {schema}.{table}")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} CASCADE"))
        conn.commit()

    logger.info(f"Loading {len(gdf)} boundaries into {schema}.{table}")
    gdf.to_postgis(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=500,
    )

    # Log completion
    with engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{table}")
        ).scalar()
        conn.execute(
            text(
                f"INSERT INTO {schema}.pipeline_runs "
                f"(step_name, started_at, finished_at, row_count, status) "
                f"VALUES (:step, :started, NOW(), :count, 'success')"
            ),
            {"step": "cws", "started": started, "count": count},
        )
        conn.commit()

    logger.info(f"=== CWS Ingest Complete: {count} boundaries loaded ===")


if __name__ == "__main__":
    run_cws_ingest()

#!/usr/bin/env python3
"""
Census TIGER County Boundaries Ingest

Purpose:
    Download and load Census TIGER/Line county boundary shapefiles into
    utility.county_boundaries. Then run a spatial join to backfill
    county_served on cws_boundaries where SDWIS geographic areas
    didn't provide county data.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests
    - geopandas
    - sqlalchemy, geoalchemy2

Usage:
    ua-ingest tiger-county

Notes:
    - Source: Census Bureau TIGER/Line 2024 county shapefiles
    - Covers all US counties, county equivalents, and independent cities
    - Native CRS is EPSG:4269 (NAD83), reprojected to 4326 (WGS84) on load
    - Spatial join uses ST_Intersects on CWS boundary centroids
    - Only backfills county_served where currently NULL (preserves SDWIS data)
    - Truncate-and-reload pattern (idempotent)

Data Sources:
    - Input: https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip
    - Output: utility.county_boundaries table
    - Side effect: updates utility.cws_boundaries.county_served (NULL rows only)
"""

from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
from loguru import logger
from shapely.geometry import MultiPolygon
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine

TIGER_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip"
)
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "tiger_county"


def _download_tiger_county() -> Path:
    """Download TIGER county shapefile ZIP if not cached.

    Returns
    -------
    Path
        Path to the downloaded/cached ZIP file.
    """
    import requests

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "tl_2024_us_county.zip"

    if zip_path.exists():
        logger.info(f"Using cached TIGER county ZIP: {zip_path}")
        return zip_path

    logger.info(f"Downloading TIGER 2024 county boundaries (~80MB)")
    r = requests.get(TIGER_URL, stream=True, timeout=300)
    r.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)

    logger.info(f"Downloaded: {zip_path} ({zip_path.stat().st_size / 1e6:.0f}MB)")
    return zip_path


def _prepare_counties(zip_path: Path) -> gpd.GeoDataFrame:
    """Load and prepare county boundaries for PostGIS.

    Parameters
    ----------
    zip_path : Path
        Path to the TIGER county ZIP file.

    Returns
    -------
    gpd.GeoDataFrame
        County boundaries in EPSG:4326, MultiPolygon geometry.
    """
    gdf = gpd.read_file(zip_path)
    logger.info(f"Loaded {len(gdf)} county boundaries from TIGER")

    # Reproject NAD83 → WGS84
    gdf = gdf.to_crs("EPSG:4326")

    # Select and rename columns
    gdf = gdf.rename(columns={
        "GEOID": "geoid",
        "STATEFP": "state_fips",
        "COUNTYFP": "county_fips",
        "NAME": "name",
        "NAMELSAD": "name_lsad",
        "CLASSFP": "class_fp",
        "ALAND": "aland",
        "AWATER": "awater",
    })

    target_cols = [
        "geoid", "state_fips", "county_fips", "name", "name_lsad",
        "class_fp", "aland", "awater", "geometry",
    ]
    gdf = gdf[target_cols].copy()

    # Ensure MultiPolygon
    gdf["geometry"] = gdf["geometry"].apply(
        lambda g: MultiPolygon([g]) if g is not None and g.geom_type == "Polygon" else g
    )

    # Rename geometry column to match DB
    gdf = gdf.rename_geometry("geom")

    logger.info(f"Prepared {len(gdf)} county boundaries for PostGIS")
    return gdf


def _spatial_join_counties() -> int:
    """Backfill county_served on CWS boundaries using spatial join.

    Uses ST_Intersects between CWS boundary centroids and county polygons
    to fill county_served where it is currently NULL.

    Returns
    -------
    int
        Number of CWS boundaries updated.
    """
    schema = settings.utility_schema

    # Use centroid of CWS boundary to find containing county
    query = text(f"""
        UPDATE {schema}.cws_boundaries cws
        SET county_served = cb.name
        FROM {schema}.county_boundaries cb
        WHERE cws.county_served IS NULL
          AND ST_Intersects(cb.geom, ST_Centroid(cws.geom))
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        updated = result.rowcount
        conn.commit()

    logger.info(f"Spatial join filled county_served for {updated} CWS boundaries")
    return updated


def run_tiger_county_ingest() -> None:
    """Download and load TIGER county boundaries, then backfill CWS counties.

    Downloads Census TIGER/Line 2024 county shapefile, loads into
    utility.county_boundaries, then runs spatial join to fill NULL
    county_served in cws_boundaries.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== TIGER County Ingest Starting ===")

    # Download
    zip_path = _download_tiger_county()

    # Prepare
    gdf = _prepare_counties(zip_path)

    # Truncate and load
    schema = settings.utility_schema
    table = "county_boundaries"
    logger.info(f"Truncating {schema}.{table}")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        conn.commit()

    logger.info(f"Loading {len(gdf)} county boundaries into {schema}.{table}")
    gdf.to_postgis(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=500,
    )

    # Spatial join to backfill missing CWS counties
    logger.info("--- Spatial Join Phase ---")
    with engine.connect() as conn:
        null_before = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.cws_boundaries WHERE county_served IS NULL")
        ).scalar()
    logger.info(f"CWS boundaries missing county before spatial join: {null_before}")

    county_updated = _spatial_join_counties()

    with engine.connect() as conn:
        null_after = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.cws_boundaries WHERE county_served IS NULL")
        ).scalar()
    logger.info(f"CWS boundaries missing county after spatial join: {null_after}")

    # Log completion
    with engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{table}")
        ).scalar()
        conn.execute(
            text(
                f"INSERT INTO {schema}.pipeline_runs "
                f"(step_name, started_at, finished_at, row_count, status, notes) "
                f"VALUES (:step, :started, NOW(), :count, 'success', :notes)"
            ),
            {
                "step": "tiger_county",
                "started": started,
                "count": count,
                "notes": (
                    f"Spatial join: {county_updated} CWS boundaries filled "
                    f"({null_before} NULL before → {null_after} NULL after)"
                ),
            },
        )
        conn.commit()

    logger.info(
        f"=== TIGER County Ingest Complete: {count} counties loaded, "
        f"{county_updated} CWS counties filled by spatial join ==="
    )


if __name__ == "__main__":
    run_tiger_county_ingest()

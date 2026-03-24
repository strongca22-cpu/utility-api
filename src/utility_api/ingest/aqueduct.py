#!/usr/bin/env python3
"""
Aqueduct 4.0 PostGIS Ingest

Purpose:
    Load WRI Aqueduct 4.0 watershed risk polygons from the existing GDB
    (shared with strong-strategic project) into the utility.aqueduct_polygons
    table. Enables real-time ST_Contains queries for the /resolve endpoint.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - geopandas
    - sqlalchemy
    - geoalchemy2

Usage:
    ua-ingest aqueduct

Notes:
    - Reuses Aqueduct GDB already downloaded by strong-strategic project
    - Loads baseline_annual layer (~23K polygons globally)
    - Truncate-and-reload pattern (idempotent)
    - Indicator columns use _score (normalized 0-5), not _raw

Data Sources:
    - Input: /data/datasets/strong-strategic/raw/aqueduct/.../Aq40_Y2023D07M05.gdb
    - Output: utility.aqueduct_polygons table
"""

from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine

# Columns to extract from Aqueduct GDB and their target DB column names
INDICATOR_COLS = [
    "string_id",
    "pfaf_id",
    "gid_1",
    "aqid",
    "bws_score",
    "bws_label",
    "bwd_score",
    "iav_score",
    "sev_score",
    "drr_score",
    "w_awr_def_tot_score",
]


def _load_gdb(gdb_path: Path) -> gpd.GeoDataFrame:
    """Load baseline_annual layer from Aqueduct GDB.

    Parameters
    ----------
    gdb_path : Path
        Path to the Aqueduct File GeoDatabase directory.

    Returns
    -------
    gpd.GeoDataFrame
        Aqueduct polygons with risk indicators and geometry.
    """
    logger.info(f"Loading Aqueduct GDB: {gdb_path}")
    layers = gpd.list_layers(gdb_path)
    logger.info(f"Available layers: {layers['name'].tolist()}")

    # Find baseline_annual layer
    target_layer = None
    for name in layers["name"].tolist():
        if "baseline_annual" in name.lower():
            target_layer = name
            break

    if target_layer is None:
        target_layer = layers["name"].tolist()[0]
        logger.warning(f"No baseline_annual layer found, using: {target_layer}")

    logger.info(f"Reading layer: {target_layer}")
    gdf = gpd.read_file(gdb_path, layer=target_layer)
    logger.info(f"Loaded {len(gdf)} polygons, CRS: {gdf.crs}")

    # Ensure EPSG:4326
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)

    return gdf


def _prepare_for_postgis(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Select and rename columns for the aqueduct_polygons table.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Raw Aqueduct data from GDB.

    Returns
    -------
    gpd.GeoDataFrame
        Cleaned data ready for PostGIS insert.
    """
    # Check which indicator columns are available
    available = [col for col in INDICATOR_COLS if col in gdf.columns]
    missing = [col for col in INDICATOR_COLS if col not in gdf.columns]
    if missing:
        logger.warning(f"Missing columns in GDB (will be NULL): {missing}")

    # Select available columns + geometry
    out = gdf[available + ["geometry"]].copy()

    # Add missing columns as None
    for col in missing:
        out[col] = None

    # Ensure MultiPolygon geometry (some may be Polygon)
    from shapely.geometry import MultiPolygon

    out["geometry"] = out["geometry"].apply(
        lambda g: MultiPolygon([g]) if g.geom_type == "Polygon" else g
    )

    # Drop rows without string_id (shouldn't happen, but safety)
    out = out.dropna(subset=["string_id"])

    # Rename geometry column to match DB column name 'geom'
    out = out.rename_geometry("geom")

    logger.info(f"Prepared {len(out)} rows for PostGIS")
    return out


def run_aqueduct_ingest() -> None:
    """Load Aqueduct 4.0 polygons into utility.aqueduct_polygons.

    Truncates existing data and reloads (idempotent).
    """
    gdb_path = Path(settings.aqueduct_gdb_path)
    if not gdb_path.exists():
        raise FileNotFoundError(
            f"Aqueduct GDB not found at {gdb_path}. "
            f"Check AQUEDUCT_GDB_PATH in .env"
        )

    # Log pipeline run
    started = datetime.now(timezone.utc)
    logger.info("=== Aqueduct Ingest Starting ===")

    # Load and prepare
    gdf = _load_gdb(gdb_path)
    gdf = _prepare_for_postgis(gdf)

    # Truncate and reload
    schema = settings.utility_schema
    table = "aqueduct_polygons"
    logger.info(f"Truncating {schema}.{table}")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        conn.commit()

    logger.info(f"Loading {len(gdf)} polygons into {schema}.{table}")
    gdf.to_postgis(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=1000,
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
            {"step": "aqueduct", "started": started, "count": count},
        )
        conn.commit()

    logger.info(f"=== Aqueduct Ingest Complete: {count} polygons loaded ===")


if __name__ == "__main__":
    run_aqueduct_ingest()

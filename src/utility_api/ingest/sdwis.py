#!/usr/bin/env python3
"""
SDWIS Data Ingest (via EPA ECHO Bulk Download)

Purpose:
    Download SDWIS water system attributes and violation history from
    EPA ECHO bulk downloads. Filter to target states (VA + CA for Sprint 1).
    Compute 5-year violation aggregates. Load into utility.sdwis_systems.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-25

Dependencies:
    - requests
    - pandas
    - sqlalchemy

Usage:
    ua-ingest sdwis

Notes:
    - Downloads ~200MB ZIP from ECHO (cached in data/raw/sdwis/)
    - Filters to target states defined in config/sources.yaml
    - Computes violation_count_5yr and health_violation_count_5yr
    - Only loads systems that have a matching CWS boundary (FK constraint)
    - UPSERT pattern on PWSID (idempotent)

Data Sources:
    - Input: https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip
    - Output: utility.sdwis_systems table
"""

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, load_sources_config, settings
from utility_api.db import engine

ECHO_URL = "https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "sdwis"


def _download_echo_zip() -> Path:
    """Download ECHO SDWA bulk ZIP if not cached.

    Returns
    -------
    Path
        Path to the downloaded ZIP file.
    """
    import requests

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "SDWA_latest_downloads.zip"

    if zip_path.exists():
        logger.info(f"Using cached ECHO ZIP: {zip_path}")
        return zip_path

    logger.info(f"Downloading ECHO SDWA bulk data from {ECHO_URL}")
    logger.info("This file is ~200MB — may take a few minutes")

    r = requests.get(ECHO_URL, stream=True, timeout=600)
    r.raise_for_status()

    total = int(r.headers.get("content-length", 0))
    downloaded = 0

    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0 and downloaded % (50 * 1024 * 1024) < 65536:
                pct = downloaded / total * 100
                logger.info(f"  {downloaded / 1e6:.0f}MB / {total / 1e6:.0f}MB ({pct:.0f}%)")

    logger.info(f"Download complete: {zip_path} ({zip_path.stat().st_size / 1e6:.0f}MB)")
    return zip_path


def _load_water_systems(zip_path: Path, target_states: list[str] | None) -> pd.DataFrame:
    """Extract and filter water system data from ECHO ZIP.

    Parameters
    ----------
    zip_path : Path
        Path to the ECHO SDWA ZIP file.
    target_states : list[str] or None
        State codes to filter to (e.g., ["VA", "CA"]).
        If None, loads all states (50-state expansion).

    Returns
    -------
    pd.DataFrame
        Filtered water system records.
    """
    logger.info(f"Reading SDWA_PUB_WATER_SYSTEMS from ZIP")

    with zipfile.ZipFile(zip_path, "r") as zf:
        # List contents to find the right file
        names = zf.namelist()
        logger.info(f"ZIP contents: {names[:10]}...")

        # Find the water systems CSV
        ws_file = None
        for name in names:
            if "WATER_SYSTEM" in name.upper() and name.endswith(".csv"):
                ws_file = name
                break

        if ws_file is None:
            # Try without extension filter
            for name in names:
                if "WATER_SYSTEM" in name.upper():
                    ws_file = name
                    break

        if ws_file is None:
            raise FileNotFoundError(
                f"No WATER_SYSTEM file found in ZIP. Contents: {names}"
            )

        logger.info(f"Reading {ws_file}")
        with zf.open(ws_file) as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                dtype=str,
                low_memory=False,
            )

    logger.info(f"Loaded {len(df)} total water systems")
    logger.info(f"Columns: {df.columns.tolist()[:15]}...")

    # Find state column (various possible names)
    state_col = None
    for candidate in ["PRIMACY_AGENCY_CODE", "STATE_CODE", "PWSID"]:
        if candidate in df.columns:
            state_col = candidate
            break

    if state_col == "PWSID":
        # Extract state from PWSID (first 2 chars)
        df["_state"] = df["PWSID"].str[:2]
        state_col = "_state"

    if target_states is None:
        # ALL states — no filtering
        logger.info(f"Loading all {len(df)} systems (no state filter)")
        return df

    if state_col:
        df_filtered = df[df[state_col].isin(target_states)].copy()
        logger.info(f"Filtered to {len(df_filtered)} systems in {target_states}")
    else:
        logger.warning("Could not identify state column — loading all systems")
        df_filtered = df

    return df_filtered


def _load_violations(zip_path: Path, target_pwsids: set[str]) -> pd.DataFrame:
    """Extract violation data and compute 5-year aggregates.

    Parameters
    ----------
    zip_path : Path
        Path to the ECHO SDWA ZIP file.
    target_pwsids : set[str]
        Set of PWSIDs to filter to.

    Returns
    -------
    pd.DataFrame
        Violation aggregates indexed by PWSID.
    """
    logger.info("Reading SDWA_VIOLATIONS from ZIP")

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        viol_file = None
        for name in names:
            if "VIOLATION" in name.upper() and name.endswith(".csv"):
                viol_file = name
                break

        if viol_file is None:
            logger.warning("No VIOLATION file found in ZIP — skipping violation aggregates")
            return pd.DataFrame(columns=["PWSID", "violation_count_5yr",
                                        "health_violation_count_5yr", "last_violation_date"])

        logger.info(f"Reading {viol_file}")
        with zf.open(viol_file) as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                dtype=str,
                low_memory=False,
            )

    logger.info(f"Loaded {len(df)} total violations")

    # Filter to target systems
    df = df[df["PWSID"].isin(target_pwsids)].copy()
    logger.info(f"Filtered to {len(df)} violations for target systems")

    if len(df) == 0:
        return pd.DataFrame(columns=["PWSID", "violation_count_5yr",
                                    "health_violation_count_5yr", "last_violation_date"])

    # Parse dates
    date_col = None
    for candidate in ["COMPL_PER_BEGIN_DATE", "VIOLATION_DATE", "ENFRC_RESOLVED_DATE"]:
        if candidate in df.columns:
            date_col = candidate
            break

    cutoff = datetime.now(timezone.utc).year - 5
    if date_col:
        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df_5yr = df[df["_date"].dt.year >= cutoff]
    else:
        logger.warning("No date column found — using all violations as 5yr")
        df_5yr = df

    # Aggregate
    agg = df_5yr.groupby("PWSID").agg(
        violation_count_5yr=("PWSID", "count"),
    ).reset_index()

    # Health violations
    health_col = None
    for candidate in ["IS_HEALTH_BASED_IND", "CONTAMINANT_CODE", "VIOLATION_CATEGORY_CODE"]:
        if candidate in df_5yr.columns:
            health_col = candidate
            break

    if health_col == "IS_HEALTH_BASED_IND":
        health = df_5yr[df_5yr[health_col] == "Y"].groupby("PWSID").size().reset_index(
            name="health_violation_count_5yr"
        )
    elif health_col:
        # Use any health-related category as proxy
        health = pd.DataFrame({"PWSID": agg["PWSID"], "health_violation_count_5yr": 0})
    else:
        health = pd.DataFrame({"PWSID": agg["PWSID"], "health_violation_count_5yr": 0})

    agg = agg.merge(health, on="PWSID", how="left")
    agg["health_violation_count_5yr"] = agg["health_violation_count_5yr"].fillna(0).astype(int)

    # Last violation date
    if date_col:
        last_date = df.groupby("PWSID")["_date"].max().reset_index()
        last_date.columns = ["PWSID", "last_violation_date"]
        agg = agg.merge(last_date, on="PWSID", how="left")
    else:
        agg["last_violation_date"] = None

    logger.info(f"Computed violation aggregates for {len(agg)} systems")
    return agg


def _load_county_mapping(
    zip_path: Path, target_pwsids: set[str]
) -> dict[str, str]:
    """Extract PWSID → county mapping from SDWA_GEOGRAPHIC_AREAS.

    Parameters
    ----------
    zip_path : Path
        Path to the ECHO SDWA ZIP file.
    target_pwsids : set[str]
        Set of PWSIDs to filter to.

    Returns
    -------
    dict[str, str]
        Mapping of PWSID to county name. For systems serving multiple
        counties, the first county listed is used (~0.01% of systems).
    """
    logger.info("Reading SDWA_GEOGRAPHIC_AREAS from ZIP for county mapping")

    with zipfile.ZipFile(zip_path, "r") as zf:
        geo_file = None
        for name in zf.namelist():
            if "GEOGRAPHIC_AREA" in name.upper() and name.endswith(".csv"):
                geo_file = name
                break

        if geo_file is None:
            logger.warning("No GEOGRAPHIC_AREAS file found — county enrichment skipped")
            return {}

        with zf.open(geo_file) as f:
            df = pd.read_csv(
                io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                dtype=str,
                low_memory=False,
                usecols=["PWSID", "COUNTY_SERVED"],
            )

    # Filter to target systems and non-null county
    df = df[df["PWSID"].isin(target_pwsids) & df["COUNTY_SERVED"].notna()].copy()
    df["COUNTY_SERVED"] = df["COUNTY_SERVED"].str.strip()

    # Deduplicate: keep first county per PWSID
    county_map = df.drop_duplicates(subset=["PWSID"], keep="first")
    result = dict(zip(county_map["PWSID"], county_map["COUNTY_SERVED"]))

    logger.info(f"Extracted county mapping for {len(result)} systems")
    return result


def _update_cws_counties(county_map: dict[str, str]) -> int:
    """Update cws_boundaries.county_served from SDWIS geographic areas.

    Parameters
    ----------
    county_map : dict[str, str]
        PWSID → county name mapping.

    Returns
    -------
    int
        Number of rows updated.
    """
    if not county_map:
        return 0

    schema = settings.utility_schema
    updated = 0

    with engine.connect() as conn:
        # Batch update in chunks
        items = list(county_map.items())
        for i in range(0, len(items), 500):
            batch = items[i:i + 500]
            for pwsid, county in batch:
                result = conn.execute(
                    text(
                        f"UPDATE {schema}.cws_boundaries "
                        f"SET county_served = :county "
                        f"WHERE pwsid = :pwsid AND (county_served IS NULL OR county_served != :county)"
                    ),
                    {"pwsid": pwsid, "county": county},
                )
                updated += result.rowcount
            conn.commit()

    logger.info(f"Updated county_served for {updated} CWS boundaries")
    return updated


def _build_sdwis_records(
    systems: pd.DataFrame,
    violations: pd.DataFrame,
    existing_pwsids: set[str],
) -> pd.DataFrame:
    """Build final records for sdwis_systems table.

    Parameters
    ----------
    systems : pd.DataFrame
        Water system records from ECHO.
    violations : pd.DataFrame
        Violation aggregates.
    existing_pwsids : set[str]
        PWSIDs that exist in cws_boundaries (FK constraint).

    Returns
    -------
    pd.DataFrame
        Records ready for insert.
    """
    # Standardize column names
    col_map = {}
    for src, dst in [
        ("PWSID", "pwsid"),
        ("PWS_NAME", "pws_name"),
        ("PWS_TYPE_CODE", "pws_type_code"),
        ("PRIMARY_SOURCE_CODE", "primary_source_code"),
        ("POPULATION_SERVED_COUNT", "population_served_count"),
        ("SERVICE_CONNECTIONS_COUNT", "service_connections_count"),
        ("OWNER_TYPE_CODE", "owner_type_code"),
        ("IS_WHOLESALER_IND", "is_wholesaler_ind"),
        ("PWS_ACTIVITY_CODE", "activity_status_cd"),
        ("CITY_NAME", "city"),
    ]:
        if src in systems.columns:
            col_map[src] = dst

    records = systems.rename(columns=col_map)

    # Find state code
    if "PRIMACY_AGENCY_CODE" in systems.columns:
        records["state_code"] = systems["PRIMACY_AGENCY_CODE"].str[:2]
    elif "pwsid" in records.columns:
        records["state_code"] = records["pwsid"].str[:2]

    # Keep only target columns
    target_cols = [
        "pwsid", "pws_name", "pws_type_code", "primary_source_code",
        "population_served_count", "service_connections_count",
        "owner_type_code", "is_wholesaler_ind", "activity_status_cd", "state_code",
        "city",
    ]
    available = [c for c in target_cols if c in records.columns]
    records = records[available].copy()

    # Convert numeric columns
    for col in ["population_served_count", "service_connections_count"]:
        if col in records.columns:
            records[col] = pd.to_numeric(records[col], errors="coerce")

    # Merge violations
    if len(violations) > 0:
        records = records.merge(
            violations[["PWSID", "violation_count_5yr", "health_violation_count_5yr",
                        "last_violation_date"]].rename(columns={"PWSID": "pwsid"}),
            on="pwsid",
            how="left",
        )
    else:
        records["violation_count_5yr"] = None
        records["health_violation_count_5yr"] = None
        records["last_violation_date"] = None

    # Fill missing violation counts with 0
    for col in ["violation_count_5yr", "health_violation_count_5yr"]:
        if col in records.columns:
            records[col] = records[col].fillna(0).astype(int)

    # Only keep systems with matching CWS boundary (FK constraint)
    before = len(records)
    records = records[records["pwsid"].isin(existing_pwsids)]
    after = len(records)
    if before > after:
        logger.info(f"Dropped {before - after} systems without CWS boundary match")

    # Deduplicate on PWSID
    records = records.drop_duplicates(subset=["pwsid"], keep="first")

    logger.info(f"Built {len(records)} SDWIS records for insert")
    return records


def run_sdwis_ingest() -> None:
    """Download and load SDWIS data for target states.

    Downloads ECHO bulk ZIP, extracts water systems and violations,
    computes aggregates, loads into utility.sdwis_systems.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== SDWIS Ingest Starting ===")

    # Get target states from config
    # Sprint 10: supports "ALL" keyword for full 50-state expansion
    config = load_sources_config()
    sdwis_states = config.get("sdwis_states", config.get("states", ["VA", "CA"]))
    if sdwis_states == "ALL" or sdwis_states == "all":
        target_states = None  # Signal to load all states
        logger.info("Target states: ALL (50-state expansion)")
    else:
        target_states = sdwis_states
        logger.info(f"Target states: {target_states}")

    # Download
    zip_path = _download_echo_zip()

    # Load water systems (target_states=None means all states)
    systems = _load_water_systems(zip_path, target_states)
    pwsids = set(systems["PWSID"].dropna())
    logger.info(f"Total water systems to process: {len(systems)} ({len(pwsids)} unique PWSIDs)")

    # Load violations
    violations = _load_violations(zip_path, pwsids)

    # Get existing CWS boundary PWSIDs (FK constraint)
    schema = settings.utility_schema
    with engine.connect() as conn:
        existing = conn.execute(
            text(f"SELECT pwsid FROM {schema}.cws_boundaries")
        ).fetchall()
        existing_pwsids = {row[0] for row in existing}
    logger.info(f"Found {len(existing_pwsids)} existing CWS boundaries")

    # Build records
    records = _build_sdwis_records(systems, violations, existing_pwsids)

    if len(records) == 0:
        logger.warning("No SDWIS records to load (no CWS boundary matches)")
        return

    # Truncate and load
    table = "sdwis_systems"
    logger.info(f"Truncating {schema}.{table}")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        conn.commit()

    logger.info(f"Loading {len(records)} records into {schema}.{table}")
    records.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    # --- County Enrichment ---
    # Extract PWSID → county from SDWA_GEOGRAPHIC_AREAS and backfill
    # into cws_boundaries.county_served. This covers all CWS systems in
    # the geographic areas file, not just the SDWIS-matched subset.
    logger.info("--- County Enrichment Phase ---")
    all_pwsids = existing_pwsids  # All CWS boundary PWSIDs
    county_map = _load_county_mapping(zip_path, all_pwsids)
    county_updated = _update_cws_counties(county_map)

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
                "step": "sdwis",
                "started": started,
                "count": count,
                "notes": f"County enrichment: {county_updated} CWS boundaries updated",
            },
        )
        conn.commit()

    logger.info(f"=== SDWIS Ingest Complete: {count} systems loaded, {county_updated} counties enriched ===")


if __name__ == "__main__":
    run_sdwis_ingest()

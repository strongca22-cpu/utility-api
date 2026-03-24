#!/usr/bin/env python3
"""
CA SWRCB eWRIMS Permit Ingest

Purpose:
    Download and load California State Water Resources Control Board
    water rights data from the eWRIMS system into utility.permits.
    Uses the data.ca.gov CKAN Datastore API.

    Joins two datasets:
      - Demand Analysis Flat File: spatial + volume + owner + status
      - Demand Analysis Uses and Seasons: USE_CODE (purpose of use)

    Targeted load — excludes Domestic use rights. One row per water right
    (not per POD), with use_codes stored as a JSON list.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests
    - pandas
    - pyyaml
    - sqlalchemy

Usage:
    ua-ingest ca-ewrims

Notes:
    - CKAN Datastore API max ~32K records per request; uses offset pagination
    - All numeric fields in CKAN come as text strings — requires casting
    - USE_CODE lives in a separate table, joined on APPLICATION_NUMBER
    - Multi-use rights: one water right can have multiple USE_CODEs
    - category_group assigned based on highest-priority USE_CODE
    - Raw attributes stored in raw_attrs JSONB column
    - Face values are always in Acre-feet per Year (or null)
    - Max diversion rates have 7 different unit types

Data Sources:
    - Demand Analysis Flat File: resource_id=e8235902-adc3-48ce-ab96-fcf230a09208
    - Uses and Seasons: resource_id=fcf8b0d1-8775-4dfb-8a43-7a25f625c4e6
    - Output: utility.permits table
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# CKAN Datastore API
CKAN_BASE = "https://data.ca.gov/api/3/action/datastore_search"

# Resource IDs
FLAT_FILE_RESOURCE = "e8235902-adc3-48ce-ab96-fcf230a09208"
USES_SEASONS_RESOURCE = "fcf8b0d1-8775-4dfb-8a43-7a25f625c4e6"

# Pagination
CKAN_PAGE_SIZE = 32000

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ca_ewrims"

# USE_CODEs to exclude from targeted load
EXCLUDE_USE_CODES = {"Domestic"}

# Priority order for assigning category_group when multiple use codes exist.
# Higher-priority groups are assigned first (index 0 = highest priority).
CATEGORY_PRIORITY = [
    "industrial",
    "energy",
    "municipal",
    "mining",
    "environmental",
    "water_withdrawal",
    "infrastructure",
    "agricultural",
    "commercial",
    "other",
]


def _load_category_map() -> dict[str, str]:
    """Load CA eWRIMS USE_CODE → category_group mapping.

    Returns
    -------
    dict[str, str]
        USE_CODE → category_group mapping.
    """
    config_path = PROJECT_ROOT / "config" / "category_mapping.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config.get("ca_ewrims_use_code", {})


def _fetch_ckan_resource(resource_id: str, fields: list[str] | None = None) -> list[dict]:
    """Paginated fetch of a CKAN datastore resource.

    Parameters
    ----------
    resource_id : str
        CKAN resource ID.
    fields : list[str] | None
        Optional list of fields to retrieve (reduces payload).

    Returns
    -------
    list[dict]
        All records from the resource.
    """
    all_records = []
    offset = 0

    while True:
        params: dict[str, Any] = {
            "resource_id": resource_id,
            "limit": CKAN_PAGE_SIZE,
            "offset": offset,
        }
        if fields:
            params["fields"] = ",".join(fields)

        for attempt in range(3):
            try:
                r = requests.get(CKAN_BASE, params=params, timeout=300)
                r.raise_for_status()
                data = r.json()

                if not data.get("success"):
                    raise ValueError(f"CKAN API error: {data.get('error', 'unknown')}")

                records = data["result"]["records"]
                all_records.extend(records)
                total = data["result"].get("total", 0)

                logger.debug(
                    f"Resource {resource_id[:8]}...: fetched {len(records)} records "
                    f"(offset={offset}, total so far={len(all_records)}/{total})"
                )
                break

            except Exception as e:
                if attempt < 2:
                    wait = 10 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

        if len(records) < CKAN_PAGE_SIZE:
            break

        offset += CKAN_PAGE_SIZE
        time.sleep(1)  # Polite pause

    logger.info(f"Fetched {len(all_records)} total records from resource {resource_id[:8]}...")
    return all_records


def _cache_records(records: list[dict], cache_name: str) -> Path:
    """Cache raw CKAN records to JSON file.

    Parameters
    ----------
    records : list[dict]
        Records from CKAN API.
    cache_name : str
        Name for the cache file.

    Returns
    -------
    Path
        Path to the cache file.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / f"{cache_name}.json"
    with open(cache_path, "w") as f:
        json.dump(records, f)
    logger.info(f"Cached {len(records)} records to {cache_path.name}")
    return cache_path


def _safe_float(val: Any) -> float | None:
    """Safely convert a value to float, returning None on failure.

    Parameters
    ----------
    val : Any
        Value to convert (often a string from CKAN).

    Returns
    -------
    float | None
        Converted float or None.
    """
    if val is None or val == "" or val == "NaN":
        return None
    try:
        result = float(val)
        if pd.isna(result):
            return None
        return result
    except (ValueError, TypeError):
        return None


def _safe_date(val: Any) -> str | None:
    """Safely parse a date string to ISO format.

    Parameters
    ----------
    val : Any
        Date string from CKAN (various formats).

    Returns
    -------
    str | None
        ISO date string or None.
    """
    if val is None or val == "" or val == "NaN":
        return None
    try:
        # CKAN dates may be ISO format or various others
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _build_use_code_lookup(
    uses_records: list[dict],
) -> dict[str, list[str]]:
    """Build APPLICATION_NUMBER → list of USE_CODEs mapping.

    Excludes USE_CODEs in EXCLUDE_USE_CODES.

    Parameters
    ----------
    uses_records : list[dict]
        Records from the Uses and Seasons resource.

    Returns
    -------
    dict[str, list[str]]
        APPLICATION_NUMBER → deduplicated list of USE_CODEs.
    """
    lookup: dict[str, set[str]] = {}
    for rec in uses_records:
        app_num = rec.get("APPLICATION_NUMBER", "")
        use_code = rec.get("USE_CODE", "")
        if not app_num or not use_code:
            continue
        if use_code in EXCLUDE_USE_CODES:
            continue
        lookup.setdefault(app_num, set()).add(use_code)

    # Convert sets to sorted lists
    return {k: sorted(v) for k, v in lookup.items()}


def _pick_category_group(
    use_codes: list[str], category_map: dict[str, str]
) -> str:
    """Pick the highest-priority category_group from a list of USE_CODEs.

    Parameters
    ----------
    use_codes : list[str]
        List of USE_CODEs for a water right.
    category_map : dict[str, str]
        USE_CODE → category_group mapping.

    Returns
    -------
    str
        The highest-priority category_group.
    """
    groups = {category_map.get(uc, "other") for uc in use_codes}
    for priority_group in CATEGORY_PRIORITY:
        if priority_group in groups:
            return priority_group
    return "other"


def _build_ewrims_rows(
    flat_records: list[dict],
    use_lookup: dict[str, list[str]],
    category_map: dict[str, str],
) -> list[dict]:
    """Convert eWRIMS flat file records to permit table rows.

    Filters to records that have at least one non-excluded USE_CODE.

    Parameters
    ----------
    flat_records : list[dict]
        Records from the Demand Analysis Flat File.
    use_lookup : dict[str, list[str]]
        APPLICATION_NUMBER → list of USE_CODEs.
    category_map : dict[str, str]
        USE_CODE → category_group mapping.

    Returns
    -------
    list[dict]
        Rows ready for database insert.
    """
    rows = []
    skipped_no_use = 0
    skipped_domestic_only = 0

    for rec in flat_records:
        app_num = rec.get("APPLICATION_NUMBER", "")
        if not app_num:
            continue

        # Get use codes for this right
        use_codes = use_lookup.get(app_num)

        # Skip if no non-excluded use codes
        if not use_codes:
            # Check if it was domestic-only (excluded)
            skipped_domestic_only += 1
            continue

        # Parse coordinates
        lat = _safe_float(rec.get("LATITUDE"))
        lng = _safe_float(rec.get("LONGITUDE"))

        # Parse volume fields
        face_value = _safe_float(rec.get("FACE_VALUE_AMOUNT"))
        face_units = rec.get("FACE_VALUE_UNITS") or None
        if face_units == "":
            face_units = None

        max_dd = _safe_float(rec.get("MAX_DD_APPL"))
        max_dd_units = rec.get("MAX_DD_UNITS") or None
        if max_dd_units == "":
            max_dd_units = None

        # Status
        status_raw = rec.get("WATER_RIGHT_STATUS", "")
        status = status_raw.lower() if status_raw else None

        # Build the primary source_category from water right type
        wr_type = rec.get("WATER_RIGHT_TYPE", "")

        # Determine category_group from use codes
        category_group = _pick_category_group(use_codes, category_map)

        # Priority date as issued_date (closest analog)
        issued = _safe_date(rec.get("PRIORITY_DATE"))

        # Owner name as facility_name
        owner = rec.get("PRIMARY_OWNER_NAME", "")

        rows.append({
            "source": "ca_swrcb_ewrims",
            "permit_number": app_num,
            "facility_name": owner or None,
            "source_category": wr_type or None,
            "category_group": category_group,
            "use_codes": use_codes,
            "status": status,
            "state_code": "CA",
            "county": None,  # County is in the POD detail table, not flat file
            "issued_date": issued,
            "expiration_date": None,
            "face_value_amount": face_value,
            "face_value_units": face_units,
            "max_diversion_rate": max_dd,
            "max_diversion_units": max_dd_units,
            "geom_wkt": f"SRID=4326;POINT({lng} {lat})" if lng and lat else None,
            "raw_attrs": rec,
        })

    logger.info(
        f"Built {len(rows)} rows from {len(flat_records)} records "
        f"(skipped {skipped_domestic_only} with no targeted use codes)"
    )
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
        params = []
        for row in rows:
            p = dict(row)
            p["raw_attrs"] = json.dumps(p["raw_attrs"]) if p["raw_attrs"] else None
            p["use_codes"] = json.dumps(p["use_codes"]) if p["use_codes"] else None
            params.append(p)

        chunk_size = 500
        for i in range(0, len(params), chunk_size):
            chunk = params[i : i + chunk_size]
            for p in chunk:
                conn.execute(insert_sql, p)

            if (i // chunk_size + 1) % 20 == 0:
                logger.info(f"Inserted {i + len(chunk)}/{len(params)} rows...")

        conn.commit()

    return len(rows)


def run_ca_ewrims_ingest() -> None:
    """Download and load CA eWRIMS water rights into PostGIS.

    Fetches the Demand Analysis Flat File and Uses/Seasons table from
    data.ca.gov CKAN API, joins on APPLICATION_NUMBER, filters to
    targeted USE_CODEs, and loads into utility.permits.
    Truncate-and-reload for ca_swrcb_ewrims source only.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== CA eWRIMS Permit Ingest Starting ===")

    category_map = _load_category_map()

    # --- Fetch Uses and Seasons (needed for USE_CODE filtering) ---
    logger.info("Fetching Uses and Seasons table (for USE_CODE join)...")
    uses_fields = ["APPLICATION_NUMBER", "USE_CODE"]
    uses_records = _fetch_ckan_resource(USES_SEASONS_RESOURCE, fields=uses_fields)
    _cache_records(uses_records, "ewrims_uses_seasons")
    use_lookup = _build_use_code_lookup(uses_records)
    logger.info(f"Built USE_CODE lookup for {len(use_lookup)} applications")

    # --- Fetch Demand Analysis Flat File ---
    logger.info("Fetching Demand Analysis Flat File...")
    flat_records = _fetch_ckan_resource(FLAT_FILE_RESOURCE)
    _cache_records(flat_records, "ewrims_flat_file")
    logger.info(f"Fetched {len(flat_records)} flat file records")

    # --- Build rows ---
    rows = _build_ewrims_rows(flat_records, use_lookup, category_map)

    # --- Truncate CA rows and reload ---
    schema = settings.utility_schema
    with engine.connect() as conn:
        conn.execute(text(
            f"DELETE FROM {schema}.permits WHERE source = 'ca_swrcb_ewrims'"
        ))
        conn.commit()
    logger.info("Cleared existing CA eWRIMS permit rows")

    # Insert
    inserted = _insert_rows(rows)
    logger.info(f"Inserted {inserted} CA eWRIMS permit rows")

    # Log pipeline run
    with engine.connect() as conn:
        conn.execute(
            text(
                f"INSERT INTO {schema}.pipeline_runs "
                f"(step_name, started_at, finished_at, row_count, status, notes) "
                f"VALUES (:step, :started, NOW(), :count, 'success', :notes)"
            ),
            {
                "step": "ca_ewrims",
                "started": started,
                "count": inserted,
                "notes": f"Targeted load: excluded Domestic. {len(use_lookup)} apps with use codes.",
            },
        )
        conn.commit()

    logger.info(f"=== CA eWRIMS Ingest Complete: {inserted} permits loaded ===")


if __name__ == "__main__":
    run_ca_ewrims_ingest()

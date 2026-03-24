#!/usr/bin/env python3
"""
MDWD Financial Data Ingest (Harvard Dataverse)

Purpose:
    Download and load the Municipal Drinking Water Database from Harvard
    Dataverse. Contains financial, demographic, and rate data for ~2,200
    municipal CWS systems. Sparse coverage but high analytical value.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests
    - pandas
    - sqlalchemy

Usage:
    ua-ingest mdwd

Notes:
    - Harvard Dataverse dataset DOI: 10.7910/DVN/DFB6NG
    - Downloads CSV files via Dataverse API
    - Joins to CWS boundaries on PWSID
    - UPSERT on (pwsid, year) — idempotent
    - Coverage: ~2,200 of ~44K CWS nationally (~5%)
    - MDWD uses PWSID format matching EPA format

Data Sources:
    - Input: Harvard Dataverse (doi:10.7910/DVN/DFB6NG)
    - Output: utility.mdwd_financials table
"""

import io
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, load_sources_config, settings
from utility_api.db import engine

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "mdwd"

# Harvard Dataverse API for listing dataset files
DATAVERSE_API = "https://dataverse.harvard.edu/api/datasets/:persistentId"
DATASET_DOI = "doi:10.7910/DVN/DFB6NG"


def _list_dataverse_files() -> list[dict]:
    """List files in the MDWD Harvard Dataverse dataset.

    Returns
    -------
    list[dict]
        File metadata dicts with 'id', 'label', 'description'.
    """
    import requests

    url = f"{DATAVERSE_API}?persistentId={DATASET_DOI}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()

    files = []
    for f in data.get("data", {}).get("latestVersion", {}).get("files", []):
        df = f.get("dataFile", {})
        files.append({
            "id": df.get("id"),
            "label": f.get("label", df.get("filename", "")),
            "description": f.get("description", ""),
            "size": df.get("filesize", 0),
            "content_type": df.get("contentType", ""),
        })

    logger.info(f"Found {len(files)} files in MDWD dataset")
    for f in files:
        logger.debug(f"  {f['label']} ({f['size'] / 1e6:.1f}MB) [{f['content_type']}]")

    return files


def _download_file(file_id: int, filename: str) -> Path:
    """Download a single file from Harvard Dataverse.

    Parameters
    ----------
    file_id : int
        Dataverse file ID.
    filename : str
        Local filename to save as.

    Returns
    -------
    Path
        Path to the downloaded file.
    """
    import requests

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    local_path = RAW_DIR / filename

    if local_path.exists():
        logger.info(f"Using cached file: {local_path}")
        return local_path

    url = f"https://dataverse.harvard.edu/api/access/datafile/{file_id}"
    logger.info(f"Downloading {filename} (file ID {file_id})")

    r = requests.get(url, timeout=300, stream=True)
    r.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)

    logger.info(f"Downloaded: {local_path} ({local_path.stat().st_size / 1e6:.1f}MB)")
    return local_path


def _find_and_download_mdwd() -> pd.DataFrame:
    """Find and download the main MDWD data file.

    Returns
    -------
    pd.DataFrame
        Raw MDWD data.
    """
    files = _list_dataverse_files()

    # Look for a CSV or tab-delimited file with utility/municipal/water data
    target_file = None
    for f in files:
        label_lower = f["label"].lower()
        # Prefer CSV files, look for the main dataset file
        if label_lower.endswith(".csv") or label_lower.endswith(".tab"):
            if any(kw in label_lower for kw in ["mdwd", "municipal", "water", "utility", "main"]):
                target_file = f
                break

    # If no keyword match, take the largest CSV/tab file
    if target_file is None:
        csv_files = [f for f in files if f["label"].lower().endswith((".csv", ".tab"))]
        if csv_files:
            target_file = max(csv_files, key=lambda x: x["size"])

    # If still nothing, try Stata files
    if target_file is None:
        dta_files = [f for f in files if f["label"].lower().endswith(".dta")]
        if dta_files:
            target_file = max(dta_files, key=lambda x: x["size"])

    if target_file is None:
        raise FileNotFoundError(
            f"Could not find a suitable data file in MDWD dataset. "
            f"Available files: {[f['label'] for f in files]}"
        )

    logger.info(f"Selected file: {target_file['label']}")
    local_path = _download_file(target_file["id"], target_file["label"])

    # Read based on file type
    if local_path.suffix.lower() == ".dta":
        df = pd.read_stata(local_path)
    elif local_path.suffix.lower() == ".tab":
        df = pd.read_csv(local_path, sep="\t", dtype=str, low_memory=False)
    else:
        df = pd.read_csv(local_path, dtype=str, low_memory=False)

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {df.columns.tolist()[:20]}...")
    return df


def _prepare_mdwd_records(
    df: pd.DataFrame,
    existing_pwsids: set[str],
    target_states: list[str],
) -> pd.DataFrame:
    """Map MDWD columns to DB schema and filter.

    Parameters
    ----------
    df : pd.DataFrame
        Raw MDWD data.
    existing_pwsids : set[str]
        PWSIDs that exist in cws_boundaries.
    target_states : list[str]
        Target state codes.

    Returns
    -------
    pd.DataFrame
        Records ready for insert.
    """
    # Find PWSID column (various possible names)
    pwsid_col = None
    for candidate in ["pwsid", "PWSID", "pws_id", "PWS_ID", "water_system_id"]:
        if candidate in df.columns:
            pwsid_col = candidate
            break

    if pwsid_col is None:
        # Search for any column containing 'pws' or 'water_system'
        for col in df.columns:
            if "pws" in col.lower() or "water_system" in col.lower():
                pwsid_col = col
                break

    if pwsid_col is None:
        logger.error(f"Cannot find PWSID column. Available: {df.columns.tolist()}")
        raise ValueError("No PWSID column found in MDWD data")

    logger.info(f"Using PWSID column: {pwsid_col}")

    # Normalize PWSID to uppercase
    df["pwsid"] = df[pwsid_col].astype(str).str.strip().str.upper()

    # Filter to target states (state from PWSID prefix)
    df["_state"] = df["pwsid"].str[:2]
    df = df[df["_state"].isin(target_states)].copy()
    logger.info(f"Filtered to {len(df)} records in {target_states}")

    # Map columns — try multiple naming conventions
    # NOTE: MDWD (Harvard Dataverse) is a Census of Governments fiscal dataset.
    # It does NOT contain water rate/bill data (avg_monthly_bill columns).
    # Rate data is a Sprint 3 deliverable (LLM parsing from utility websites).
    # Financial columns map to water-utility-specific revenues, not general govt.
    col_mapping = {}
    search_map = {
        "fips_place_code": ["fips_place", "fips", "place_fips", "FIPS"],
        "year": ["year", "YEAR", "data_year", "survey_year"],
        "median_household_income": ["median_income", "med_hh_income", "median_household_income",
                                     "mhi", "medincome"],
        "pct_below_poverty": ["pov_pct", "poverty_rate", "pct_poverty", "pct_below_poverty",
                               "poverty_pct", "pov_rate"],
        "water_utility_revenue": ["water_utility_revenue", "water_util_revenue",
                                   "water_revenue"],
        "water_utility_expenditure": ["water_util_total_exp", "water_utility_expenditure",
                                       "water_util_expenditure"],
        "water_utility_debt": ["total_debt_outstanding", "debt_outstanding",
                                "outstanding_debt", "water_utility_debt"],
        "population": ["population", "pop", "total_population", "pop_served"],
    }

    for target, candidates in search_map.items():
        for candidate in candidates:
            # Case-insensitive match
            matches = [c for c in df.columns if c.lower() == candidate.lower()]
            if matches:
                col_mapping[matches[0]] = target
                break

    logger.info(f"Mapped columns: {col_mapping}")

    # Rename and select
    df = df.rename(columns=col_mapping)
    target_cols = ["pwsid"] + list(search_map.keys())
    available = [c for c in target_cols if c in df.columns]
    records = df[available].copy()

    # Ensure year column exists
    if "year" not in records.columns:
        # Default to 2022 (MDWD primary snapshot year)
        records["year"] = 2022
        logger.warning("No year column found — defaulting to 2022")
    else:
        records["year"] = pd.to_numeric(records["year"], errors="coerce")

    # Convert numeric columns
    numeric_cols = [
        "avg_monthly_bill_5ccf", "avg_monthly_bill_10ccf",
        "median_household_income", "pct_below_poverty",
        "water_utility_revenue", "water_utility_expenditure",
        "water_utility_debt", "population",
    ]
    for col in numeric_cols:
        if col in records.columns:
            records[col] = pd.to_numeric(records[col], errors="coerce")

    # Filter to systems with CWS boundary match
    before = len(records)
    records = records[records["pwsid"].isin(existing_pwsids)]
    logger.info(f"Matched {len(records)}/{before} records to CWS boundaries")

    # Keep most recent year per PWSID, preferring rows with financial data.
    # MDWD has two cadences: Census of Governments financials (every 5yr: 2017)
    # and ACS demographics (annual: 2018). Prefer the vintage that has both.
    if "year" in records.columns:
        # Flag rows that have financial data (water_utility_revenue)
        has_financials = records["water_utility_revenue"].notna()
        records["_has_financials"] = has_financials.astype(int)
        # Sort: prefer rows WITH financials, then most recent year
        records = records.sort_values(
            ["_has_financials", "year"], ascending=[False, False]
        ).drop_duplicates(subset=["pwsid"], keep="first")
        records = records.drop(columns=["_has_financials"])

    logger.info(f"Prepared {len(records)} MDWD records for insert")
    return records


def run_mdwd_ingest() -> None:
    """Download and load MDWD financial data.

    Downloads from Harvard Dataverse, maps columns, filters to target
    states, loads into utility.mdwd_financials.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== MDWD Ingest Starting ===")

    # Get target states
    config = load_sources_config()
    target_states = config.get("states", ["VA", "CA"])
    logger.info(f"Target states: {target_states}")

    # Download and load
    df = _find_and_download_mdwd()

    # Get existing CWS boundary PWSIDs
    schema = settings.utility_schema
    with engine.connect() as conn:
        existing = conn.execute(
            text(f"SELECT pwsid FROM {schema}.cws_boundaries")
        ).fetchall()
        existing_pwsids = {row[0] for row in existing}
    logger.info(f"Found {len(existing_pwsids)} existing CWS boundaries")

    # Prepare records
    records = _prepare_mdwd_records(df, existing_pwsids, target_states)

    if len(records) == 0:
        logger.warning("No MDWD records to load")
        with engine.connect() as conn:
            conn.execute(
                text(
                    f"INSERT INTO {schema}.pipeline_runs "
                    f"(step_name, started_at, finished_at, row_count, status, notes) "
                    f"VALUES (:step, :started, NOW(), 0, 'success', 'No matching records')"
                ),
                {"step": "mdwd", "started": started},
            )
            conn.commit()
        return

    # Truncate and load
    table = "mdwd_financials"
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
            {"step": "mdwd", "started": started, "count": count},
        )
        conn.commit()

    logger.info(f"=== MDWD Ingest Complete: {count} records loaded ===")


if __name__ == "__main__":
    run_mdwd_ingest()

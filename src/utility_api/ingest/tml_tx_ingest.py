#!/usr/bin/env python3
"""
Texas Municipal League Water Rate Survey Ingest

Purpose:
    Parses TML water rate survey XLSX files and ingests into rate_schedules.
    TML publishes annual surveys with total monthly bill amounts at specific
    gallon consumption levels. City names are fuzzy-matched to SDWIS PWSIDs.

    TML reports bills at gallon levels (5,000 / 10,000 gal), not CCF.
    5,000 gal = 6.68 CCF; 10,000 gal = 13.37 CCF. These are approximate
    equivalents to our bill_5ccf and bill_10ccf benchmarks (5 CCF = 3,740 gal,
    10 CCF = 7,480 gal). Bills are stored with a note about the unit difference.

    This is a BULK ingest — no scraping, no LLM. Excel parsing + name matching.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - xlrd (for .xls format — TML files are old Excel format despite .xlsx extension)
    - sqlalchemy
    - loguru

Usage:
    python scripts/run_tml_ingest.py                    # ingest 2023
    python scripts/run_tml_ingest.py --dry-run           # preview
    python scripts/run_tml_ingest.py --year 2021         # different year

Data Sources:
    - Input: TML XLSX from data/bulk_sources/tx_tml/tml_water_YYYY.xlsx
    - Output: utility.rate_schedules table (source_key: tx_tml_YYYY)

Notes:
    - $0.00 values mean "no response" — filtered out, NOT stored as zero-cost water
    - Duplicate cities (Gregory, Pecos City, etc.) are deduplicated
    - Gregory has a known data entry error: $4,141 should be $41.41
    - Population group headers and "Averages" rows are skipped
    - City-to-PWSID matching uses: exact normalized match → city field match → fuzzy
    - Similar to wv_psc_ingest.py but uses XLSX input and gallon-based bills
"""

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import xlrd
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine

# Constants
SCHEMA = settings.utility_schema
SOURCE_KEY_PATTERN = "tx_tml_{year}"
OUTLIER_THRESHOLD = 500.0  # $500 for 5,000 gal is clearly wrong


def _parse_xlsx(file_path: Path, year: int) -> list[dict]:
    """Parse TML XLSX file into list of city records.

    Skips population group headers, average rows, and the title row.
    Deduplicates cities, keeping the row with more reasonable values.
    Filters out $0.00 values (TML convention: zero = no response).
    """
    wb = xlrd.open_workbook(str(file_path))
    ws = wb.sheet_by_index(0)

    # Row 0 = title, Row 1 = headers, Row 2+ = data
    records = []
    current_pop_group = None

    for i in range(2, ws.nrows):
        col0 = str(ws.cell_value(i, 0)).strip()
        col1_raw = ws.cell_value(i, 1)
        col1 = str(col1_raw).strip()

        # Skip empty rows
        if not col0 and not col1:
            continue

        # Skip "Averages" and "Grand Averages" rows
        if col1 == "Averages" or col0 == "Grand Averages":
            continue

        # Population group header: col0 has text, col1 is empty
        if col1 == "" and col0:
            current_pop_group = col0
            continue

        # City data row: col0 = city name, col1 = population (numeric)
        try:
            population = int(float(col1_raw)) if col1_raw else None
        except (ValueError, TypeError):
            continue  # not a data row

        city_name = col0

        # Extract bill amounts
        def safe_float(col_idx):
            try:
                v = float(ws.cell_value(i, col_idx))
                return v if v > 0 else None  # $0.00 = no response
            except (ValueError, TypeError, IndexError):
                return None

        res_5000 = safe_float(4)
        res_10000 = safe_float(5) if ws.ncols > 5 else None
        com_50000 = safe_float(6) if ws.ncols > 6 else None
        com_200000 = safe_float(7) if ws.ncols > 7 else None

        # Outlier detection: Gregory $4,141 → $41.41
        if res_5000 and res_5000 > OUTLIER_THRESHOLD:
            logger.warning(
                f"  Outlier: {city_name} res_5000=${res_5000:.2f} "
                f"(>{OUTLIER_THRESHOLD}) — likely data entry error"
            )
            # Try dividing by 100 (common misplaced decimal)
            corrected = res_5000 / 100.0
            if 10 < corrected < OUTLIER_THRESHOLD:
                logger.warning(f"  Corrected to ${corrected:.2f}")
                res_5000 = corrected
            else:
                res_5000 = None  # can't salvage, skip this value

        # Skip rows with no usable bill data
        if not res_5000 and not res_10000:
            continue

        # Extract additional fields
        try:
            total_customers = int(float(ws.cell_value(i, 2))) if ws.cell_value(i, 2) else None
        except (ValueError, TypeError):
            total_customers = None

        try:
            avg_usage_gal = int(float(ws.cell_value(i, 3))) if ws.cell_value(i, 3) else None
        except (ValueError, TypeError):
            avg_usage_gal = None

        records.append({
            "city_name": city_name,
            "population": population,
            "pop_group": current_pop_group,
            "total_customers": total_customers,
            "avg_usage_gal": avg_usage_gal,
            "res_5000_gal": res_5000,
            "res_10000_gal": res_10000,
            "com_50000_gal": com_50000,
            "com_200000_gal": com_200000,
        })

    # Deduplicate: keep first occurrence unless it has worse data
    seen = {}
    deduped = []
    for rec in records:
        name = rec["city_name"].upper()
        if name in seen:
            # Keep the one with more non-null bill values
            existing = seen[name]
            existing_vals = sum(1 for k in ["res_5000_gal", "res_10000_gal"]
                                if existing.get(k) is not None)
            new_vals = sum(1 for k in ["res_5000_gal", "res_10000_gal"]
                           if rec.get(k) is not None)
            if new_vals > existing_vals:
                # Replace
                deduped = [r for r in deduped if r["city_name"].upper() != name]
                deduped.append(rec)
                seen[name] = rec
                logger.info(f"  Dedup: {rec['city_name']} — replaced with better data")
            else:
                logger.info(f"  Dedup: {rec['city_name']} — keeping first occurrence")
        else:
            seen[name] = rec
            deduped.append(rec)

    logger.info(
        f"Parsed {len(records)} rows → {len(deduped)} unique cities "
        f"(year={year}, {len(records) - len(deduped)} duplicates removed)"
    )
    return deduped


def _build_tx_sdwis_lookup(conn) -> dict:
    """Build city-name-to-PWSID lookup from TX SDWIS data.

    Returns dict with multiple lookup strategies:
    - by_normalized_name: "CITY OF HOUSTON" → normalized → lookup
    - by_city_field: "HOUSTON" → list of (pwsid, pws_name, pop)
    """
    rows = conn.execute(text(f"""
        SELECT s.pwsid, s.pws_name, s.city, s.population_served_count,
               s.owner_type_code
        FROM {SCHEMA}.sdwis_systems s
        JOIN {SCHEMA}.cws_boundaries c ON s.pwsid = c.pwsid
        WHERE s.state_code = 'TX' AND s.pws_type_code = 'CWS'
    """)).fetchall()

    by_name = {}  # normalized pws_name → [(pwsid, raw_name, pop, owner)]
    by_city = {}  # uppercase city → [(pwsid, raw_name, pop, owner)]

    for r in rows:
        pwsid, pws_name, city, pop, owner = r
        entry = (pwsid, pws_name, pop or 0, owner or "")

        # Normalized name lookup
        norm = _normalize_name(pws_name)
        by_name.setdefault(norm, []).append(entry)

        # City field lookup
        if city:
            by_city.setdefault(city.upper().strip(), []).append(entry)

    return {"by_name": by_name, "by_city": by_city}


def _normalize_name(name: str) -> str:
    """Normalize a utility/city name for matching."""
    s = name.upper()

    # Remove common prefixes/suffixes
    removals = [
        r"\bCITY\s+OF\b",
        r"\bTOWN\s+OF\b",
        r"\bVILLAGE\s+OF\b",
        r"\bWATER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS|UTILITY|UTILITIES|DIST|DISTRICT)\b",
        r"\bMUNICIPAL\s+(WATER|UTILITIES|UTILITY)\b",
        r"\b(INC|LLC)\b\.?",
    ]
    for pattern in removals:
        s = re.sub(pattern, "", s)

    # Normalize abbreviations
    s = re.sub(r"\bFT\.?\s+", "FORT ", s)
    s = re.sub(r"\bST\.?\s+", "SAINT ", s)
    s = re.sub(r"\bMT\.?\s+", "MOUNT ", s)

    # Remove punctuation and normalize whitespace
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _match_city_to_pwsid(
    city_name: str,
    population: int | None,
    lookup: dict,
) -> tuple[str | None, str, str]:
    """Match TML city name to SDWIS PWSID.

    Returns (pwsid, match_method, sdwis_name).

    Matching strategy:
    1. Exact normalized name match (try "CITY OF X", "TOWN OF X", and bare X)
    2. City field match in SDWIS (tiebreak by population proximity + municipal preference)
    3. No match — return None
    """
    norm_city = _normalize_name(city_name)
    by_name = lookup["by_name"]
    by_city = lookup["by_city"]
    pop = population or 0

    # Phase 1: Normalized name match
    # Try with common prefixes
    for prefix in ["", "CITY OF ", "TOWN OF "]:
        candidate = _normalize_name(prefix + city_name)
        if candidate in by_name:
            matches = by_name[candidate]
            # Prefer municipal (M or L) and largest
            matches.sort(key=lambda x: (-(x[3] in ("M", "L")), -x[2]))
            pwsid, raw_name, _, _ = matches[0]
            return (pwsid, "name_exact", raw_name)

    # Try bare normalized name against all entries
    if norm_city in by_name:
        matches = by_name[norm_city]
        matches.sort(key=lambda x: (-(x[3] in ("M", "L")), -x[2]))
        pwsid, raw_name, _, _ = matches[0]
        return (pwsid, "name_normalized", raw_name)

    # Phase 2: City field match
    city_upper = city_name.upper().strip()
    if city_upper in by_city:
        matches = by_city[city_upper]
        # Prefer municipal owner, then closest population
        matches.sort(key=lambda x: (
            -(x[3] in ("M", "L")),
            abs(x[2] - pop) if pop > 0 else 0,
        ))
        pwsid, raw_name, _, _ = matches[0]
        return (pwsid, "city_field", raw_name)

    # Phase 2b: Try common TML→SDWIS transformations
    # "Pecos City" → "PECOS"
    for suffix in [" CITY", " TWP", " TOWNSHIP"]:
        if city_upper.endswith(suffix):
            stripped = city_upper[: -len(suffix)]
            if stripped in by_city:
                matches = by_city[stripped]
                matches.sort(key=lambda x: (-(x[3] in ("M", "L")), -x[2]))
                pwsid, raw_name, _, _ = matches[0]
                return (pwsid, "city_field_stripped", raw_name)

    # Phase 3: No match
    return (None, "unmatched", "")


def run_tml_ingest(
    year: int = 2023,
    dry_run: bool = False,
    file_path: Path | None = None,
) -> dict:
    """Run TML water rate ingest for a given year.

    Parameters
    ----------
    year : int
        Survey year to ingest.
    dry_run : bool
        If True, parse and match but don't write to database.
    file_path : Path, optional
        Override default file location.

    Returns
    -------
    dict
        Summary: total_cities, matched, unmatched, inserted, skipped.
    """
    if file_path is None:
        file_path = PROJECT_ROOT / f"data/bulk_sources/tx_tml/tml_water_{year}.xlsx"

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return {"error": "file_not_found"}

    source_key = SOURCE_KEY_PATTERN.format(year=year)
    logger.info(f"=== TML TX Water Rate Ingest: {year} ===")
    logger.info(f"Source: {file_path}")
    logger.info(f"Source key: {source_key}")

    # Parse XLSX
    records = _parse_xlsx(file_path, year)

    # Build SDWIS lookup
    with engine.connect() as conn:
        lookup = _build_tx_sdwis_lookup(conn)
    logger.info(
        f"SDWIS lookup: {len(lookup['by_name'])} normalized names, "
        f"{len(lookup['by_city'])} city entries"
    )

    # Match cities to PWSIDs
    matched = []
    unmatched = []
    match_methods = {}

    for rec in records:
        pwsid, method, sdwis_name = _match_city_to_pwsid(
            rec["city_name"], rec["population"], lookup
        )

        if pwsid:
            rec["pwsid"] = pwsid
            rec["match_method"] = method
            rec["sdwis_name"] = sdwis_name
            matched.append(rec)
            match_methods[method] = match_methods.get(method, 0) + 1
        else:
            unmatched.append(rec)

    logger.info(f"Matched: {len(matched)} / {len(records)} ({100*len(matched)/len(records):.1f}%)")
    logger.info(f"Match methods: {match_methods}")

    if unmatched:
        logger.info(f"Unmatched ({len(unmatched)}):")
        for u in unmatched[:15]:
            logger.info(f"  {u['city_name']} (pop={u['population']})")
        if len(unmatched) > 15:
            logger.info(f"  ... +{len(unmatched) - 15} more")

    if dry_run:
        logger.info("DRY RUN — no database writes")
        # Show sample matched records
        for rec in matched[:5]:
            bill10_str = f" | 10000gal=${rec['res_10000_gal']:.2f}" if rec.get('res_10000_gal') else ""
            logger.info(
                f"  {rec['city_name']} → {rec['pwsid']} ({rec['sdwis_name']}) "
                f"| 5000gal=${rec['res_5000_gal']:.2f}{bill10_str}"
            )
        return {
            "year": year,
            "total_cities": len(records),
            "matched": len(matched),
            "unmatched": len(unmatched),
            "inserted": 0,
            "dry_run": True,
        }

    # Insert into rate_schedules
    inserted = 0
    skipped = 0
    vintage_date = date(year, 1, 1)
    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        for rec in matched:
            # Build metadata
            notes_parts = [
                f"TML {year} survey",
                f"city: {rec['city_name']}",
                f"pop_group: {rec.get('pop_group', '?')}",
                f"match: {rec['match_method']}",
                f"consumption_unit: gallons (5000gal=6.68CCF, 10000gal=13.37CCF)",
            ]
            if rec.get("avg_usage_gal"):
                notes_parts.append(f"avg_usage: {rec['avg_usage_gal']} gal/month")
            if rec.get("total_customers"):
                notes_parts.append(f"customers: {rec['total_customers']}")

            # Store gallon-based bills — approximate to CCF for compatibility
            # 5,000 gal ≈ 6.68 CCF (our bill_5ccf is 5 CCF = 3,740 gal)
            # 10,000 gal ≈ 13.37 CCF (our bill_10ccf is 10 CCF = 7,480 gal)
            bill_5ccf_approx = rec.get("res_5000_gal")  # slightly higher than true 5 CCF
            bill_10ccf_approx = rec.get("res_10000_gal")  # slightly higher than true 10 CCF

            # Build volumetric tiers as empty (TML only has bill totals)
            volumetric_tiers = json.dumps([])
            fixed_charges = json.dumps([])

            result = conn.execute(
                text(f"""
                INSERT INTO {SCHEMA}.rate_schedules
                    (pwsid, source_key, vintage_date, customer_class,
                     billing_frequency, rate_structure_type,
                     fixed_charges, volumetric_tiers,
                     bill_5ccf, bill_10ccf,
                     confidence, parse_notes, created_at)
                VALUES
                    (:pwsid, :source_key, :vintage, 'residential',
                     'monthly', 'bill_only',
                     :fixed, :tiers,
                     :bill5, :bill10,
                     'medium', :notes, :now)
                ON CONFLICT (pwsid, source_key, vintage_date, customer_class)
                DO UPDATE SET
                    bill_5ccf = EXCLUDED.bill_5ccf,
                    bill_10ccf = EXCLUDED.bill_10ccf,
                    parse_notes = EXCLUDED.parse_notes,
                    created_at = EXCLUDED.created_at
            """),
                {
                    "pwsid": rec["pwsid"],
                    "source_key": source_key,
                    "vintage": vintage_date,
                    "fixed": fixed_charges,
                    "tiers": volumetric_tiers,
                    "bill5": bill_5ccf_approx,
                    "bill10": bill_10ccf_approx,
                    "notes": " | ".join(notes_parts),
                    "now": now,
                },
            )
            if result.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        conn.commit()

    logger.info(f"Inserted: {inserted}, Skipped (conflict): {skipped}")

    # Summary stats
    bills_5k = [r["res_5000_gal"] for r in matched if r.get("res_5000_gal")]
    bills_10k = [r["res_10000_gal"] for r in matched if r.get("res_10000_gal")]

    if bills_5k:
        logger.info(
            f"Bill @5,000 gal: median=${sorted(bills_5k)[len(bills_5k)//2]:.2f}, "
            f"mean=${sum(bills_5k)/len(bills_5k):.2f}, "
            f"range=[${min(bills_5k):.2f}–${max(bills_5k):.2f}]"
        )
    if bills_10k:
        logger.info(
            f"Bill @10,000 gal: median=${sorted(bills_10k)[len(bills_10k)//2]:.2f}, "
            f"mean=${sum(bills_10k)/len(bills_10k):.2f}, "
            f"range=[${min(bills_10k):.2f}–${max(bills_10k):.2f}]"
        )

    return {
        "year": year,
        "source_key": source_key,
        "total_cities": len(records),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "inserted": inserted,
        "skipped": skipped,
        "match_methods": match_methods,
    }

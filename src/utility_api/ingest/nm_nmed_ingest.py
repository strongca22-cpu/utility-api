#!/usr/bin/env python3
"""
New Mexico NMED Drinking Water Bureau Annual Rate Survey Ingest

Purpose:
    Parses the NMED annual water and wastewater user charge survey PDF
    and ingests into the utility.rate_schedules table. The PDF lists publicly
    owned community water systems with monthly bill at 6,000 gallons.

    Since NMED uses utility names (not PWSIDs), this module fuzzy-matches
    against EPA SDWIS records for New Mexico.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - pymupdf (fitz) — PDF text extraction
    - httpx — PDF download
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest nm-nmed --dry-run
    ua-ingest nm-nmed
    ua-ingest nm-nmed --refresh

    # Python
    from utility_api.ingest.nm_nmed_ingest import run_nm_nmed_ingest
    run_nm_nmed_ingest(dry_run=True)

Notes:
    - PDF is 15 pages, text-selectable, repeated header each page
    - Consumption standard: 6,000 gallons/month
    - MDWCA = Mutual Domestic Water Consumers Association (common NM entity)
    - "N/A" means not applicable or not reported — treat as null
    - Utility names sometimes span two lines in PDF
    - Both residential and commercial rates at 6,000 gal
    - We ingest residential water rates only

Data Sources:
    - Input: NMED rate survey PDF (downloaded + cached)
    - Input: utility.sdwis_systems (name matching for NM CWS)
    - Output: utility.rate_schedules table (source_key=nm_nmed_rate_survey_2025)

Configuration:
    - PDF cached at data/raw/nm_nmed/
    - Database connection via .env (DATABASE_URL)
"""

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import fitz  # pymupdf
import httpx
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import water_rate_to_schedule, write_rate_schedule


# --- Constants ---

DATA_DIR = PROJECT_ROOT / "data" / "raw" / "nm_nmed"
PDF_FILE = DATA_DIR / "2025-nmed-rate-survey.pdf"
MATCH_LOG_FILE = DATA_DIR / "name_match_log.json"
PDF_URL = "https://service.web.env.nm.gov/urls/nxEGQnEO"
SOURCE_TAG = "nm_nmed_rate_survey_2025"
VINTAGE_DATE = date(2024, 12, 1)  # December 2024 rates

GAL_PER_CCF = 748.052
CONSUMPTION_GALLONS = 6000

# Known NM counties for validation
NM_COUNTIES = {
    "bernalillo", "catron", "chaves", "cibola", "colfax", "curry", "de baca",
    "dona ana", "eddy", "grant", "guadalupe", "harding", "hidalgo", "lea",
    "lincoln", "los alamos", "luna", "mckinley", "mora", "otero", "quay",
    "rio arriba", "roosevelt", "san juan", "san miguel", "sandoval",
    "santa fe", "sierra", "socorro", "taos", "torrance", "union", "valencia",
}


# --- PDF Download ---


def _download_pdf(refresh: bool = False) -> Path:
    """Download the NMED rate survey PDF if not already cached."""
    if PDF_FILE.exists() and not refresh:
        logger.info(f"Using cached PDF: {PDF_FILE.name}")
        return PDF_FILE

    logger.info(f"Downloading: {PDF_URL}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.env.nm.gov/drinking_water/rates/",
    }
    resp = httpx.get(PDF_URL, timeout=30, follow_redirects=True, headers=headers)
    resp.raise_for_status()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PDF_FILE.write_bytes(resp.content)
    logger.info(f"Saved: {len(resp.content):,} bytes → {PDF_FILE.name}")
    return PDF_FILE


# --- PDF Parsing ---


def _parse_pdf(pdf_path: Path) -> list[dict]:
    """Extract utility rows from the NMED rate survey PDF.

    The PDF has a wide table with 16+ columns. We extract:
    - System Name (col 1)
    - County (col 2)
    - Residential 6,000 gal water rate (col 3)
    - Residential connections (col 5)

    The text extraction produces lines where:
    - Header block repeats on each page (skip)
    - Data rows have: name, county, dollar amounts, connection counts
    - Names can span two lines if long

    Parameters
    ----------
    pdf_path : Path
        Path to the PDF file.

    Returns
    -------
    list[dict]
        Each dict has: utility_name, county, monthly_bill, residential_connections.
    """
    doc = fitz.open(str(pdf_path))
    all_lines = []
    for page in doc:
        text = page.get_text()
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped:
                all_lines.append(stripped)
    doc.close()

    records = []

    # Skip patterns — headers and footers
    skip_patterns = [
        r"^NMED Drinking Water Bureau",
        r"^2025 Public Water",
        r"^Publicly Owned",
        r"^RESIDENTIAL",
        r"^COMMERCIAL",
        r"^Number of",
        r"^WATER Rate",
        r"^Month",
        r"^Dec\.",
        r"^Connections",
        r"^July 2024",
        r"^2024 Total",
        r"^2024 Average",
        r"^Did Not",
        r"^Complete",
        r"^AWWA",
        r"^water audit",
        r"^Apparent",
        r"^Real Losses",
        r"^Non-",
        r"^revenue",
        r"^Data",
        r"^Validity",
        r"^Score",
        r"^2025 PUBLIC WATER",
        r"^AWWA WATER AUDIT",
        r"^Revised",
        r"^County$",
        r"^6,000 Gal\.",
        r"^WATER$",
        r"^Sewer Rate",
        r"^SEWER$",
        r"^PRODUCTION$",
        r"^\(Gallons",
        r"^per Connection",
        r"^water$",
        r"^MG/YR$",
        r"^\(out$",
        r"^of 100\)",
    ]

    # State machine: accumulate utility name, then parse data fields
    i = 0
    while i < len(all_lines):
        line = all_lines[i]

        # Skip header/footer lines
        if any(re.match(p, line, re.IGNORECASE) for p in skip_patterns):
            i += 1
            continue

        # Check if this line starts with a dollar amount — it's a data field, not a name
        if re.match(r"^\$[\d,]+\.\d{2}$", line):
            i += 1
            continue

        # Check if this is just a number (connections count, production, etc.)
        if re.match(r"^[\d,]+$", line) or line in ("N/A", "x", "N", "n"):
            i += 1
            continue

        # Check if this line looks like a county name
        if line.lower().replace(" ", "").replace(".", "") in {
            c.replace(" ", "") for c in NM_COUNTIES
        }:
            i += 1
            continue

        # This might be a utility name — look ahead for county and dollar amount
        name_parts = [line]

        # Look ahead: the next non-skip line should be a county, then dollar amounts
        j = i + 1
        county = None
        bill = None

        # Accumulate name parts until we hit a county
        while j < len(all_lines) and j < i + 5:
            next_line = all_lines[j].strip()

            # Check if it's a county
            next_lower = next_line.lower().replace(".", "")
            if next_lower in NM_COUNTIES or next_lower.replace(" ", "") in {
                c.replace(" ", "") for c in NM_COUNTIES
            }:
                county = next_line
                j += 1
                break

            # Check if it's a dollar amount (would mean we missed the county)
            if re.match(r"^\$[\d,]+\.\d{2}$", next_line):
                break

            # Skip known skip patterns
            if any(re.match(p, next_line, re.IGNORECASE) for p in skip_patterns):
                j += 1
                continue

            # If it's not a number or N/A, it's probably a name continuation
            if not re.match(r"^[\d,]+$", next_line) and next_line not in ("N/A", "x", "N", "n"):
                name_parts.append(next_line)
                j += 1
                continue

            j += 1
            break

        if county is None:
            i += 1
            continue

        # Now find the first dollar amount after county — that's the residential water rate
        while j < len(all_lines) and j < i + 10:
            next_line = all_lines[j].strip()
            bill_match = re.match(r"^\$([\d,]+\.\d{2})$", next_line)
            if bill_match:
                bill = float(bill_match.group(1).replace(",", ""))
                break
            if next_line == "N/A":
                bill = None
                break
            j += 1

        utility_name = " ".join(name_parts).strip()

        # Clean up name
        utility_name = re.sub(r"\s+", " ", utility_name)

        if utility_name and county:
            records.append({
                "utility_name": utility_name,
                "county": county,
                "monthly_bill": bill,
            })

        i = j + 1 if j > i else i + 1

    return records


# --- Name Matching ---


def _normalize_name(name: str) -> str:
    """Normalize a NM utility name for fuzzy matching."""
    s = name.upper()

    removals = [
        r"\bWATER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS|ASSOC|ASSOCIATION|UTILITY|UTILITIES|DIST|DISTRICT|SUPPLY|SERVICE)\b",
        r"\bMUTUAL\s+DOMESTIC\s+WATER\s+CONSUMERS?\s+ASSOC(IATION)?\b",
        r"\bMDWCA\b",
        r"\bMDWC\s*&\s*SA?\b",
        r"\bWATER\s+USERS?\s+ASSOC(IATION)?\b",
        r"\bWUA\b",
        r"\bW\s*&\s*SD\b",
        r"\bW\s*&\s*S\b",
        r"\bWATER\s+AND\s+SANITATION\s+DIST(RICT)?\b",
        r"\bWATER\s+AND\s+SEWER\s+DIST(RICT)?\b",
        r"\bREGIONAL\s+WATER\b",
        r"\bMUNICIPAL\s+WATER\b",
        r"\bTOWN\s+OF\b",
        r"\bCITY\s+OF\b",
        r"\bVILLAGE\s+OF\b",
    ]

    for pattern in removals:
        s = re.sub(pattern, "", s)

    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_sdwis_lookup(conn) -> dict:
    """Build normalized name → (pwsid, raw_name, city) lookup for NM."""
    schema = settings.utility_schema
    rows = conn.execute(text(f"""
        SELECT s.pwsid, s.pws_name, s.city
        FROM {schema}.sdwis_systems s
        JOIN {schema}.cws_boundaries c ON s.pwsid = c.pwsid
        WHERE s.state_code = 'NM' AND s.pws_type_code = 'CWS'
    """)).fetchall()

    lookup = {}
    for pwsid, pws_name, city in rows:
        norm = _normalize_name(pws_name)
        if norm not in lookup:
            lookup[norm] = []
        lookup[norm].append((pwsid, pws_name, city))
    return lookup


def _match_utility_to_pwsid(name: str, county: str, sdwis_lookup: dict) -> tuple[str | None, str | None, str]:
    """Match an NMED utility name to a SDWIS PWSID."""
    norm = _normalize_name(name)
    if not norm:
        return None, None, "empty_name"

    # Exact
    if norm in sdwis_lookup:
        c = sdwis_lookup[norm]
        return c[0][0], c[0][1], "exact"

    # Substring
    for key, candidates in sdwis_lookup.items():
        if norm in key or key in norm:
            if len(norm) >= 4 and len(key) >= 4:
                return candidates[0][0], candidates[0][1], "substring"

    # Word overlap
    norm_words = set(norm.split())
    best_score = 0
    best_match = None

    for key, candidates in sdwis_lookup.items():
        sdwis_words = set(key.split())
        overlap = norm_words & sdwis_words
        if not overlap or not any(len(w) > 3 for w in overlap):
            continue

        overlap_chars = sum(len(w) for w in overlap)
        total_chars = sum(len(w) for w in norm_words | sdwis_words)
        score = overlap_chars / total_chars if total_chars > 0 else 0

        if score > best_score and score >= 0.5:
            best_score = score
            best_match = (candidates[0][0], candidates[0][1])

    if best_match:
        return best_match[0], best_match[1], f"word_overlap({best_score:.2f})"

    return None, None, "no_match"


# --- Main Ingest ---


def run_nm_nmed_ingest(
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run the NM NMED rate survey ingest."""
    started = datetime.now(timezone.utc)
    logger.info("=== NM NMED Rate Survey Ingest Starting ===")
    logger.info(f"Target: rate_schedules (source_key={SOURCE_TAG})")

    # Step 1: Download PDF
    pdf_path = _download_pdf(refresh=refresh)

    # Step 2: Parse PDF
    parsed = _parse_pdf(pdf_path)
    logger.info(f"Parsed {len(parsed)} utility records from PDF")

    # Count nulls
    with_bill = [r for r in parsed if r["monthly_bill"] is not None]
    logger.info(f"With bill data: {len(with_bill)}, missing: {len(parsed) - len(with_bill)}")

    # Step 3: Build SDWIS lookup
    with engine.connect() as conn:
        sdwis_lookup = _build_sdwis_lookup(conn)
    logger.info(f"SDWIS lookup: {sum(len(v) for v in sdwis_lookup.values())} NM CWS systems")

    # Step 4: Match and build records
    stats = {
        "total_pdf_utilities": len(parsed),
        "with_bill": len(with_bill),
        "matched": 0,
        "inserted": 0,
        "unmatched": 0,
        "duplicate_pwsids": [],
    }

    records = []
    seen_pwsids = {}
    match_log = []

    for r in parsed:
        name = r["utility_name"]
        county = r["county"]
        bill = r["monthly_bill"]

        if bill is None:
            match_log.append({"name": name, "county": county, "status": "no_bill"})
            continue

        pwsid, sdwis_name, method = _match_utility_to_pwsid(name, county, sdwis_lookup)

        match_log.append({
            "nmed_name": name,
            "county": county,
            "pwsid": pwsid,
            "sdwis_name": sdwis_name,
            "method": method,
            "monthly_bill": bill,
        })

        if pwsid is None:
            stats["unmatched"] += 1
            continue

        if pwsid in seen_pwsids:
            stats["duplicate_pwsids"].append({"pwsid": pwsid, "kept": seen_pwsids[pwsid], "skipped": name})
            continue
        seen_pwsids[pwsid] = name

        # Approximate bills at CCF benchmarks from 6,000 gal
        # 6,000 gal = 8.02 CCF
        rate_per_gal = bill / CONSUMPTION_GALLONS
        bill_5ccf = round(rate_per_gal * 5.0 * GAL_PER_CCF, 2)
        bill_10ccf = round(rate_per_gal * 10.0 * GAL_PER_CCF, 2)

        notes_parts = [
            f"Name match: {method} (NMED: '{name}' → SDWIS: '{sdwis_name}')",
            f"Bill @6000gal=${bill:.2f}",
            f"County: {county}",
        ]

        records.append({
            "pwsid": pwsid,
            "utility_name": name[:255],
            "state_code": "NM",
            "county": county,
            "rate_effective_date": VINTAGE_DATE,
            "rate_structure_type": "uniform",
            "rate_class": "residential",
            "billing_frequency": "monthly",
            "fixed_charge_monthly": None,
            "meter_size_inches": None,
            "tier_1_limit_ccf": None,
            "tier_1_rate": None,
            "tier_2_limit_ccf": None,
            "tier_2_rate": None,
            "tier_3_limit_ccf": None,
            "tier_3_rate": None,
            "tier_4_limit_ccf": None,
            "tier_4_rate": None,
            "bill_5ccf": bill_5ccf,
            "bill_10ccf": bill_10ccf,
            "bill_6ccf": None,
            "bill_9ccf": None,
            "bill_12ccf": None,
            "bill_24ccf": None,
            "source": SOURCE_TAG,
            "source_url": "https://www.env.nm.gov/drinking_water/rates/",
            "raw_text_hash": None,
            "parse_confidence": "medium" if "exact" in method or "substring" in method else "low",
            "parse_model": None,
            "parse_notes": "; ".join(notes_parts),
        })
        stats["matched"] += 1

    # Save match log
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MATCH_LOG_FILE, "w") as f:
        json.dump(match_log, f, indent=2)

    logger.info(f"Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")
    if stats["duplicate_pwsids"]:
        logger.info(f"Duplicate PWSIDs: {len(stats['duplicate_pwsids'])}")

    unmatched = [m for m in match_log if m.get("method") == "no_match"]
    if unmatched:
        logger.info(f"Unmatched utilities ({len(unmatched)}):")
        for u in unmatched[:15]:
            logger.info(f"  {u.get('nmed_name', '?')} ({u.get('county', '?')}, ${u.get('monthly_bill', '?')}/mo)")

    if dry_run:
        logger.info("\n[DRY RUN] Sample records:")
        for r in records[:10]:
            logger.info(
                f"  {r['pwsid']} | {r['utility_name'][:35]:35s} | "
                f"@6kgal: 5ccf=${r['bill_5ccf']:.2f} 10ccf=${r['bill_10ccf']:.2f} | [{r['parse_confidence']}]"
            )
        if records:
            bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"]]
            if bills:
                logger.info(
                    f"Bill @10CCF: n={len(bills)}, avg=${sum(bills)/len(bills):.2f}, "
                    f"min=${min(bills):.2f}, max=${max(bills):.2f}"
                )
        stats["inserted"] = 0
        return stats

    # Write to DB
    schema = settings.utility_schema
    with engine.connect() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {schema}.rate_schedules WHERE source_key = :source
        """), {"source": SOURCE_TAG}).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing {SOURCE_TAG} records from rate_schedules")

        # Batch insert into rate_schedules (Phase 3: direct write, no water_rates)
        for record in records:
            schedule = water_rate_to_schedule(record)
            write_rate_schedule(conn, schedule)
        conn.commit()
        stats["inserted"] = len(records)

    logger.info(f"Inserted {stats['inserted']} records")

    # Pipeline run
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, NOW(), :count, 'success', :notes)
        """), {
            "step": "nm-nmed-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={SOURCE_TAG}, pdf_utilities={stats['total_pdf_utilities']}, "
                f"with_bill={stats['with_bill']}, matched={stats['matched']}, "
                f"inserted={stats['inserted']}, unmatched={stats['unmatched']}"
            ),
        })
        conn.commit()

    logger.info(f"=== NM NMED Ingest Complete ({elapsed:.1f}s) ===")
    return stats

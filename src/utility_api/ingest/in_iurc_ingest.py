#!/usr/bin/env python3
"""
Indiana IURC Annual Water Bill Analysis Ingest

Purpose:
    Parses the Indiana Utility Regulatory Commission annual water billing
    survey PDF and ingests into the utility.water_rates table. The PDF
    lists all IURC-regulated water utilities with monthly bill at 4,000
    gallons consumption.

    Since IURC uses utility names (not PWSIDs), this module fuzzy-matches
    against EPA SDWIS records for Indiana.

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
    ua-ingest in-iurc --dry-run
    ua-ingest in-iurc
    ua-ingest in-iurc --refresh

    # Python
    from utility_api.ingest.in_iurc_ingest import run_in_iurc_ingest
    run_in_iurc_ingest(dry_run=True)

Notes:
    - PDF is 4 pages, text-selectable, repeated header each page
    - Some entries have parent/sub-row structure (Indiana American Water)
    - Trailing * means fire protection surcharge included in bill
    - Consumption standard: 4,000 gal / 534.7222 cu. ft.
    - Only one bill amount per utility — goes to water_rates as bill-at-consumption
    - Ownership types: IOU, Municipal, NFP (Not-For-Profit), C.D. (Conservancy District)

Data Sources:
    - Input: IURC PDF (downloaded + cached)
    - Input: utility.sdwis_systems (name matching)
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.water_rates table (source=in_iurc_water_billing_2024)

Configuration:
    - PDF cached at data/raw/in_iurc/
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

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import water_rate_to_schedule, write_rate_schedule


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "in_iurc"
PDF_FILE = DATA_DIR / "2024-Water-Billing-Survey-Final.pdf"
MATCH_LOG_FILE = DATA_DIR / "name_match_log.json"
PDF_URL = "https://www.in.gov/iurc/files/2024-Water-Billing-Survey-Final.pdf"
SOURCE_TAG = "in_iurc_water_billing_2024"
VINTAGE_DATE = date(2024, 1, 1)

GAL_PER_CCF = 748.052
CONSUMPTION_GALLONS = 4000


# --- PDF Download ---


def _download_pdf(refresh: bool = False) -> Path:
    """Download the IURC PDF if not already cached.

    Parameters
    ----------
    refresh : bool
        Force re-download if True.

    Returns
    -------
    Path
        Path to the local PDF file.
    """
    if PDF_FILE.exists() and not refresh:
        logger.info(f"Using cached PDF: {PDF_FILE.name}")
        return PDF_FILE

    logger.info(f"Downloading: {PDF_URL}")
    resp = httpx.get(PDF_URL, timeout=30, follow_redirects=True)
    resp.raise_for_status()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PDF_FILE.write_bytes(resp.content)
    logger.info(f"Saved: {len(resp.content):,} bytes → {PDF_FILE.name}")
    return PDF_FILE


# --- PDF Parsing ---


def _parse_pdf(pdf_path: Path) -> list[dict]:
    """Extract utility rows from the IURC water billing PDF.

    Handles the multi-row structure (Indiana American sub-areas) and
    strips fire protection surcharge markers (*).

    Parameters
    ----------
    pdf_path : Path
        Path to the PDF file.

    Returns
    -------
    list[dict]
        Each dict has: utility_name, ownership, order_date, monthly_bill,
        has_fire_surcharge, raw_name.
    """
    doc = fitz.open(str(pdf_path))
    all_text = ""
    for page in doc:
        all_text += page.get_text() + "\n"
    doc.close()

    # Split into lines and clean
    lines = [ln.strip() for ln in all_text.split("\n") if ln.strip()]

    records = []
    current_parent = None  # Track parent row for sub-areas (Indiana American)

    # Skip header lines and footnote lines
    skip_patterns = [
        r"^\d{4}\s+ANNUAL WATER BILL ANALYSIS",
        r"^As of",
        r"^Utility Name",
        r"^Ownership",
        r"^Last Rate",
        r"^Case Cause",
        r"^No\.",
        r"^Order Date",
        r"^Average Monthly",
        r"^4,000 gal",
        r"^\*Fire Protection surcharge",
    ]

    i = 0
    while i < len(lines):
        line = lines[i]

        # Skip headers and footnotes
        if any(re.match(p, line) for p in skip_patterns):
            i += 1
            continue

        # Try to parse a dollar amount — this indicates a bill line
        # Look ahead for the bill amount pattern: $XX.XX
        bill_match = re.search(r"\$(\d+(?:,\d{3})*\.\d{2})", line)

        if bill_match:
            bill = float(bill_match.group(1).replace(",", ""))
            # Everything before the first occurrence of IOU/Municipal/NFP/C.D. is the name
            # But the bill line may just be the bill amount on its own line
            # or it could be part of a larger record

            # The bill amount is always at the end of a record block
            # Work backwards to find the utility name and ownership
            _extract_record_from_context(lines, i, bill, records, current_parent)
            i += 1
            continue

        # Check if this is a parent header (e.g., "Indiana American")
        # Parent headers are followed by child lines with sub-area names
        ownership_types = {"IOU", "Municipal", "NFP", "C.D."}
        if line in ownership_types:
            # This is an ownership line — the previous line was a name
            i += 1
            continue

        # Track parent names for multi-row blocks
        if line in ("Indiana American", "Aqua Indiana", "Community Utilities of Indiana (CUII) -"):
            current_parent = line.rstrip(" -")
            i += 1
            continue

        i += 1

    return records


def _extract_record_from_context(
    lines: list[str],
    bill_line_idx: int,
    bill_amount: float,
    records: list[dict],
    current_parent: str | None,
) -> None:
    """Extract a utility record from the lines surrounding a bill amount.

    The PDF text extraction produces lines like:
        Utility Name
        Ownership
        CauseNo
        OrderDate
        $XX.XX

    We work backwards from the bill amount line to find the name.

    Parameters
    ----------
    lines : list[str]
        All lines from PDF.
    bill_line_idx : int
        Index of the line containing the bill amount.
    bill_amount : float
        Extracted bill amount.
    records : list[dict]
        Accumulator for parsed records.
    current_parent : str | None
        Parent utility name for sub-area records.
    """
    bill_line = lines[bill_line_idx]

    # The bill amount is always the last field. Look backwards for context.
    # We need: utility_name, ownership, order_date
    # Typical pattern (within 5 lines before the bill):
    #   [name_line]
    #   [ownership: IOU|Municipal|NFP|C.D.]
    #   [cause_no]
    #   [order_date: M/D/YY]
    #   [$XX.XX]

    # But sometimes the bill is on the same line as the date or even the name

    ownership_types = {"IOU", "Municipal", "NFP", "C.D."}
    date_pattern = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")

    # Collect context lines (up to 6 lines before, including current)
    start = max(0, bill_line_idx - 6)
    context = lines[start:bill_line_idx + 1]

    # Find ownership line
    ownership = None
    ownership_idx = None
    for j, cl in enumerate(context):
        if cl in ownership_types:
            ownership = cl
            ownership_idx = j

    # Find order date
    order_date = None
    for cl in context:
        m = date_pattern.search(cl)
        if m:
            try:
                order_date = m.group(1)
            except Exception:
                pass

    # Find utility name: everything before ownership in context that isn't
    # a header, date, cause number, or bill amount
    name_parts = []
    skip_patterns_inner = [
        r"^\d{4}\s+ANNUAL",
        r"^As of",
        r"^\*Fire Protection",
        r"^\$\d",
        r"^\d{1,2}/\d{1,2}/\d{2}",
        r"^\d{4,5}(-U)?$",  # cause number
    ]

    for j, cl in enumerate(context):
        if cl in ownership_types:
            break
        if any(re.match(p, cl) for p in skip_patterns_inner):
            continue
        if re.match(r"^\$", cl):
            continue
        # Skip bill amount lines
        if re.search(r"\$\d+\.\d{2}", cl):
            continue
        name_parts.append(cl)

    raw_name = " ".join(name_parts).strip()

    # If no name found from context, and we have a parent, this is a sub-area
    if not raw_name and current_parent:
        raw_name = current_parent

    if not raw_name:
        return  # Can't determine utility name

    # Handle fire protection surcharge marker
    has_fire = raw_name.endswith("*") or "*" in raw_name
    clean_name = raw_name.replace("*", "").strip()

    # For sub-areas under a parent, prefix with parent name
    if current_parent and clean_name != current_parent:
        # Check if this looks like a sub-area name (short, geographic)
        if len(clean_name.split()) <= 4 and not any(
            kw in clean_name.lower()
            for kw in ["water", "utility", "township", "county", "municipal"]
        ):
            # Could be a sub-area — but only if recent parent context
            pass  # Name is already set from context

    records.append({
        "utility_name": clean_name,
        "raw_name": raw_name,
        "ownership": ownership,
        "order_date": order_date,
        "monthly_bill": bill_amount,
        "has_fire_surcharge": has_fire,
    })


def _parse_pdf_regex(pdf_path: Path) -> list[dict]:
    """Alternative regex-based parser for the IURC PDF.

    This approach concatenates all text and uses regex patterns to find
    utility-bill pairs, which is more reliable for the structured format.

    Parameters
    ----------
    pdf_path : Path
        Path to the PDF file.

    Returns
    -------
    list[dict]
        Parsed utility records.
    """
    doc = fitz.open(str(pdf_path))
    all_text = ""
    for page in doc:
        all_text += page.get_text() + "\n"
    doc.close()

    lines = all_text.split("\n")
    records = []

    # State machine approach: walk lines, accumulate name + metadata,
    # emit record when we see a dollar amount
    current_name_parts = []
    current_ownership = None
    current_date = None
    current_parent = None  # For Indiana American sub-areas

    ownership_set = {"IOU", "Municipal", "NFP", "C.D."}
    header_skip = {
        "2024 ANNUAL WATER BILL ANALYSIS",
        "As of January 1, 2024",
        "Utility Name",
        "Ownership",
        "Last Rate",
        "Case Cause",
        "No.",
        "Order Date",
        "Average Monthly Bill for",
        "4,000 gal./ 534.7222 cu. ft.",
    }

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip headers/footnotes
        if line in header_skip:
            continue
        if line.startswith("*Fire Protection surcharge"):
            continue

        # Dollar amount → emit record
        bill_match = re.match(r"^\$(\d+(?:,\d{3})*\.\d{2})$", line)
        if bill_match:
            bill = float(bill_match.group(1).replace(",", ""))
            name = " ".join(current_name_parts).strip()

            if name:
                has_fire = "*" in name
                clean = name.replace("*", "").strip()

                # Prepend parent for sub-area entries
                if current_parent and clean and clean != current_parent:
                    # Only prepend if name looks like a sub-area (no "water"/"utility" etc.)
                    if not any(kw in clean.lower() for kw in [
                        "water", "utility", "township", "county", "municipal",
                        "association", "project", "conservancy", "homeowners",
                        "campground", "knobs", "basin", "acres"
                    ]):
                        clean = f"{current_parent} - {clean}"

                records.append({
                    "utility_name": clean,
                    "raw_name": name,
                    "ownership": current_ownership,
                    "order_date": current_date,
                    "monthly_bill": bill,
                    "has_fire_surcharge": has_fire,
                })

            current_name_parts = []
            current_ownership = None
            current_date = None
            continue

        # Ownership line
        if line in ownership_set:
            current_ownership = line
            continue

        # Date line (M/D/YY or M/D/YYYY)
        if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", line):
            current_date = line
            continue

        # Cause number (e.g., 45142, 45651-U)
        if re.match(r"^\d{4,5}(-U)?$", line):
            continue

        # Check for parent utility headers
        # "Indiana American" followed by "IOU" then sub-areas
        if line == "Indiana American":
            current_parent = "Indiana American"
            current_name_parts = []
            continue
        if line == "Aqua Indiana":
            current_parent = "Aqua Indiana"
            current_name_parts = []
            continue

        # Area markers (Area One, Area Two, etc.) — reset name accumulator
        if re.match(r"^Area (One|Two|Three|Four)", line):
            has_fire = "*" in line
            area_name = line.replace("*", "").strip()
            current_name_parts = [area_name]
            continue

        # If we hit a long geographic list (Indiana American service areas),
        # skip it — it's a description, not a utility name
        if len(line) > 80 and "," in line:
            continue

        # Regular name line — accumulate
        current_name_parts.append(line)

        # If this looks like a standalone utility (not a sub-area), clear parent
        if any(kw in line.lower() for kw in [
            "water", "municipal", "township", "county", "conservancy",
            "authority", "homeowners", "campground"
        ]):
            current_parent = None

    return records


# --- Name Matching (adapted from wv_psc_ingest.py) ---


def _normalize_name(name: str) -> str:
    """Normalize a utility name for fuzzy matching.

    Parameters
    ----------
    name : str
        Raw utility name.

    Returns
    -------
    str
        Normalized name for comparison.
    """
    s = name.upper()

    removals = [
        r"\bWATER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS|ASSOC|ASSOCIATION|UTILITY|UTILITIES|DIST|DISTRICT|DIVISION|PROJECT|CORPORATION|CORP)\b",
        r"\bTOWN\s+OF\b",
        r"\bCITY\s+OF\b",
        r"\bVILLAGE\s+OF\b",
        r"\bMUNICIPAL\s+(WATER|UTILITIES|UTILITY)\b",
        r"\b(INC|LLC|L\.L\.C)\b\.?",
        r"\bCOMMUNITY\s+(WATER|FACIL|FACILITIES)\b",
        r"\bIMPROVEMENT\b",
        r"\bCONSERVANCY\s+DIST(RICT)?\.?\b",
        r"\bRURAL\s+WATER\b",
        r"\bPUBLIC\s+(WATER|UTILITY)\b",
        r"\b(INSIDE|OUTSIDE)\s+(CITY|CORP)\b",
        r"\bAUTHORITY\b",
    ]

    for pattern in removals:
        s = re.sub(pattern, "", s)

    # Remove parenthetical content
    s = re.sub(r"\(.*?\)", "", s)
    # Remove punctuation and normalize whitespace
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _build_sdwis_lookup(conn, state: str = "IN") -> dict:
    """Build normalized name → (pwsid, raw_name) lookup from SDWIS.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Active database connection.
    state : str
        State code to filter SDWIS records.

    Returns
    -------
    dict
        Mapping of normalized_name → list of (pwsid, raw_name, city) tuples.
    """
    schema = settings.utility_schema
    rows = conn.execute(text(f"""
        SELECT s.pwsid, s.pws_name, s.city
        FROM {schema}.sdwis_systems s
        JOIN {schema}.cws_boundaries c ON s.pwsid = c.pwsid
        WHERE s.state_code = :state AND s.pws_type_code = 'CWS'
    """), {"state": state}).fetchall()

    lookup = {}
    for pwsid, pws_name, city in rows:
        norm = _normalize_name(pws_name)
        if norm not in lookup:
            lookup[norm] = []
        lookup[norm].append((pwsid, pws_name, city))

    return lookup


def _match_utility_to_pwsid(
    iurc_name: str,
    sdwis_lookup: dict,
) -> tuple[str | None, str | None, str]:
    """Match an IURC utility name to a SDWIS PWSID.

    Uses progressively relaxed matching:
    1. Exact normalized match
    2. Substring containment
    3. Word overlap scoring

    Parameters
    ----------
    iurc_name : str
        Utility name from IURC PDF.
    sdwis_lookup : dict
        Normalized name → [(pwsid, raw_name, city)] from SDWIS.

    Returns
    -------
    tuple[str | None, str | None, str]
        (pwsid, sdwis_name, match_method) — None if no match found.
    """
    iurc_norm = _normalize_name(iurc_name)

    if not iurc_norm:
        return None, None, "empty_name"

    # 1. Exact normalized match
    if iurc_norm in sdwis_lookup:
        candidates = sdwis_lookup[iurc_norm]
        return candidates[0][0], candidates[0][1], "exact"

    # 2. Substring containment (both directions)
    for norm_key, candidates in sdwis_lookup.items():
        if iurc_norm in norm_key or norm_key in iurc_norm:
            if len(iurc_norm) >= 4 and len(norm_key) >= 4:
                return candidates[0][0], candidates[0][1], "substring"

    # 3. Word overlap scoring
    iurc_words = set(iurc_norm.split())
    best_score = 0
    best_match = None

    for norm_key, candidates in sdwis_lookup.items():
        sdwis_words = set(norm_key.split())
        if not iurc_words or not sdwis_words:
            continue

        overlap = iurc_words & sdwis_words
        if not overlap:
            continue

        # Require at least one significant word overlap (>3 chars)
        significant = any(len(w) > 3 for w in overlap)
        if not significant:
            continue

        overlap_chars = sum(len(w) for w in overlap)
        total_chars = sum(len(w) for w in iurc_words | sdwis_words)
        score = overlap_chars / total_chars if total_chars > 0 else 0

        if score > best_score and score >= 0.5:
            best_score = score
            best_match = (candidates[0][0], candidates[0][1])

    if best_match:
        return best_match[0], best_match[1], f"word_overlap({best_score:.2f})"

    return None, None, "no_match"


# --- Main Ingest ---


def run_in_iurc_ingest(
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run the Indiana IURC water billing ingest.

    Parameters
    ----------
    dry_run : bool
        If True, parse and match but don't write to DB.
    refresh : bool
        If True, force re-download PDF.

    Returns
    -------
    dict
        Summary stats.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== Indiana IURC Water Billing Ingest Starting ===")
    logger.info(f"Source: {PDF_URL}")
    logger.info(f"Target: water_rates (source={SOURCE_TAG})")

    # Step 1: Download PDF
    pdf_path = _download_pdf(refresh=refresh)

    # Step 2: Parse PDF
    parsed = _parse_pdf_regex(pdf_path)
    logger.info(f"Parsed {len(parsed)} utility records from PDF")

    # Show parse summary
    ownership_counts = {}
    for r in parsed:
        o = r.get("ownership", "unknown")
        ownership_counts[o] = ownership_counts.get(o, 0) + 1
    logger.info(f"Ownership breakdown: {ownership_counts}")

    fire_count = sum(1 for r in parsed if r["has_fire_surcharge"])
    logger.info(f"Fire protection surcharge included: {fire_count}/{len(parsed)}")

    # Step 3: Build SDWIS lookup for IN
    with engine.connect() as conn:
        sdwis_lookup = _build_sdwis_lookup(conn, state="IN")
    logger.info(f"SDWIS lookup: {sum(len(v) for v in sdwis_lookup.values())} IN CWS systems")

    # Step 4: Match and build records
    stats = {
        "total_pdf_utilities": len(parsed),
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
        bill = r["monthly_bill"]

        pwsid, sdwis_name, method = _match_utility_to_pwsid(name, sdwis_lookup)

        match_log.append({
            "iurc_name": name,
            "pwsid": pwsid,
            "sdwis_name": sdwis_name,
            "method": method,
            "monthly_bill": bill,
            "ownership": r.get("ownership"),
        })

        if pwsid is None:
            stats["unmatched"] += 1
            continue

        # Dedup
        if pwsid in seen_pwsids:
            stats["duplicate_pwsids"].append({
                "pwsid": pwsid,
                "kept": seen_pwsids[pwsid],
                "skipped": name,
            })
            continue
        seen_pwsids[pwsid] = name

        # Approximate bills at CCF benchmarks from 4,000 gal data point
        # 4,000 gal = 5.347 CCF. Scale linearly (rough, but only one data point).
        rate_per_gal = bill / CONSUMPTION_GALLONS
        bill_5ccf = round(rate_per_gal * 5.0 * GAL_PER_CCF, 2)  # 3,740 gal
        bill_10ccf = round(rate_per_gal * 10.0 * GAL_PER_CCF, 2)  # 7,480 gal

        notes_parts = [
            f"Name match: {method} (IURC: '{name}' → SDWIS: '{sdwis_name}')",
            f"Bill @4000gal=${bill:.2f}",
        ]
        if r.get("ownership"):
            notes_parts.append(f"Ownership: {r['ownership']}")
        if r["has_fire_surcharge"]:
            notes_parts.append("Fire protection surcharge included")

        records.append({
            "pwsid": pwsid,
            "utility_name": name[:255],
            "state_code": "IN",
            "county": None,
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
            "source_url": PDF_URL,
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
    logger.info(f"Match log saved to {MATCH_LOG_FILE.name}")

    logger.info(f"Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")
    if stats["duplicate_pwsids"]:
        logger.info(f"Duplicate PWSIDs: {len(stats['duplicate_pwsids'])}")
        for d in stats["duplicate_pwsids"][:5]:
            logger.info(f"  {d['pwsid']}: kept '{d['kept']}', skipped '{d['skipped']}'")

    # Log unmatched
    unmatched = [m for m in match_log if m["pwsid"] is None]
    if unmatched:
        logger.info(f"Unmatched utilities ({len(unmatched)}):")
        for u in unmatched:
            logger.info(f"  {u['iurc_name']} (${u['monthly_bill']:.2f}/mo, {u['ownership']})")

    # Dry run: show samples
    if dry_run:
        logger.info("\n[DRY RUN] Sample records:")
        for r in records[:10]:
            logger.info(
                f"  {r['pwsid']} | {r['utility_name'][:35]:35s} | "
                f"@4kgal=${r['bill_5ccf'] or 0:.2f}→${r['bill_10ccf'] or 0:.2f} | "
                f"[{r['parse_confidence']}]"
            )

        if records:
            bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
            if bills:
                logger.info(
                    f"Bill @10CCF: n={len(bills)}, avg=${sum(bills)/len(bills):.2f}, "
                    f"min=${min(bills):.2f}, max=${max(bills):.2f}"
                )
        stats["inserted"] = 0
        return stats

    # Step 5: Write to DB
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

    # Log pipeline run
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, NOW(), :count, 'success', :notes)
        """), {
            "step": "in-iurc-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={SOURCE_TAG}, pdf_utilities={stats['total_pdf_utilities']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"unmatched={stats['unmatched']}, "
                f"duplicates={len(stats['duplicate_pwsids'])}"
            ),
        })
        conn.commit()

    logger.info(f"=== Indiana IURC Ingest Complete ({elapsed:.1f}s) ===")
    return stats

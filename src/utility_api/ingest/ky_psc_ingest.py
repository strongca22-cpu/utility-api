#!/usr/bin/env python3
"""
Kentucky PSC Water Tariff Directory Ingest

Purpose:
    Crawls the Kentucky Public Service Commission IIS file server to
    download individual tariff PDFs for ~127 water districts, associations,
    and IOUs. Extracts rate structures via Claude API, fuzzy-matches to
    SDWIS PWSIDs, and writes to rate_schedules.

    Directly analogous to the WV PSC pipeline (wv_psc_ingest.py).

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - pymupdf (fitz) — PDF text extraction
    - httpx — HTTP client for IIS crawl + PDF download
    - anthropic — Claude API for rate parsing
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest ky-psc --dry-run --limit 5
    ua-ingest ky-psc --dry-run
    ua-ingest ky-psc

    # Python
    from utility_api.ingest.ky_psc_ingest import run_ky_psc_ingest
    run_ky_psc_ingest(dry_run=True, limit=5)

Notes:
    - IIS directory at psc.ky.gov lists ~136 subdirectories
    - Each subdirectory may have a Tariff.pdf
    - Rate structures vary: minimum bill + per-gallon tiers
    - Some PDFs use CCF/HCF — converted to gallons
    - City-owned utilities (Louisville, Lexington) NOT in this directory
    - Target section: "Monthly Rates" or "Monthly Service Rate" for 5/8" meter
    - Ignore: fire protection, wholesale, tap-on, purchased water sections

Data Sources:
    - Input: KY PSC tariff directory (IIS listing + PDF downloads)
    - Input: utility.sdwis_systems (name matching for KY CWS)
    - Output: utility.rate_schedules (source_key: ky_psc_water_tariffs_2025)
    - Output: utility.pipeline_runs (audit trail)

Configuration:
    - PDFs cached at data/raw/ky_psc/{utility_slug}/Tariff.pdf
    - Database connection via .env (DATABASE_URL)
    - ANTHROPIC_API_KEY in .env for Claude API
"""

import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import fitz  # pymupdf
import httpx
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# --- Constants ---

DATA_DIR = PROJECT_ROOT / "data" / "raw" / "ky_psc"
MATCH_LOG_FILE = DATA_DIR / "name_match_log.json"
SOURCE_KEY = "ky_psc_water_tariffs_2025"
STATE = "KY"

DIRECTORY_URL = (
    "https://psc.ky.gov/tariffs/water/"
    "Districts,%20Associations,%20%26%20Privately%20Owned/"
)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

GAL_PER_CCF = 748.052

RATE_PARSE_PROMPT = """Extract the residential water rate structure from this Kentucky water utility tariff.

Look for the section labeled "Monthly Rates", "Monthly Water Rates", or "Monthly Service Rate" for GENERAL CUSTOMERS or RESIDENTIAL customers with a 5/8" or 5/8 x 3/4 inch meter.

IGNORE these sections:
- Fire protection rates
- Wholesale water rates
- Purchased water rates
- Tap-on/connection fees
- Commercial/industrial rates
- Mobile Home Park rates
- U.S. Corps of Engineers rates

Extract as JSON:
{
  "fixed_charge": <minimum bill amount in dollars for first tier, e.g. 24.87>,
  "first_tier_gallons": <number of gallons included in minimum bill, e.g. 2000>,
  "tiers": [
    {"from_gal": 0, "to_gal": 2000, "rate_per_gal": null, "note": "included in minimum"},
    {"from_gal": 2001, "to_gal": 5000, "rate_per_gal": 0.00978},
    {"from_gal": 5001, "to_gal": 10000, "rate_per_gal": 0.00829}
  ],
  "effective_date": "YYYY-MM-DD or null if not found",
  "billing_unit": "gallons or ccf or hcf",
  "case_number": "PSC case number if visible"
}

If rates are in CCF or HCF instead of gallons, note the billing_unit and provide the rates as given (I'll convert).
If you cannot find residential rates, return {"error": "no_residential_rates_found"}.
Only return the JSON, no other text."""


# --- IIS Directory Parsing ---


def _list_utility_directories() -> list[dict]:
    """Fetch and parse the IIS directory listing.

    Returns
    -------
    list[dict]
        Each dict has: name (display name), url (full URL to subdirectory).
    """
    logger.info(f"Fetching directory listing: {DIRECTORY_URL}")
    resp = httpx.get(DIRECTORY_URL, timeout=30, follow_redirects=True, headers=HTTP_HEADERS)
    resp.raise_for_status()

    # Parse <dir> entries: <A HREF="/tariffs/water/.../{name}/">Display Name</A>
    entries = re.findall(
        r'&lt;dir&gt;\s*<A HREF="([^"]+)">([^<]+)</A>',
        resp.text,
    )

    results = []
    for href, label in entries:
        full_url = "https://psc.ky.gov" + href
        results.append({"name": label.strip(), "url": full_url})

    logger.info(f"Found {len(results)} utility directories")
    return results


# --- PDF Download + Text Extraction ---


def _download_tariff_pdf(utility: dict, refresh: bool = False) -> Path | None:
    """Download Tariff.pdf for a utility.

    Parameters
    ----------
    utility : dict
        Has 'name' and 'url' keys.
    refresh : bool
        Force re-download.

    Returns
    -------
    Path | None
        Local path to PDF, or None if not found.
    """
    slug = re.sub(r"[^\w\s-]", "", utility["name"]).strip().replace(" ", "_")
    local_dir = DATA_DIR / slug
    local_pdf = local_dir / "Tariff.pdf"

    if local_pdf.exists() and not refresh:
        return local_pdf

    tariff_url = utility["url"] + "Tariff.pdf"
    try:
        resp = httpx.get(tariff_url, timeout=30, follow_redirects=True, headers=HTTP_HEADERS)
        if resp.status_code == 200 and len(resp.content) > 1000:
            local_dir.mkdir(parents=True, exist_ok=True)
            local_pdf.write_bytes(resp.content)
            return local_pdf
        else:
            return None
    except Exception as e:
        logger.debug(f"  Failed to download {utility['name']}: {e}")
        return None


def _extract_rate_pages(pdf_path: Path, max_chars: int = 15000) -> str:
    """Extract text from pages containing rate information.

    Focuses on pages with dollar amounts and rate-related keywords.

    Parameters
    ----------
    pdf_path : Path
        Path to tariff PDF.
    max_chars : int
        Maximum characters to return.

    Returns
    -------
    str
        Extracted text from rate-relevant pages.
    """
    doc = fitz.open(str(pdf_path))
    rate_pages = []

    for i, page in enumerate(doc):
        text = page.get_text()
        lower = text.lower()
        # Look for rate-relevant pages
        if ("$" in text and
            any(kw in lower for kw in [
                "monthly rate", "monthly water rate", "monthly service",
                "minimum bill", "per 1,000", "per gallon", "per gal",
                "first", "next", "over", "gallons",
            ])):
            rate_pages.append((i, text))

    doc.close()

    if not rate_pages:
        return ""

    # Concatenate rate pages, up to max_chars
    result = ""
    for page_num, text in rate_pages:
        if len(result) + len(text) > max_chars:
            break
        result += f"\n--- Page {page_num + 1} ---\n{text}"

    return result


# --- Claude API Parsing ---


def _parse_rates_with_llm(text: str, utility_name: str) -> dict | None:
    """Parse rate structure from tariff text using Claude API.

    Parameters
    ----------
    text : str
        Extracted tariff text.
    utility_name : str
        Utility name for logging.

    Returns
    -------
    dict | None
        Parsed rate structure, or None if parsing failed.
    """
    import anthropic

    client = anthropic.Anthropic()

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"{RATE_PARSE_PROMPT}\n\n--- TARIFF TEXT ---\n{text[:10000]}",
                }
            ],
        )

        content = response.content[0].text.strip()

        # Extract JSON from response (may have markdown code fences)
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group())
            if "error" in parsed:
                logger.debug(f"  LLM returned error for {utility_name}: {parsed['error']}")
                return None
            return parsed

    except Exception as e:
        logger.debug(f"  LLM parse failed for {utility_name}: {e}")

    return None


# --- Rate Structure Conversion ---


def _convert_to_rate_schedule(parsed: dict, utility_name: str) -> dict:
    """Convert LLM-parsed rates to rate_schedules format.

    Parameters
    ----------
    parsed : dict
        LLM output with fixed_charge, tiers, etc.
    utility_name : str
        For logging.

    Returns
    -------
    dict
        Fields ready for rate_schedules INSERT.
    """
    billing_unit = parsed.get("billing_unit", "gallons").lower()
    is_ccf = "ccf" in billing_unit or "hcf" in billing_unit

    fixed_charge = parsed.get("fixed_charge")
    if fixed_charge is not None:
        fixed_charge = float(fixed_charge)

    tiers = parsed.get("tiers", [])
    volumetric_tiers = []
    tier_num = 0

    for t in tiers:
        rate = t.get("rate_per_gal") or t.get("rate_per_gallon")
        if rate is None:
            continue  # Skip "included in minimum" tiers

        rate = float(rate)
        from_gal = float(t.get("from_gal", 0))
        to_gal = t.get("to_gal")

        if is_ccf:
            # Convert CCF boundaries to gallons
            from_gal = from_gal * GAL_PER_CCF
            if to_gal is not None:
                to_gal = float(to_gal) * GAL_PER_CCF
            # Convert rate from per-CCF to per-1000-gal
            rate_per_1000 = rate / GAL_PER_CCF * 1000
        else:
            # Rate is per gallon → convert to per 1000 gallons
            if to_gal is not None:
                to_gal = float(to_gal)
            rate_per_1000 = rate * 1000

        tier_num += 1
        volumetric_tiers.append({
            "tier": tier_num,
            "min_gal": round(from_gal, 0),
            "max_gal": round(to_gal, 0) if to_gal else None,
            "rate_per_1000_gal": round(rate_per_1000, 4),
        })

    # Fixed charges JSONB
    fixed_charges_jsonb = None
    if fixed_charge is not None:
        fixed_charges_jsonb = json.dumps([{
            "name": "Minimum Bill",
            "amount": fixed_charge,
            "frequency": "monthly",
            "meter_size": "5/8\"",
        }])

    # Determine structure type
    if not volumetric_tiers:
        structure_type = "flat"
    elif len(volumetric_tiers) == 1:
        structure_type = "uniform"
    else:
        rates = [t["rate_per_1000_gal"] for t in volumetric_tiers]
        if all(rates[i] >= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "decreasing_block"
        else:
            structure_type = "increasing_block"

    # Calculate bills at CCF benchmarks
    def calc_bill(gallons):
        bill = fixed_charge or 0
        remaining = gallons
        # Subtract gallons included in minimum bill
        first_tier_gal = parsed.get("first_tier_gallons", 0)
        if first_tier_gal:
            remaining = max(0, gallons - float(first_tier_gal))

        for tier in volumetric_tiers:
            if remaining <= 0:
                break
            min_g = tier["min_gal"]
            max_g = tier["max_gal"]
            rate = tier["rate_per_1000_gal"]

            if max_g is not None:
                tier_width = max_g - min_g
                use = min(remaining, tier_width)
            else:
                use = remaining

            bill += (use / 1000) * rate
            remaining -= use

        return round(bill, 2)

    bill_5ccf = calc_bill(5 * GAL_PER_CCF)
    bill_10ccf = calc_bill(10 * GAL_PER_CCF)
    bill_20ccf = calc_bill(20 * GAL_PER_CCF)

    # Conservation signal
    cons_signal = None
    if len(volumetric_tiers) >= 2:
        rates = [t["rate_per_1000_gal"] for t in volumetric_tiers if t["rate_per_1000_gal"] > 0]
        if len(rates) >= 2:
            cons_signal = round(max(rates) / min(rates), 3)

    # Effective date
    eff_date = None
    raw_date = parsed.get("effective_date")
    if raw_date:
        try:
            eff_date = datetime.strptime(str(raw_date), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    return {
        "fixed_charges": fixed_charges_jsonb,
        "volumetric_tiers": json.dumps(volumetric_tiers) if volumetric_tiers else None,
        "rate_structure_type": structure_type,
        "tier_count": len(volumetric_tiers),
        "bill_5ccf": bill_5ccf,
        "bill_10ccf": bill_10ccf,
        "bill_20ccf": bill_20ccf,
        "conservation_signal": cons_signal,
        "effective_date": eff_date,
        "case_number": parsed.get("case_number"),
    }


# --- Name Matching (same as IN IURC / WV PSC) ---


def _normalize_name(name: str) -> str:
    """Normalize utility name for fuzzy matching."""
    s = name.upper()

    removals = [
        r"\bWATER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS|ASSOC|ASSOCIATION|UTILITY|UTILITIES|DIST|DISTRICT|DIVISION|OPERATING)\b",
        r"\bTOWN\s+OF\b",
        r"\bCITY\s+OF\b",
        r"\bCOUNTY\s+WATER\b",
        r"\b(INC|LLC|L\.L\.C)\b\.?",
    ]

    for pattern in removals:
        s = re.sub(pattern, "", s)

    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_sdwis_lookup(conn) -> dict:
    """Build normalized name → (pwsid, raw_name) lookup for KY."""
    schema = settings.utility_schema
    rows = conn.execute(text(f"""
        SELECT s.pwsid, s.pws_name, s.city
        FROM {schema}.sdwis_systems s
        JOIN {schema}.cws_boundaries c ON s.pwsid = c.pwsid
        WHERE s.state_code = :state AND s.pws_type_code = 'CWS'
    """), {"state": STATE}).fetchall()

    lookup = {}
    for pwsid, pws_name, city in rows:
        norm = _normalize_name(pws_name)
        if norm not in lookup:
            lookup[norm] = []
        lookup[norm].append((pwsid, pws_name, city))
    return lookup


def _match_utility_to_pwsid(name: str, sdwis_lookup: dict) -> tuple[str | None, str | None, str]:
    """Match a KY PSC utility name to a SDWIS PWSID."""
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


def run_ky_psc_ingest(
    dry_run: bool = False,
    refresh: bool = False,
    limit: int | None = None,
) -> dict:
    """Run the Kentucky PSC water tariff ingest.

    Parameters
    ----------
    dry_run : bool
        If True, download + parse but don't write to DB.
    refresh : bool
        If True, force re-download PDFs.
    limit : int | None
        Process at most N utilities (for testing).

    Returns
    -------
    dict
        Summary stats.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== Kentucky PSC Water Tariff Ingest Starting ===")
    logger.info(f"Target: rate_schedules (source_key={SOURCE_KEY})")

    # Step 1: List utility directories
    utilities = _list_utility_directories()
    if limit:
        utilities = utilities[:limit]
        logger.info(f"Limited to {limit} utilities")

    # Step 2: Build SDWIS lookup
    with engine.connect() as conn:
        sdwis_lookup = _build_sdwis_lookup(conn)
    logger.info(f"SDWIS lookup: {sum(len(v) for v in sdwis_lookup.values())} KY CWS systems")

    # Step 3: Process each utility
    stats = {
        "total_directories": len(utilities),
        "pdfs_found": 0,
        "pdfs_parsed": 0,
        "matched": 0,
        "inserted": 0,
        "unmatched": 0,
        "parse_failed": 0,
        "no_pdf": 0,
    }

    records = []
    match_log = []
    seen_pwsids = {}
    api_cost = 0.0

    for idx, utility in enumerate(utilities):
        name = utility["name"]
        logger.info(f"[{idx+1}/{len(utilities)}] {name}")

        # Download tariff
        pdf_path = _download_tariff_pdf(utility, refresh=refresh)
        if pdf_path is None:
            logger.debug(f"  No Tariff.pdf found")
            stats["no_pdf"] += 1
            match_log.append({"name": name, "status": "no_pdf"})
            continue
        stats["pdfs_found"] += 1

        # Extract rate pages
        rate_text = _extract_rate_pages(pdf_path)
        if not rate_text:
            logger.debug(f"  No rate pages found in PDF")
            stats["parse_failed"] += 1
            match_log.append({"name": name, "status": "no_rate_pages"})
            continue

        # LLM parse (with polite delay)
        if idx > 0:
            time.sleep(0.5)

        parsed = _parse_rates_with_llm(rate_text, name)
        if parsed is None:
            stats["parse_failed"] += 1
            match_log.append({"name": name, "status": "llm_parse_failed"})
            continue
        stats["pdfs_parsed"] += 1

        # Estimate API cost (~$0.001 per Haiku call)
        api_cost += 0.001

        # Convert to rate_schedule format
        rate_data = _convert_to_rate_schedule(parsed, name)

        # Match to PWSID
        pwsid, sdwis_name, method = _match_utility_to_pwsid(name, sdwis_lookup)

        match_log.append({
            "name": name,
            "pwsid": pwsid,
            "sdwis_name": sdwis_name,
            "method": method,
            "bill_10ccf": rate_data["bill_10ccf"],
            "tiers": rate_data["tier_count"],
        })

        if pwsid is None:
            stats["unmatched"] += 1
            logger.debug(f"  No PWSID match")
            continue

        if pwsid in seen_pwsids:
            continue
        seen_pwsids[pwsid] = name
        stats["matched"] += 1

        # Build record
        notes = (
            f"Name match: {method} (PSC: '{name}' → SDWIS: '{sdwis_name}'); "
            f"tiers={rate_data['tier_count']}; "
            f"struct={rate_data['rate_structure_type']}"
        )
        if rate_data.get("case_number"):
            notes += f"; case={rate_data['case_number']}"

        records.append({
            "pwsid": pwsid,
            "source_key": SOURCE_KEY,
            "vintage_date": rate_data.get("effective_date") or date(2025, 1, 1),
            "customer_class": "residential",
            "billing_frequency": "monthly",
            "rate_structure_type": rate_data["rate_structure_type"],
            "fixed_charges": rate_data["fixed_charges"],
            "volumetric_tiers": rate_data["volumetric_tiers"],
            "surcharges": None,
            "bill_5ccf": rate_data["bill_5ccf"],
            "bill_10ccf": rate_data["bill_10ccf"],
            "bill_20ccf": rate_data["bill_20ccf"],
            "conservation_signal": rate_data["conservation_signal"],
            "tier_count": rate_data["tier_count"],
            "source_url": utility["url"] + "Tariff.pdf",
            "confidence": "medium",
            "parse_notes": notes,
            "needs_review": False,
        })

        logger.info(
            f"  → {pwsid} | ${rate_data['bill_5ccf']:.2f}→${rate_data['bill_10ccf']:.2f}→${rate_data['bill_20ccf']:.2f} | "
            f"tiers={rate_data['tier_count']} | {rate_data['rate_structure_type']} | [{method}]"
        )

    # Save match log
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MATCH_LOG_FILE, "w") as f:
        json.dump(match_log, f, indent=2)

    logger.info(f"\n--- Summary ---")
    logger.info(f"Directories: {stats['total_directories']}")
    logger.info(f"PDFs found: {stats['pdfs_found']}, parsed: {stats['pdfs_parsed']}")
    logger.info(f"Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")
    logger.info(f"Parse failed: {stats['parse_failed']}, No PDF: {stats['no_pdf']}")
    logger.info(f"Estimated API cost: ${api_cost:.3f}")

    # Unmatched list
    unmatched = [m for m in match_log if m.get("method") == "no_match"]
    if unmatched:
        logger.info(f"Unmatched utilities:")
        for u in unmatched[:15]:
            logger.info(f"  {u['name']} (@10CCF=${u.get('bill_10ccf', '?')})")

    if dry_run:
        logger.info("\n[DRY RUN] Sample records:")
        for r in records[:10]:
            logger.info(
                f"  {r['pwsid']} | ${r['bill_5ccf']:.2f}→${r['bill_10ccf']:.2f}→${r['bill_20ccf']:.2f} | "
                f"tiers={r['tier_count']} | [{r['confidence']}]"
            )
        if records:
            bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"]]
            if bills:
                import statistics
                logger.info(
                    f"Bill @10CCF: n={len(bills)}, median=${statistics.median(bills):.2f}, "
                    f"avg=${sum(bills)/len(bills):.2f}, min=${min(bills):.2f}, max=${max(bills):.2f}"
                )
        stats["inserted"] = 0
        return stats

    # Write to DB
    schema = settings.utility_schema
    with engine.connect() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {schema}.rate_schedules
            WHERE source_key = :source_key
        """), {"source_key": SOURCE_KEY}).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing {SOURCE_KEY} records")

        for record in records:
            conn.execute(text(f"""
                INSERT INTO {schema}.rate_schedules (
                    pwsid, source_key, vintage_date, customer_class,
                    billing_frequency, rate_structure_type,
                    fixed_charges, volumetric_tiers, surcharges,
                    bill_5ccf, bill_10ccf, bill_20ccf,
                    conservation_signal, tier_count,
                    source_url, confidence, parse_notes, needs_review
                ) VALUES (
                    :pwsid, :source_key, :vintage_date, :customer_class,
                    :billing_frequency, :rate_structure_type,
                    CAST(:fixed_charges AS jsonb), CAST(:volumetric_tiers AS jsonb),
                    CAST(:surcharges AS jsonb),
                    :bill_5ccf, :bill_10ccf, :bill_20ccf,
                    :conservation_signal, :tier_count,
                    :source_url, :confidence, :parse_notes, :needs_review
                )
            """), record)

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
            "step": "ky-psc-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source_key={SOURCE_KEY}, directories={stats['total_directories']}, "
                f"pdfs_found={stats['pdfs_found']}, parsed={stats['pdfs_parsed']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"unmatched={stats['unmatched']}, parse_failed={stats['parse_failed']}, "
                f"api_cost=${api_cost:.3f}, elapsed={elapsed:.1f}s"
            ),
        })
        conn.commit()

    logger.info(f"=== Kentucky PSC Ingest Complete ({elapsed:.1f}s) ===")
    return stats

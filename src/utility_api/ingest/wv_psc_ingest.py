#!/usr/bin/env python3
"""
WV Public Service Commission Water Rate Ingest

Purpose:
    Scrapes water utility cost rankings from the West Virginia Public
    Service Commission website and ingests into the utility.water_rates
    table.

    The WV PSC publishes cost rankings at two consumption levels
    (3,400 and 4,000 gallons) with minimum charges for all 325
    certificated water utilities. This gives us two bill data points
    per utility, enough to compute an implied volumetric rate and
    base charge.

    Since PSC data uses utility names (not PWSIDs), this module
    performs fuzzy name matching against EPA SDWIS records to resolve
    PWSIDs. Unmatched utilities are logged for manual review.

    Source: WV Public Service Commission
    URL: https://www.psc.state.wv.us/scripts/Utilities/rptWaterRankings4000.cfm
    Data vintage: March 2026 (updated continuously)

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - httpx (HTTP client)
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest wv-psc                    # Fetch + ingest
    ua-ingest wv-psc --dry-run          # Preview without DB writes
    ua-ingest wv-psc --refresh          # Force re-fetch from PSC

    # Python
    from utility_api.ingest.wv_psc_ingest import run_wv_psc_ingest
    run_wv_psc_ingest(dry_run=True)

Notes:
    - Two consumption levels scraped: 3,400 gal and 4,000 gal
    - From two data points we derive:
      * Fixed charge ≈ minimum charge (or extrapolated y-intercept)
      * Volumetric rate = (cost_4000 - cost_3400) / 600 gallons * 1000
      * bill_5ccf = interpolated from the two data points
      * bill_10ccf = extrapolated (less reliable)
    - Name matching: normalized fuzzy match against SDWIS pws_name
    - Utilities with type PSD/ASS/MUN/PRI — all included
    - WV county codes (3-letter) mapped to full county names

Data Sources:
    - Input: WV PSC rankings HTML tables (fetched → cached)
    - Input: utility.sdwis_systems (name matching)
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.water_rates table (source=wv_psc_2026)

Configuration:
    - HTML responses cached at data/raw/wv_psc/
    - Database connection via .env (DATABASE_URL)
"""

import json
import re
import time
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import httpx
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "wv_psc"
CACHE_FILE_3400 = DATA_DIR / "rankings_3400.html"
CACHE_FILE_4000 = DATA_DIR / "rankings_4000.html"
MATCH_LOG_FILE = DATA_DIR / "name_match_log.json"
SOURCE_TAG = "wv_psc_2026"

# PSC URLs
URL_3400 = "https://www.psc.state.wv.us/scripts/Utilities/rptWaterRankings3400.cfm"
URL_4000 = "https://www.psc.state.wv.us/scripts/Utilities/rptWaterRankings4000.cfm"

# Conversion factors
GAL_PER_CCF = 748.052
KGAL_TO_CCF = 0.748052

# WV county code → full name mapping (3-letter PSC codes)
WV_COUNTY_CODES = {
    "BAR": "Barbour", "BER": "Berkeley", "BOO": "Boone", "BRA": "Braxton",
    "BRO": "Brooke", "CAB": "Cabell", "CAL": "Calhoun", "CLA": "Clay",
    "DOD": "Doddridge", "FAY": "Fayette", "GIL": "Gilmer", "GRA": "Grant",
    "GRE": "Greenbrier", "HAM": "Hampshire", "HAN": "Hancock", "HAR": "Hardy",
    "HRR": "Harrison", "JAC": "Jackson", "JEF": "Jefferson", "KAN": "Kanawha",
    "LEW": "Lewis", "LIN": "Lincoln", "LOG": "Logan", "MAR": "Marion",
    "MAS": "Mason", "MCD": "McDowell", "MER": "Mercer", "MIN": "Mineral",
    "MNG": "Mingo", "MON": "Monongalia", "MNR": "Monroe", "MOR": "Morgan",
    "NIC": "Nicholas", "OHI": "Ohio", "PEN": "Pendleton", "PLE": "Pleasants",
    "POC": "Pocahontas", "PRE": "Preston", "PUT": "Putnam", "RAL": "Raleigh",
    "RAN": "Randolph", "RIT": "Ritchie", "ROA": "Roane", "SUM": "Summers",
    "TAY": "Taylor", "TUC": "Tucker", "TYL": "Tyler", "UPS": "Upshur",
    "WAY": "Wayne", "WEB": "Webster", "WET": "Wetzel", "WIR": "Wirt",
    "WOO": "Wood", "WYO": "Wyoming",
}


# --- HTML Table Parser ---


class PSCTableParser(HTMLParser):
    """Parse WV PSC water rankings HTML table into structured rows.

    The table has columns: TYPE, UTILITY (linked), COST/xxxx GAL,
    MINIMUM, CODE, Counties, RANK.
    """

    def __init__(self):
        super().__init__()
        self.rows = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_row = []
        self._current_cell = ""
        self._current_href = None
        self._row_count = 0
        self._skip_header = True

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self._in_table = True
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._current_row = []
        elif tag == "td" and self._in_row:
            self._in_cell = True
            self._current_cell = ""
            self._current_href = None
        elif tag == "a" and self._in_cell:
            self._current_href = attrs_dict.get("href", "")

    def handle_endtag(self, tag):
        if tag == "td" and self._in_cell:
            self._in_cell = False
            self._current_row.append({
                "text": self._current_cell.strip(),
                "href": self._current_href,
            })
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._current_row:
                if self._skip_header:
                    self._skip_header = False
                else:
                    self.rows.append(self._current_row)
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data


def _parse_rankings_html(html: str) -> list[dict]:
    """Parse WV PSC rankings HTML into structured dicts.

    The PSC page uses nested tables (one per county cell), making
    HTMLParser unreliable. We use regex extraction instead, which
    works reliably on the predictable ColdFusion-generated HTML.

    Each data row follows this pattern:
    <tr>
      <td>TYPE</td>
      <td><a href="...?TariffID=NNN">UTILITY NAME</a></td>
      <td>$XX.XX</td>  <!-- cost -->
      <td>$XX.XX</td>  <!-- minimum -->
      <td>CODE</td>
      <td colspan="3"><table>...<td>COUNTY</td>...</table></td>
      <td>RANK</td>
    </tr>

    Parameters
    ----------
    html : str
        Raw HTML from the rankings page.

    Returns
    -------
    list[dict]
        Each dict has: type, utility_name, tariff_id, cost, minimum,
        code, counties_raw, rank.
    """
    results = []

    # Match data rows: TariffID link is the reliable anchor
    # Pattern: TYPE cell, then link with TariffID, then cost cells, then rank
    row_pattern = re.compile(
        r'<tr>\s*'
        r'<td[^>]*>((?:PSD|MUN|ASS|PRI)\b[^<]*)</td>\s*'  # TYPE
        r'<td[^>]*><a\s+href="[^"]*TariffID=(\d+)"[^>]*>'  # TariffID
        r'([^<]+)</a>[^<]*</td>\s*'  # UTILITY NAME
        r'<td[^>]*>\$?([\d,.]+)[^<]*</td>\s*'  # COST
        r'<td[^>]*>\$?([\d,.]+)[^<]*</td>\s*'  # MINIMUM
        r'<td[^>]*>([^<]*)</td>\s*'  # CODE
        r'<td[^>]*colspan="3">\s*<table><tr>(.*?)</tr></table></td>\s*'  # COUNTIES (nested table)
        r'<td[^>]*>(\d+)</td>',  # RANK
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        util_type = match.group(1).strip().rstrip("\xa0").strip()
        tariff_id = int(match.group(2))
        utility_name = match.group(3).strip()
        cost_str = match.group(4).replace(",", "")
        min_str = match.group(5).replace(",", "")
        code = match.group(6).strip().rstrip("\xa0").strip()
        counties_html = match.group(7)
        rank = int(match.group(8))

        # Parse cost
        try:
            cost = float(cost_str)
        except ValueError:
            cost = None

        # Parse minimum
        try:
            minimum = float(min_str)
        except ValueError:
            minimum = None

        # Extract county codes from nested table cells
        county_codes = re.findall(r'<td>\s*([A-Z]{3})\s', counties_html)
        counties_raw = ",".join(county_codes)

        results.append({
            "type": util_type,
            "utility_name": utility_name,
            "tariff_id": tariff_id,
            "cost": cost,
            "minimum": minimum,
            "code": code,
            "counties_raw": counties_raw,
            "rank": rank,
        })

    return results


# --- Data Fetching ---


def _fetch_rankings(url: str, cache_file: Path, refresh: bool) -> str:
    """Fetch rankings HTML from PSC, with caching.

    Parameters
    ----------
    url : str
        PSC rankings URL.
    cache_file : Path
        Local cache path.
    refresh : bool
        Force re-fetch if True.

    Returns
    -------
    str
        HTML content.
    """
    if cache_file.exists() and not refresh:
        logger.info(f"Loading cached: {cache_file.name}")
        return cache_file.read_text(encoding="utf-8")

    logger.info(f"Fetching: {url}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = httpx.get(url, timeout=30.0, follow_redirects=True, headers=headers)
    resp.raise_for_status()
    html = resp.text

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(html, encoding="utf-8")
    logger.info(f"Cached to {cache_file.name}")

    return html


def fetch_wv_psc_data(refresh: bool = False) -> dict:
    """Fetch and parse both consumption level rankings.

    Attempts to fetch both 3,400 and 4,000 gallon rankings. The 3,400
    page may not always be available (intermittent 404), so the 4,000
    page is the primary source and the 3,400 page is optional enrichment.

    Parameters
    ----------
    refresh : bool
        Force re-fetch from PSC website.

    Returns
    -------
    dict
        Keys: 'rankings_3400', 'rankings_4000' — each a list of dicts.
        rankings_3400 may be empty if the page is unavailable.
    """
    # 4000 gal is the primary page — must succeed
    html_4000 = _fetch_rankings(URL_4000, CACHE_FILE_4000, refresh)
    r4000 = _parse_rankings_html(html_4000)
    logger.info(f"Parsed: {len(r4000)} utilities at 4000 gal")

    # 3400 gal is optional enrichment
    r3400 = []
    try:
        time.sleep(1.0)  # Polite delay
        html_3400 = _fetch_rankings(URL_3400, CACHE_FILE_3400, refresh)
        r3400 = _parse_rankings_html(html_3400)
        logger.info(f"Parsed: {len(r3400)} utilities at 3400 gal")
    except Exception as e:
        logger.warning(f"3400-gal page unavailable ({e}); proceeding with 4000-gal only")

    return {"rankings_3400": r3400, "rankings_4000": r4000}


# --- Name Matching ---


def _normalize_name(name: str) -> str:
    """Normalize a utility name for fuzzy matching.

    Strips common suffixes (PSD, INC, LLC, etc.), lowercases,
    removes punctuation and extra whitespace.

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

    # Remove common suffixes/prefixes
    removals = [
        r"\bPUBLIC\s+SERVICE\s+DISTRICT\b",
        r"\bP\.?\s*S\.?\s*D\.?\b",
        r"\bWATER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS|ASSOC|ASSOCIATION|UTILITY|UTILITIES|DIST|DISTRICT)\b",
        r"\bSEWER\s+(DEPARTMENT|DEPT|COMPANY|CO|SYSTEM|WORKS)\b",
        r"\bWATER\s+AND\s+SEWER\b",
        r"\bTOWN\s+OF\b",
        r"\bCITY\s+OF\b",
        r"\bVILLAGE\s+OF\b",
        r"\bMUNICIPAL\s+(WATER|UTILITIES|UTILITY)\b",
        r"\b(INC|LLC|L\.L\.C)\b\.?",
        r"\bCOMMUNITY\s+(WATER|FACIL|FACILITIES)\b",
        r"\bIMPROVEMENT\b",
    ]

    for pattern in removals:
        s = re.sub(pattern, "", s)

    # Remove parenthetical city names (SDWIS puts city in parens)
    s = re.sub(r"\(.*?\)", "", s)

    # Remove punctuation and normalize whitespace
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _build_sdwis_lookup(conn) -> dict:
    """Build normalized name → (pwsid, raw_name) lookup from SDWIS.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Active database connection.

    Returns
    -------
    dict
        Mapping of normalized_name → list of (pwsid, raw_name) tuples.
        Multiple PWSIDs can share a normalized name.
    """
    schema = settings.utility_schema
    rows = conn.execute(text(f"""
        SELECT s.pwsid, s.pws_name, s.city
        FROM {schema}.sdwis_systems s
        JOIN {schema}.cws_boundaries c ON s.pwsid = c.pwsid
        WHERE s.state_code = 'WV' AND s.pws_type_code = 'CWS'
    """)).fetchall()

    lookup = {}
    for pwsid, pws_name, city in rows:
        norm = _normalize_name(pws_name)
        if norm not in lookup:
            lookup[norm] = []
        lookup[norm].append((pwsid, pws_name))

    return lookup


def _match_utility_to_pwsid(
    psc_name: str,
    psc_counties: str,
    sdwis_lookup: dict,
) -> tuple[str | None, str | None, str]:
    """Match a PSC utility name to a SDWIS PWSID.

    Uses progressively relaxed matching:
    1. Exact normalized match
    2. Substring containment (PSC name in SDWIS or vice versa)
    3. Word overlap scoring

    Parameters
    ----------
    psc_name : str
        Utility name from PSC rankings.
    psc_counties : str
        County codes from PSC (e.g., "KAN,PUT").
    sdwis_lookup : dict
        Normalized name → [(pwsid, raw_name)] from SDWIS.

    Returns
    -------
    tuple[str | None, str | None, str]
        (pwsid, sdwis_name, match_method) — None if no match found.
    """
    psc_norm = _normalize_name(psc_name)

    if not psc_norm:
        return None, None, "empty_name"

    # 1. Exact normalized match
    if psc_norm in sdwis_lookup:
        candidates = sdwis_lookup[psc_norm]
        return candidates[0][0], candidates[0][1], "exact"

    # 2. Substring containment
    for norm_key, candidates in sdwis_lookup.items():
        if psc_norm in norm_key or norm_key in psc_norm:
            if len(psc_norm) >= 4 and len(norm_key) >= 4:  # avoid trivial matches
                return candidates[0][0], candidates[0][1], "substring"

    # 3. Word overlap scoring
    psc_words = set(psc_norm.split())
    best_score = 0
    best_match = None

    for norm_key, candidates in sdwis_lookup.items():
        sdwis_words = set(norm_key.split())
        if not psc_words or not sdwis_words:
            continue

        # Jaccard-like: intersection / union, weighted by word length
        overlap = psc_words & sdwis_words
        if not overlap:
            continue

        # Require at least one significant word overlap (>3 chars)
        significant = any(len(w) > 3 for w in overlap)
        if not significant:
            continue

        overlap_chars = sum(len(w) for w in overlap)
        total_chars = sum(len(w) for w in psc_words | sdwis_words)
        score = overlap_chars / total_chars if total_chars > 0 else 0

        if score > best_score and score >= 0.5:
            best_score = score
            best_match = (candidates[0][0], candidates[0][1])

    if best_match:
        return best_match[0], best_match[1], f"word_overlap({best_score:.2f})"

    return None, None, "no_match"


# --- Rate Computation ---


def _compute_rates(cost_3400: float | None, cost_4000: float | None, minimum: float | None) -> dict:
    """Compute rate structure from two consumption-level costs.

    From two data points (cost at 3,400 gal and 4,000 gal), we can
    derive:
    - Implied volumetric rate per 1000 gal
    - Fixed charge (extrapolated y-intercept, or minimum charge)
    - Bills at standard CCF levels (interpolation/extrapolation)

    Parameters
    ----------
    cost_3400 : float | None
        Total bill at 3,400 gallons/month.
    cost_4000 : float | None
        Total bill at 4,000 gallons/month.
    minimum : float | None
        Minimum monthly charge.

    Returns
    -------
    dict
        Contains: volumetric_rate_per_kgal, fixed_charge_monthly,
        bill_5ccf, bill_10ccf, tier_1_rate (CCF), structure_type.
    """
    result = {
        "volumetric_rate_per_kgal": None,
        "fixed_charge_monthly": minimum,
        "bill_5ccf": None,
        "bill_10ccf": None,
        "tier_1_rate": None,
        "structure_type": None,
    }

    if cost_3400 is not None and cost_4000 is not None:
        # Implied volumetric rate from the 600-gal increment
        vol_rate_per_gal = (cost_4000 - cost_3400) / 600.0

        if vol_rate_per_gal < 0:
            # Declining block or tier boundary artifact — can't reliably
            # derive structure from 2 points. Use average rate from 4000-gal
            # point instead, but flag as declining_block.
            avg_rate_per_gal = cost_4000 / 4000.0
            result["volumetric_rate_per_kgal"] = round(avg_rate_per_gal * 1000.0, 2)
            result["tier_1_rate"] = round(avg_rate_per_gal * 1000.0 * KGAL_TO_CCF, 4)
            result["fixed_charge_monthly"] = minimum or 0
            result["bill_5ccf"] = round(cost_4000 * (5.0 * GAL_PER_CCF / 4000.0), 2)
            result["bill_10ccf"] = round(cost_4000 * (10.0 * GAL_PER_CCF / 4000.0), 2)
            result["structure_type"] = "decreasing_block"

        elif abs(vol_rate_per_gal) < 0.0001:
            # Zero volumetric rate — flat rate system
            result["fixed_charge_monthly"] = round(cost_4000, 2)
            result["tier_1_rate"] = None
            result["bill_5ccf"] = round(cost_4000, 2)
            result["bill_10ccf"] = round(cost_4000, 2)
            result["structure_type"] = "flat"

        else:
            # Positive volumetric rate — standard uniform
            vol_rate_per_kgal = vol_rate_per_gal * 1000.0
            result["volumetric_rate_per_kgal"] = round(vol_rate_per_kgal, 2)
            result["tier_1_rate"] = round(vol_rate_per_kgal * KGAL_TO_CCF, 4)

            # Extrapolate fixed charge from y-intercept
            implied_fixed = cost_4000 - vol_rate_per_gal * 4000
            if implied_fixed >= 0:
                result["fixed_charge_monthly"] = round(implied_fixed, 2)
            # If implied_fixed is negative, keep minimum charge

            # Compute bills at standard consumption levels
            gal_5ccf = 5.0 * GAL_PER_CCF  # 3,740.26 gal
            result["bill_5ccf"] = round(
                (result["fixed_charge_monthly"] or 0) + vol_rate_per_gal * gal_5ccf, 2
            )

            gal_10ccf = 10.0 * GAL_PER_CCF  # 7,480.52 gal
            result["bill_10ccf"] = round(
                (result["fixed_charge_monthly"] or 0) + vol_rate_per_gal * gal_10ccf, 2
            )
            result["structure_type"] = "uniform"

    elif cost_4000 is not None:
        # Only one data point — use average rate as approximation
        rate_per_gal = cost_4000 / 4000.0
        result["bill_5ccf"] = round(rate_per_gal * 5.0 * GAL_PER_CCF, 2)
        result["bill_10ccf"] = round(rate_per_gal * 10.0 * GAL_PER_CCF, 2)
        result["structure_type"] = "uniform"

    return result


# --- Main Ingest ---


def run_wv_psc_ingest(
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run the WV PSC water rate ingest.

    Parameters
    ----------
    dry_run : bool
        If True, fetch and parse but don't write to DB.
    refresh : bool
        If True, force re-fetch from PSC website.

    Returns
    -------
    dict
        Summary stats.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== WV PSC Rate Ingest Starting ===")

    # Step 1: Fetch data
    data = fetch_wv_psc_data(refresh=refresh)
    rankings_3400 = data["rankings_3400"]
    rankings_4000 = data["rankings_4000"]

    # Step 2: Merge the two consumption levels by utility name
    # Build lookup: utility_name → cost_3400
    lookup_3400 = {}
    for r in rankings_3400:
        lookup_3400[r["utility_name"]] = r["cost"]

    # Step 3: Build SDWIS name lookup for matching
    with engine.connect() as conn:
        sdwis_lookup = _build_sdwis_lookup(conn)
    logger.info(f"SDWIS lookup: {sum(len(v) for v in sdwis_lookup.values())} WV CWS systems")

    # Step 4: Parse and match
    stats = {
        "total_psc_utilities": len(rankings_4000),
        "matched": 0,
        "inserted": 0,
        "unmatched": 0,
        "duplicate_pwsids": [],
    }

    records = []
    seen_pwsids = {}
    match_log = []  # Log all matches for review

    for r in rankings_4000:
        psc_name = r["utility_name"]
        cost_4000 = r["cost"]
        cost_3400 = lookup_3400.get(psc_name)
        minimum = r["minimum"]
        counties_raw = r["counties_raw"]

        # Resolve county names
        county_codes = [c.strip() for c in counties_raw.split(",") if c.strip()]
        county_names = [WV_COUNTY_CODES.get(c, c) for c in county_codes]
        county = ", ".join(county_names) if county_names else None

        # Match to PWSID
        pwsid, sdwis_name, method = _match_utility_to_pwsid(
            psc_name, counties_raw, sdwis_lookup
        )

        match_log.append({
            "psc_name": psc_name,
            "pwsid": pwsid,
            "sdwis_name": sdwis_name,
            "method": method,
            "cost_4000": cost_4000,
            "cost_3400": cost_3400,
        })

        if pwsid is None:
            stats["unmatched"] += 1
            continue

        # Dedup
        if pwsid in seen_pwsids:
            prev_name = seen_pwsids[pwsid]
            stats["duplicate_pwsids"].append({
                "pwsid": pwsid,
                "kept": prev_name,
                "skipped": psc_name,
            })
            continue
        seen_pwsids[pwsid] = psc_name

        # Compute rates
        rates = _compute_rates(cost_3400, cost_4000, minimum)

        # Confidence based on data availability
        if cost_3400 is not None and cost_4000 is not None:
            if "exact" in method:
                confidence = "high"
            else:
                confidence = "medium"
        else:
            confidence = "low"

        # Notes
        notes_parts = []
        notes_parts.append(f"Name match: {method} (PSC: '{psc_name}' → SDWIS: '{sdwis_name}')")
        if r["type"]:
            type_map = {"PSD": "Public Service District", "MUN": "Municipal",
                        "ASS": "Association", "PRI": "Private"}
            notes_parts.append(f"Type: {type_map.get(r['type'], r['type'])}")
        if cost_3400 is not None:
            notes_parts.append(f"PSC cost@3400gal=${cost_3400:.2f}")
        if cost_4000 is not None:
            notes_parts.append(f"PSC cost@4000gal=${cost_4000:.2f}")
        if r["rank"]:
            notes_parts.append(f"PSC rank: {r['rank']}/325")

        records.append({
            "pwsid": pwsid,
            "utility_name": psc_name,
            "state_code": "WV",
            "county": county,
            "rate_effective_date": date(2026, 3, 20),  # PSC page date
            "rate_structure_type": rates["structure_type"],
            "rate_class": "residential",
            "billing_frequency": "monthly",
            "fixed_charge_monthly": rates["fixed_charge_monthly"],
            "meter_size_inches": None,
            "tier_1_limit_ccf": None,  # uniform assumed
            "tier_1_rate": rates["tier_1_rate"],
            "tier_2_limit_ccf": None,
            "tier_2_rate": None,
            "tier_3_limit_ccf": None,
            "tier_3_rate": None,
            "tier_4_limit_ccf": None,
            "tier_4_rate": None,
            "bill_5ccf": rates["bill_5ccf"],
            "bill_10ccf": rates["bill_10ccf"],
            "bill_6ccf": None,
            "bill_9ccf": None,
            "bill_12ccf": None,
            "bill_24ccf": None,
            "source": SOURCE_TAG,
            "source_url": "https://www.psc.state.wv.us/scripts/Utilities/rptWaterRankings4000.cfm",
            "raw_text_hash": None,
            "parse_confidence": confidence,
            "parse_model": None,
            "parse_notes": "; ".join(notes_parts),
        })
        stats["matched"] += 1

    # Save match log for review
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MATCH_LOG_FILE, "w") as f:
        json.dump(match_log, f, indent=2)
    logger.info(f"Match log saved to {MATCH_LOG_FILE.name}")

    logger.info(f"Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")
    if stats["duplicate_pwsids"]:
        logger.info(f"Duplicate PWSIDs: {len(stats['duplicate_pwsids'])}")

    # Log unmatched for review
    unmatched = [m for m in match_log if m["pwsid"] is None]
    if unmatched:
        logger.info(f"Unmatched utilities ({len(unmatched)}):")
        for u in unmatched[:15]:
            logger.info(f"  {u['psc_name']} (cost@4k=${u['cost_4000']})")
        if len(unmatched) > 15:
            logger.info(f"  ... and {len(unmatched) - 15} more (see {MATCH_LOG_FILE.name})")

    # Dry run: show samples
    if dry_run:
        logger.info("[DRY RUN] Sample records:")
        for r in records[:8]:
            logger.info(
                f"  {r['pwsid']} | {r['utility_name'][:30]:30s} | "
                f"base=${r['fixed_charge_monthly'] or 0:.2f} | "
                f"T1@${r['tier_1_rate'] or 0:.4f}/CCF | "
                f"@5ccf=${r['bill_5ccf'] or 0:.2f} | "
                f"@10ccf=${r['bill_10ccf'] or 0:.2f} | "
                f"[{r['parse_confidence']}]"
            )
        if len(records) > 8:
            logger.info(f"  ... and {len(records) - 8} more")

        # Summary stats
        if records:
            bills_10 = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
            if bills_10:
                logger.info(
                    f"Bill @10CCF stats: avg=${sum(bills_10)/len(bills_10):.2f}, "
                    f"min=${min(bills_10):.2f}, max=${max(bills_10):.2f}"
                )

        stats["inserted"] = 0
        return stats

    # Step 5: Write to database
    schema = settings.utility_schema

    with engine.connect() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {schema}.water_rates
            WHERE source = :source
        """), {"source": SOURCE_TAG}).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing {SOURCE_TAG} records")

        for record in records:
            conn.execute(text(f"""
                INSERT INTO {schema}.water_rates (
                    pwsid, utility_name, state_code, county,
                    rate_effective_date, rate_structure_type, rate_class,
                    billing_frequency,
                    fixed_charge_monthly, meter_size_inches,
                    tier_1_limit_ccf, tier_1_rate,
                    tier_2_limit_ccf, tier_2_rate,
                    tier_3_limit_ccf, tier_3_rate,
                    tier_4_limit_ccf, tier_4_rate,
                    bill_5ccf, bill_10ccf,
                    bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf,
                    source, source_url, raw_text_hash,
                    parse_confidence, parse_model, parse_notes
                ) VALUES (
                    :pwsid, :utility_name, :state_code, :county,
                    :rate_effective_date, :rate_structure_type, :rate_class,
                    :billing_frequency,
                    :fixed_charge_monthly, :meter_size_inches,
                    :tier_1_limit_ccf, :tier_1_rate,
                    :tier_2_limit_ccf, :tier_2_rate,
                    :tier_3_limit_ccf, :tier_3_rate,
                    :tier_4_limit_ccf, :tier_4_rate,
                    :bill_5ccf, :bill_10ccf,
                    :bill_6ccf, :bill_9ccf, :bill_12ccf, :bill_24ccf,
                    :source, :source_url, :raw_text_hash,
                    :parse_confidence, :parse_model, :parse_notes
                )
            """), record)

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
            "step": "wv-psc-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={SOURCE_TAG}, psc_utilities={stats['total_psc_utilities']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"unmatched={stats['unmatched']}, "
                f"duplicates={len(stats['duplicate_pwsids'])}"
            ),
        })
        conn.commit()

    logger.info(f"=== WV PSC Ingest Complete ({elapsed:.1f}s) ===")
    return stats

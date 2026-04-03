#!/usr/bin/env python3
"""
Locality Discovery Agent

Purpose:
    Fallback discovery agent that reformulates search queries using the
    municipality/locality name extracted from the formal PWSID system name.
    Runs AFTER the standard DiscoveryAgent has failed — finds URLs that
    standard PWSID-name-based queries miss.

    The key insight: SDWIS names like "SCHENECTADY CITY WATER WORKS" don't
    match how utilities present themselves online. Searching for "Schenectady
    water rates" finds the actual municipal rate page that the formal name misses.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-03 (Sprint 29: CO-specific suffix/institutional patterns)

Dependencies:
    - sqlalchemy
    - requests (via SerperSearchClient)
    - pyyaml

Usage:
    from utility_api.agents.locality_discovery import LocalityDiscoveryAgent
    agent = LocalityDiscoveryAgent()

    # Single PWSID
    result = agent.run(pwsid='NY4600070')

    # Extract municipality name only (for dry-run preview)
    from utility_api.agents.locality_discovery import extract_municipality
    name = extract_municipality("SCHENECTADY CITY WATER WORKS")
    # → "Schenectady"

Notes:
    - Does NOT replace standard DiscoveryAgent — additive fallback only
    - Writes to scrape_registry with url_source='locality_discovery'
    - Skips PSC-regulated private companies (Veolia, Aqua, Liberty, SUEZ)
    - Reuses score_url_relevance() from discovery.py with locality-specific bonuses
    - Triggered when standard pipeline has exhausted all ranks with no parsed rate

Data Sources:
    - utility.cws_boundaries (pws_name, county_served)
    - utility.sdwis_systems (city, owner_type_code)
    - utility.scrape_registry (existing URLs for cross-contamination check)

Configuration:
    - config/agent_config.yaml (Serper settings shared with DiscoveryAgent)
"""

import re
import time
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.agents.discovery import (
    _DISCOVERY_CONFIG,
    _get_system_metadata,
    score_url_relevance,
)
from utility_api.config import settings
from utility_api.db import engine


# --- Private Company Detection ---

# PSC-regulated private companies — these need tariff lookup, not web scraping.
# Municipality name extraction doesn't apply to corporate entities.
_PRIVATE_COMPANY_PATTERNS = [
    r"\bVEOLIA\b",
    r"\bAQUA\s+(AMERICA|UTILITIES|NY|PA|NJ|OH|IL|TX)\b",
    r"\bLIBERTY\s+UTILITIES\b",
    r"\bSUEZ\b",
    r"\bAMERICAN WATER\b",
    r"\bCAL[- ]AM\b",
    r"\bCONNECTICUT WATER\b",
    r"\bSJW\b",
    r"\bESSENTIAL UTILITIES\b",
    r"\bUTILITY,?\s+INC\b",
    r"\bCORP(ORATION)?\b",
    r",?\s+INC\.?\s*$",
    r",?\s+LLC\.?\s*$",
]

_PRIVATE_RE = re.compile("|".join(_PRIVATE_COMPANY_PATTERNS), re.IGNORECASE)


# --- Municipality Suffix/Prefix Stripping ---

# Suffixes to strip (order matters — longest first to avoid partial matches)
_SUFFIXES = [
    # Multi-word suffixes first (longest match wins)
    "JOINT WATER WORKS",
    "WATER AND SEWER AUTHORITY",
    "WATER & SEWER AUTHORITY",
    "WATER SUPPLY SYSTEM",
    "WATER SUPPLY DISTRICT",
    "WATER SUPPLY",
    "WATER WORKS",
    "WATER DEPARTMENT",
    "WATER AUTHORITY",
    "WATER DISTRICT",
    "WATER DEPT",
    "WATER SYSTEM",
    "WATER UTILITY",
    "WATER SERVICE",
    "WATER DIST",
    "CONSOLIDATED WATER DISTRICT",
    "CONSOLIDATED WD",
    "CONSOLIDATEDWD",
    "CONSOLD. WATER DIST.",
    "CONSOLIDATED W.D.",
    "CONSOLIDATED W.D",
    "CONS. W.D.",
    "CONS. WD",
    "CONS WD",
    "CONS. WATER DIST.",
    "UTILITY DISTRICT",
    "PUBLIC WATER SUPPLY",
    "CITY WATER WORKS",
    "CITY PWS",
    "CITY WATER",
    "TOWN CONS. WD",
    "TOWN WD",
    # Abbreviated single-word/short suffixes
    "PWS",
    "WSD",
    "CSA",
    "WD",
    "W.D.",
    "W.D",
    "CWS",
    "WSA",
    # Sprint 29: CO-specific suffixes
    "WWWA",  # Water/Wastewater Authority (e.g. ARAPAHOE CNTY WWWA)
    "WWSA",  # Water/Wastewater Service Authority
    "MD NO",  # Metropolitan District No. X (e.g. SUPERIOR MD NO 1)
    "MD",     # Metropolitan District (e.g. SUPERIOR MD)
]

_PREFIXES = [
    "CITY OF",
    "TOWN OF",
    "VILLAGE OF",
    "BOROUGH OF",
    "HAMLET OF",
]

# Parenthetical content to strip: "(VILLAGE)", "(PURCHASE TROY)", "(SCWA)", etc.
# Strips ALL parenthetical suffixes — these are metadata, not locality names
_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*", re.IGNORECASE)

# Trailing district number: "#1", "#11", "WD #11", "DIST.#1"
_DISTRICT_NUM_RE = re.compile(r"\s*#\s*\d+\s*$")

# Trailing comma-separated district numbers: "1, 2, 4" or "1,2,4"
_COMMA_DISTRICT_RE = re.compile(r"\s+\d+(?:\s*,\s*\d+)+\s*$")

# Trailing single digit district number (after WD/suffix stripping): "GLENVILLE 11"
_TRAILING_NUM_RE = re.compile(r"\s+\d+\s*$")


def extract_municipality(pws_name: str, county: str | None = None) -> str | None:
    """Extract locality name from formal PWSID system name.

    Returns the human-recognizable municipality/locality name suitable for
    web search queries, or None if the name is a private company or
    otherwise unsuitable for locality-based discovery.

    Parameters
    ----------
    pws_name : str
        Formal SDWIS system name (e.g., "SCHENECTADY CITY WATER WORKS").
    county : str, optional
        County name from cws_boundaries — used for county-level districts.

    Returns
    -------
    str or None
        Title-cased locality name, or None if not extractable.

    Examples
    --------
    >>> extract_municipality("SCHENECTADY CITY WATER WORKS")
    'Schenectady'
    >>> extract_municipality("SUFFOLK COUNTY WATER AUTHORITY")
    'Suffolk County'
    >>> extract_municipality("TROY CITY PWS")
    'Troy'
    >>> extract_municipality("NEW WINDSOR CONSOLIDATED WD")
    'New Windsor'
    >>> extract_municipality("VEOLIA WATER NEW YORK, INC. RD-2")
    None
    >>> extract_municipality("CLIFTON PARK WATER AUTHORITY")
    'Clifton Park'
    >>> extract_municipality("YORKTOWN CONSOLD. WATER DIST.#1")
    'Yorktown'
    >>> extract_municipality("SLEEPY HOLLOW (VILLAGE)")
    'Sleepy Hollow'
    >>> extract_municipality("FORT DRUM")
    None
    >>> extract_municipality("CORNELL UNIVERSITY")
    None
    >>> extract_municipality("BROOKHAVEN NATIONAL LABORATORY")
    None
    >>> extract_municipality("WESTCHESTER JOINT WATER WORKS")
    'Westchester'
    >>> extract_municipality("SARANAC LAKE V")
    'Saranac Lake'
    >>> extract_municipality("LIBERTY VILLAGE")
    'Liberty'
    >>> extract_municipality("LERAY TOWN WD 1, 2, 4")
    'Leray'
    >>> extract_municipality("SODUS-HURON-WOLCOTT-BUTLER CSA")
    'Sodus-Huron-Wolcott-Butler'
    >>> extract_municipality("NEW CASTLE/STANWOOD W.D.")
    'New Castle'
    >>> extract_municipality("WATERFORD WATER WORKS (PURCHASE TROY)")
    'Waterford'
    >>> extract_municipality("STILLWATER TOWN (SCWA)")
    'Stillwater'
    >>> extract_municipality("GREATER PLATTSBURGH WATER DISTRICT")
    'Plattsburgh'
    """
    if not pws_name:
        return None

    name = pws_name.strip().upper()

    # --- Step 1: Detect private companies → return None ---
    if _PRIVATE_RE.search(name):
        return None

    # --- Step 2: Detect federal/military/institutional facilities → return None ---
    institutional_markers = [
        "UNIVERSITY", "COLLEGE", "NATIONAL LABORATORY", "NATIONAL LAB",
        "CORRECTIONAL", "PRISON", "MILITARY", "AIR FORCE", "NAVAL",
        "U.S.M.A.", "USMA", "FORT DRUM", "WEST POINT",
        "CAMP ", "BASE ",
        # Sprint 29: CO-specific institutional patterns
        "CSU MAIN", "CSU CAMPUS", "YMCA ", "HOUSING CAMPUS",
    ]
    if any(marker in name for marker in institutional_markers):
        return None

    # --- Step 3: Strip district numbers ---
    # "#1", "#11" etc.
    cleaned = _DISTRICT_NUM_RE.sub("", name).strip()
    # "1, 2, 4" trailing comma-separated district numbers
    cleaned = _COMMA_DISTRICT_RE.sub("", cleaned).strip()
    # Sprint 29: "NO 1", "NO 11" — e.g. "SUPERIOR MD NO 1"
    cleaned = re.sub(r"\s+NO\s+\d+\s*$", "", cleaned, flags=re.IGNORECASE).strip()

    # --- Step 4: Strip ALL parenthetical content ---
    # "(VILLAGE)", "(PURCHASE TROY)", "(SCWA)", "(QUEENSBURY)", "(C)", "(V)" etc.
    cleaned = _PAREN_RE.sub(" ", cleaned).strip()

    # --- Step 5: Strip suffixes (longest first) ---
    for suffix in _SUFFIXES:
        if cleaned.upper().endswith(suffix):
            cleaned = cleaned[: -len(suffix)].strip()
            # Remove trailing comma, dash, or period
            cleaned = cleaned.rstrip(",-. ")
            break

    # --- Step 6: Strip prefixes ---
    for prefix in _PREFIXES:
        if cleaned.upper().startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break

    # --- Step 6b (Sprint 29): Handle "X CITY OF" / "X TOWN OF" suffix ---
    # CO/western US SDWIS convention: "AURORA CITY OF", "FT COLLINS CITY OF"
    for trailing in ["CITY AND COUNTY OF", "CITY OF", "TOWN OF", "VILLAGE OF"]:
        if cleaned.upper().endswith(f" {trailing}"):
            cleaned = cleaned[: -(len(trailing) + 1)].strip()
            break

    # --- Step 7: Handle "CITY" as suffix (e.g., "WHITE PLAINS CITY", "TROY CITY") ---
    if cleaned.upper().endswith(" CITY"):
        cleaned = cleaned[:-5].strip()

    # --- Step 8: Handle "VILLAGE" as suffix ---
    if cleaned.upper().endswith(" VILLAGE"):
        cleaned = cleaned[:-8].strip()

    # --- Step 9: Handle trailing "V" abbreviation for Village ---
    # "SARANAC LAKE V", "LAKE PLACID V", "HOOSICK FALLS (V) PWS"
    if re.match(r".+\s+V$", cleaned.upper()):
        cleaned = cleaned[:-2].strip()

    # --- Step 10: Handle "TOWN" as suffix (but not compound names like "JAMESTOWN") ---
    if cleaned.upper().endswith(" TOWN") and len(cleaned) > 9:
        cleaned = cleaned[:-5].strip()

    # --- Step 11: Strip descriptor words that aren't locality names ---
    # "JOINT", "GREATER", "CONSOLIDATED" when leftover after suffix stripping
    for descriptor in ["JOINT", "GREATER", "CONSOLIDATED"]:
        if cleaned.upper().endswith(f" {descriptor}"):
            cleaned = cleaned[: -(len(descriptor) + 1)].strip()
        if cleaned.upper().startswith(f"{descriptor} "):
            cleaned = cleaned[len(descriptor) + 1:].strip()

    # --- Step 12: Strip trailing bare numbers (district IDs left after suffix strip) ---
    cleaned = _TRAILING_NUM_RE.sub("", cleaned).strip()

    # --- Step 13: Strip slash-separated compound names → take first part ---
    # "NEW CASTLE/STANWOOD" → "New Castle" (search for the primary locality)
    if "/" in cleaned:
        cleaned = cleaned.split("/")[0].strip()

    # --- Step 14: Handle county-level districts ---
    # Only add "County" if the original name explicitly had "COUNTY" in it
    # (not just because the extracted name happens to match the county field)
    if cleaned.upper().endswith(" COUNTY"):
        return cleaned.strip().title()

    # --- Step 15: Validate — reject if too short or empty ---
    cleaned = cleaned.strip()
    if not cleaned or len(cleaned) < 2:
        return None

    # --- Step 16: Reject single-character or empty results ---
    # Short real place names (Lee, Ava, Ada) are valid — don't reject by length
    # Only reject truly degenerate cases
    if len(cleaned) <= 1:
        return None

    # --- Step 16b (Sprint 29): Expand common abbreviations ---
    # FT → Fort, MT → Mount, ST → Saint (before title-casing)
    cleaned = re.sub(r"\bFT\b", "FORT", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bMT\b", "MOUNT", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bST\b", "SAINT", cleaned, flags=re.IGNORECASE)
    # CNTY → County
    cleaned = re.sub(r"\bCNTY\b", "COUNTY", cleaned, flags=re.IGNORECASE)

    # Title-case the result
    result = _title_case_locality(cleaned)

    return result


def _title_case_locality(name: str) -> str:
    """Smart title-casing for locality names.

    Handles:
        - Hyphenated: "CORNWALL-ON-HUDSON" → "Cornwall-on-Hudson"
        - Multi-word: "NEW WINDSOR" → "New Windsor"
        - Prepositions: keep "on", "of", "the" lowercase mid-name
    """
    small_words = {"on", "of", "the", "in", "at", "de", "du", "le", "la"}
    parts = name.split("-")
    result_parts = []

    for i, part in enumerate(parts):
        words = part.strip().split()
        cased_words = []
        for j, word in enumerate(words):
            lower = word.lower()
            if j > 0 and lower in small_words:
                cased_words.append(lower)
            else:
                cased_words.append(word.capitalize())
        result_parts.append(" ".join(cased_words))

    return "-".join(result_parts)


# --- Locality Query Builder ---

def build_locality_queries(
    municipality: str,
    state_code: str,
    county: str | None = None,
) -> list[str]:
    """Build search queries using the municipality name for locality discovery.

    Generates 4 queries designed to find the municipality's own website
    and rate pages, NOT state/county portals. For short or ambiguous names
    (e.g., "Lee", "Troy"), adds county context for disambiguation.

    Parameters
    ----------
    municipality : str
        Human-readable locality name (e.g., "Schenectady", "New Windsor").
    state_code : str
        2-letter state code.
    county : str, optional
        County name for disambiguation of short/ambiguous municipality names.

    Returns
    -------
    list[str]
        Up to 4 search query strings for Serper.
    """
    # Sprint 29: Use full state name for disambiguation (same as discovery.py)
    from utility_api.agents.discovery import _STATE_NAMES
    state_full = _STATE_NAMES.get(state_code.upper(), state_code) if state_code else state_code

    # Short or very common names need county context to disambiguate
    # (e.g., "Lee" alone returns results for Lee County FL, Lee MA, etc.)
    needs_disambiguation = len(municipality) <= 5 or municipality.lower() in {
        "troy", "clinton", "liberty", "cambridge", "avon", "malta",
        "highland", "warsaw", "carthage", "herkimer", "westchester",
        # Sprint 29: CO ambiguous names
        "aurora", "lafayette", "superior", "fountain", "brush",
        "lamar", "hayden", "palisade", "yuma", "lakewood",
    }

    if needs_disambiguation and county:
        # Add county to narrow results geographically
        geo_context = f"{county} County {state_full}"
        queries = [
            # Q1: Municipality + county + water rates (most specific)
            f'"{municipality}" {geo_context} water rates',

            # Q2: Municipality + state + water utility
            f'"{municipality}" {state_full} water utility rates',

            # Q3: Municipality + county + rate schedule (catches PDFs)
            f'"{municipality}" {geo_context} water rate schedule',

            # Q4: Site-restricted .gov with county context
            f'site:.gov "{municipality}" "{county}" water rates',
        ]
    else:
        queries = [
            # Q1: Direct municipality + water rates + state
            f'"{municipality}" water rates {state_full}',

            # Q2: Municipality + utility billing (catches billing/account pages)
            f'"{municipality}" {state_full} water utility billing rates',

            # Q3: Municipality + rate schedule (catches PDF rate schedules)
            f'"{municipality}" {state_full} water department rate schedule',

            # Q4: Site-restricted to .gov (strongly prefers municipal .gov domains)
            f'site:.gov "{municipality}" "{state_full}" water rates',
        ]

    return queries


# --- Locality-Enhanced URL Scoring ---

def score_locality_url(
    url: str,
    title: str,
    snippet: str,
    municipality: str,
    state_code: str,
    utility_name: str = "",
    city: str = "",
    cross_contamination_urls: set[str] | None = None,
) -> int:
    """Score URL relevance with locality-specific bonuses/penalties.

    Wraps the standard score_url_relevance() and adds:
    - Bonus: domain contains municipality name
    - Bonus: URL path contains rate/billing/water keywords
    - Penalty: domain is in blacklist
    - Penalty: URL appeared for 3+ different PWSIDs (cross-contamination)

    Parameters
    ----------
    url : str
        Candidate URL.
    title : str
        Serper result title.
    snippet : str
        Serper result snippet.
    municipality : str
        Extracted municipality name.
    state_code : str
        2-letter state code.
    utility_name : str
        Original SDWIS utility name (for base scoring).
    city : str
        City from SDWIS metadata.
    cross_contamination_urls : set[str], optional
        URLs that appeared for 3+ different PWSIDs in scrape_registry.

    Returns
    -------
    int
        Score 0-100.
    """
    # Start with base score from standard scoring
    base_score = score_url_relevance(url, title, snippet, utility_name, city, state_code)

    bonus = 0
    hostname = (urlparse(url).hostname or "").lower()

    # --- Locality name in domain bonus ---
    municipality_slug = re.sub(r"[^a-z0-9]", "", municipality.lower())
    domain_slug = re.sub(r"[^a-z0-9]", "", hostname.replace("www.", ""))
    if len(municipality_slug) > 3 and municipality_slug in domain_slug:
        bonus += 20

    # --- Cross-contamination penalty ---
    if cross_contamination_urls and url in cross_contamination_urls:
        bonus -= 15

    return max(0, min(100, base_score + bonus))


# --- Cross-Contamination Detection ---

def _get_cross_contamination_urls(schema: str) -> set[str]:
    """Find URLs that appeared for 3+ different PWSIDs in scrape_registry.

    These are likely regional/state portal pages, not utility-specific rate pages.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT url
                FROM {schema}.scrape_registry
                WHERE pwsid IS NOT NULL
                GROUP BY url
                HAVING count(DISTINCT pwsid) >= 3
            """))
            return {row.url for row in result}
    except Exception as e:
        logger.warning(f"Cross-contamination query failed: {e}")
        return set()


# --- Locality Discovery Agent ---

class LocalityDiscoveryAgent(BaseAgent):
    """Fallback discovery agent using municipality name reformulation.

    Runs AFTER the standard DiscoveryAgent has failed for a PWSID.
    Extracts the municipality/locality name from the formal PWSID system
    name and searches using human-readable queries.

    Writes to scrape_registry with url_source='locality_discovery'.
    """

    agent_name = "locality_discovery"

    def __init__(self):
        """Initialize with lazy Serper client."""
        self._serper_client = None

    def _get_serper_client(self):
        """Lazy-init the Serper client."""
        if self._serper_client is None:
            from utility_api.search.serper_client import SerperSearchClient
            self._serper_client = SerperSearchClient()
        return self._serper_client

    def run(
        self,
        pwsid: str,
        search_delay: float | None = None,
        dry_run: bool = False,
        diagnostic: bool = False,
        **kwargs,
    ) -> dict:
        """Discover rate page URLs using locality name reformulation.

        Parameters
        ----------
        pwsid : str
            EPA PWSID to discover URLs for.
        search_delay : float, optional
            Seconds between search queries (default: config value).
        dry_run : bool
            If True, extract municipality and build queries but don't search.
        diagnostic : bool
            If True, log near-miss URLs for threshold tuning.

        Returns
        -------
        dict
            pwsid, municipality, urls_found, urls_written, queries_sent,
            top_candidates, skip_reason (if skipped).
        """
        if search_delay is None:
            search_delay = _DISCOVERY_CONFIG.get("inter_query_delay", 0.2)
        schema = settings.utility_schema

        # --- Fetch metadata ---
        meta = _get_system_metadata(pwsid)
        pws_name = meta["pws_name"] or pwsid
        state = meta["state_code"] or pwsid[:2]
        county = meta.get("county")
        city = meta.get("city") or ""

        # --- Extract municipality ---
        municipality = extract_municipality(pws_name, county)
        if municipality is None:
            reason = "private_company" if _PRIVATE_RE.search(pws_name) else "unextractable"
            logger.info(
                f"LocalityDiscovery: SKIP {pwsid} — {pws_name} "
                f"(reason: {reason})"
            )
            return {
                "pwsid": pwsid,
                "pws_name": pws_name,
                "municipality": None,
                "skip_reason": reason,
                "urls_found": 0,
                "urls_written": 0,
                "queries_sent": [],
                "top_candidates": [],
            }

        # --- Build locality queries ---
        queries = build_locality_queries(municipality, state, county=county)
        logger.info(
            f"LocalityDiscovery: {pwsid} — {pws_name} → \"{municipality}\" "
            f"({len(queries)} queries)"
        )

        if dry_run:
            return {
                "pwsid": pwsid,
                "pws_name": pws_name,
                "municipality": municipality,
                "skip_reason": None,
                "urls_found": 0,
                "urls_written": 0,
                "queries_sent": queries,
                "top_candidates": [],
                "dry_run": True,
            }

        # --- Load cross-contamination set ---
        cross_contamination = _get_cross_contamination_urls(schema)

        # --- Search via Serper ---
        client = self._get_serper_client()
        all_candidates = []
        seen_urls = set()
        raw_result_count = 0

        for query in queries:
            try:
                results = client.search(query, num_results=10, pwsid=pwsid)
            except Exception as e:
                logger.warning(f"  Serper search failed for '{query}': {e}")
                results = []

            raw_result_count += len(results)
            for r in results:
                url = r["url"]
                if url in seen_urls or not url.startswith("http"):
                    continue
                seen_urls.add(url)

                score = score_locality_url(
                    url, r["title"], r["snippet"],
                    municipality, state,
                    utility_name=pws_name, city=city,
                    cross_contamination_urls=cross_contamination,
                )

                all_candidates.append({
                    "url": url,
                    "title": r["title"],
                    "snippet": r["snippet"],
                    "score": score,
                    "query": query,
                })

            time.sleep(search_delay)

        # --- Score and rank ---
        all_candidates.sort(key=lambda c: c["score"], reverse=True)
        score_threshold = _DISCOVERY_CONFIG.get("url_score_threshold", 30)
        above_threshold = [c for c in all_candidates if c["score"] > score_threshold]
        near_misses = [c for c in all_candidates if 15 <= c["score"] <= score_threshold]

        urls_per = _DISCOVERY_CONFIG.get("urls_per_pwsid", 5)
        top_candidates = above_threshold[:urls_per]

        logger.info(
            f"  Funnel: {raw_result_count} raw → {len(seen_urls)} deduped → "
            f"{len(above_threshold)} above {score_threshold} → "
            f"{len(top_candidates)} selected"
        )

        if diagnostic and near_misses:
            for nm in near_misses[:5]:
                logger.info(
                    f"  NEAR-MISS [{nm['score']:3d}] "
                    f"{nm['title'][:50]} → {nm['url'][:60]}"
                )

        # --- Write to scrape_registry ---
        urls_written = 0
        if top_candidates:
            with engine.connect() as conn:
                for rank_idx, c in enumerate(top_candidates, start=1):
                    content_type = "pdf" if c["url"].lower().endswith(".pdf") else "html"
                    result = conn.execute(text(f"""
                        INSERT INTO {schema}.scrape_registry
                            (pwsid, url, url_source, discovery_query,
                             content_type, status, discovery_score,
                             discovery_rank)
                        VALUES
                            (:pwsid, :url, 'locality_discovery', :query,
                             :ctype, 'pending', :score,
                             :rank)
                        ON CONFLICT (pwsid, url) DO NOTHING
                    """), {
                        "pwsid": pwsid,
                        "url": c["url"],
                        "query": c["query"],
                        "ctype": content_type,
                        "score": c["score"],
                        "rank": rank_idx,
                    })
                    if result.rowcount > 0:
                        urls_written += 1
                        logger.info(
                            f"  → [rank={rank_idx} score={c['score']}] "
                            f"{c['url'][:80]}"
                        )

                conn.commit()

        # --- Log to pipeline_runs ---
        self.log_run(
            status="success" if urls_written > 0 else "no_results",
            rows_affected=urls_written,
            notes=(
                f"Locality discovery: {pws_name} → \"{municipality}\" ({state}): "
                f"{raw_result_count} raw → {len(seen_urls)} dedup → "
                f"{len(above_threshold)} scored >{score_threshold} → "
                f"{urls_written} written"
            ),
        )

        return {
            "pwsid": pwsid,
            "pws_name": pws_name,
            "municipality": municipality,
            "skip_reason": None,
            "urls_found": len(all_candidates),
            "urls_written": urls_written,
            "queries_sent": queries,
            "top_candidates": top_candidates,
        }

#!/usr/bin/env python3
"""
Discovery Agent

Purpose:
    Finds rate page URLs for PWSIDs that have no known URL. Searches
    via Serper.dev (Google Search API), scores results for relevance
    using layered keyword heuristics, and writes top 3 candidates
    to scrape_registry with rank tagging.

    Sprint 24: Replaced SearXNG with Serper. Removed LLM fallback scoring
    (Google results are higher quality than SearXNG's Bing/Yahoo mix, so
    the keyword heuristic is sufficient). Added discovery_rank tracking.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-29 (Sprint 24: Serper integration, rank tagging, remove LLM scoring)

Dependencies:
    - requests (via SerperSearchClient)
    - sqlalchemy

Usage:
    from utility_api.agents.discovery import DiscoveryAgent
    result = DiscoveryAgent().run(pwsid='VA4760100')

Notes:
    - Does NOT scrape or parse — only discovers and records URLs
    - Writes to scrape_registry with status='pending' and discovery_rank=1/2/3
    - Updates pwsid_coverage.scrape_status to 'url_discovered'
    - Keyword heuristic handles scoring (no LLM calls)
    - Query builder uses SDWIS metadata (county, owner_type) for better queries
"""

import re
import time
from pathlib import Path

import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings, PROJECT_ROOT
from utility_api.db import engine


# --- Discovery Config ---

def _load_discovery_config() -> dict:
    """Load discovery settings from config/agent_config.yaml."""
    config_path = PROJECT_ROOT / "config" / "agent_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("discovery", {})
    return {}


_DISCOVERY_CONFIG = _load_discovery_config()


# --- Utility Name Expansion ---

# Common SDWIS abbreviations → full words (regex patterns, word-boundary aware)
_ABBREVIATION_PATTERNS = [
    (r"\bCO\b", "County"),
    (r"\bCTY\b", "County"),
    (r"\bUTIL(?:S)?\b", "Utilities"),
    (r"\bSVC\b", "Service"),
    (r"\bAUTH\b", "Authority"),
    (r"\bDEPT\b", "Department"),
    (r"\bDIST\b", "District"),
    (r"\bWTR\b", "Water"),
    (r"\bW/S\b", "Water and Sewer"),
    (r"\bCOMM\b", "Commission"),
    (r"\bTWP\b", "Township"),
    (r"\bBORO\b", "Borough"),
    (r"\bMUN\b", "Municipal"),
    (r"\bPSA\b", "Public Service Authority"),
    (r"\bWSA\b", "Water and Sewer Authority"),
]


def expand_utility_name(name: str, county: str | None = None) -> str:
    """Expand abbreviated SDWIS utility names to searchable form.

    Examples:
        "STAFFORD CO UTIL" + county="Stafford" → "Stafford County Utilities"
        "PWCSA - EAST" + county="Prince William" → "Prince William County Service Authority"
        "ACSA URBAN AREA" + county="Albemarle" → "Albemarle County Service Authority"
        "NAVAL STATION NORFOLK" → "NAVAL STATION NORFOLK" (no expansion)
    """
    # Strip directional/system suffixes that clutter search
    cleaned = re.sub(
        r"\s*[-–]\s*(EAST|WEST|NORTH|SOUTH|CENTRAL|MAIN|PRIMARY)\s*$",
        "", name, flags=re.IGNORECASE,
    ).strip()

    # Expand known abbreviations using regex word boundaries
    expanded = cleaned
    for pattern, replacement in _ABBREVIATION_PATTERNS:
        expanded = re.sub(pattern, replacement, expanded)

    # Handle short acronyms (PWCSA, ACSA, JCSA, HRSD, BVU, etc.)
    # Only trigger for tokens <=6 chars that are all uppercase and we have county.
    # This avoids false positives on real words like STAFFORD (8), HENRICO (7).
    first_token = cleaned.split("-")[0].split()[0].strip()
    is_likely_acronym = (
        len(first_token) <= 6
        and first_token.isupper()
        and county
    )
    if is_likely_acronym:
        core = first_token
        if core.endswith("SA") or core.endswith("CA"):
            # X Service Authority / X County Authority
            expanded = f"{county} County Service Authority"
        elif core.endswith("SD") or core.endswith("WD"):
            expanded = f"{county} Water District"
        elif core.endswith("PSA"):
            expanded = f"{county} Public Service Authority"
        else:
            # Generic: use county + water utility
            expanded = f"{county} County Water"

    return expanded


def _get_system_metadata(pwsid: str) -> dict:
    """Fetch SDWIS + CWS metadata for a PWSID.

    Returns dict with: pws_name, state_code, county, city, population, owner_type.
    """
    schema = settings.utility_schema
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT
                s.pws_name, s.state_code, s.population_served_count,
                s.owner_type_code, c.county_served, s.city
            FROM {schema}.sdwis_systems s
            LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = s.pwsid
            WHERE s.pwsid = :pwsid
        """), {"pwsid": pwsid}).fetchone()

    if not row:
        return {"pws_name": None, "state_code": pwsid[:2], "county": None,
                "city": None, "population": None, "owner_type": None}

    return {
        "pws_name": row.pws_name,
        "state_code": row.state_code,
        "county": row.county_served,
        "city": row.city,
        "population": row.population_served_count,
        "owner_type": row.owner_type_code,
    }


# --- Search Query Construction ---

def build_search_queries(
    utility_name: str,
    state: str,
    county: str | None = None,
    owner_type: str | None = None,
) -> list[str]:
    """Generate targeted search queries using all available metadata.

    Sprint 24: Reduced from 7 queries (SearXNG) to 4 (Serper). Google results
    are higher quality per query, so fewer queries cover more ground.

    Parameters
    ----------
    utility_name : str
        SDWIS pws_name (may be abbreviated).
    state : str
        2-letter state code.
    county : str, optional
        County name from CWS boundaries.
    owner_type : str, optional
        SDWIS owner_type_code: F/S/L/P/M.

    Returns
    -------
    list[str]
        Up to 4 search query strings for Serper.
    """
    queries = []

    # Expand the SDWIS name using county context
    expanded = expand_utility_name(utility_name, county)
    best_name = expanded if expanded != utility_name else utility_name

    # Q1 — Highest precision: quoted expanded name + rates + state
    queries.append(f'"{best_name}" water rates {state}')

    # Q2 — City + state + rate schedule (catches different naming)
    city_meta = _get_city_from_name(utility_name, county)
    if city_meta:
        queries.append(f'{city_meta} {state} water rate schedule')
    elif county:
        queries.append(f'{county} County {state} water rate schedule')

    # Q3 — Broader utility + fees query
    queries.append(f'{best_name} water utility rates fees {state}')

    # Q4 — Varies by owner type for targeted results
    if owner_type == "P":  # private/IOU
        queries.append(f'"{best_name}" tariff rate schedule filetype:pdf')
    elif county:
        queries.append(f'{county} county {state} water rates')
    else:
        queries.append(f'{best_name} water department rates {state}')

    return queries[:4]


def _get_city_from_name(utility_name: str, county: str | None) -> str | None:
    """Extract a city-like name from the utility name for query diversification.

    Returns None if we can't confidently extract a city name.
    """
    # If name starts with "CITY OF X" or "TOWN OF X", extract X
    m = re.match(r"(?:CITY|TOWN|VILLAGE)\s+OF\s+(.+?)(?:\s*[-,]|$)", utility_name, re.IGNORECASE)
    if m:
        return m.group(1).strip().title()
    return None


# --- URL Relevance Scoring (v2 — Sprint 21, updated Sprint 24) ---

# Domains that are never water utility rate pages
_AGGREGATOR_DOMAINS = frozenset([
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com",
    "reddit.com", "nextdoor.com", "yelp.com", "wikipedia.org",
    "patch.com", "google.com", "bing.com", "yahoo.com",
    "amazon.com", "ebay.com", "instagram.com", "tiktok.com",
])


def _score_url_path(url: str) -> int:
    """Score bonus for URL path patterns that indicate a rate page. Max +20.

    Sprint 22: Utility rate pages cluster on predictable URL paths.
    Recognizing these lifts good URLs out of the ambiguous 30-50 range
    and into the >50 threshold zone, avoiding unnecessary LLM calls.
    """
    from urllib.parse import urlparse
    from pathlib import PurePosixPath

    parsed = urlparse(url.lower())
    path = parsed.path

    # High-value path segments — utility rate pages cluster here
    high_value_segments = {
        "rates", "rate-schedule", "water-rates", "billing-rates",
        "fees", "rate-structure", "utility-rates", "water-sewer-rates",
        "rates-fees", "rates-and-fees", "tariff", "rate-table",
        "water-and-sewer-rates", "fee-schedule",
    }

    # Check path segments
    segments = [s.replace("_", "-").replace("%20", "-")
                for s in path.split("/") if s]
    for segment in segments:
        if segment in high_value_segments:
            return 20

    # Check filename stem (e.g. rate-schedule.pdf, water-rates.html)
    stem = PurePosixPath(parsed.path).stem.lower().replace("_", "-")
    if stem in high_value_segments:
        return 15

    return 0


def _score_url_freshness(title: str, snippet: str, url: str) -> int:
    """Score bonus/penalty based on year mentions in text. Range: -15 to +10.

    Sprint 22: Current-year pages are actively maintained. Pages mentioning
    only years >2 old are likely stale and less likely to parse correctly.
    """
    from datetime import date
    current_year = date.today().year

    combined = f"{title} {snippet} {url}".lower()
    years_found = [int(f"20{m}") for m in re.findall(r"20(1[5-9]|2[0-9])", combined)]

    if not years_found:
        return 0

    most_recent = max(years_found)

    if most_recent >= current_year:
        return 10       # Current or future year → actively maintained page
    elif most_recent == current_year - 1:
        return 5        # Last year → probably still valid
    elif most_recent == current_year - 2:
        return 0        # Two years stale → neutral
    else:
        years_stale = current_year - most_recent
        return max(-5 * (years_stale - 2), -15)  # -5 per stale year, floor at -15


def score_url_relevance(
    url: str,
    title: str,
    snippet: str,
    utility_name: str = "",
    city: str = "",
    state: str = "",
) -> int:
    """Score 0-100 using layered heuristics. No LLM needed.

    Sprint 24: LLM fallback removed. Serper returns Google results which are
    higher quality than SearXNG's Bing/Yahoo mix. The keyword heuristic is
    sufficient for scoring. Thresholds unchanged (>50 = import).

    Layers:
        Base keywords:       0-60  (rate, schedule, tariff, etc.)
        Domain authority:    0-15  (.gov/.org boost, aggregator penalty)
        Utility/city match:  0-25  (utility or city name in domain)
        PDF + rate keyword:  0-20  (PDF with rate-related path/content)
        URL path pattern:    0-20  (Sprint 22: /rates, /fee-schedule, etc.)
        Freshness:         -15-10  (Sprint 22: year-based bonus/penalty)
        Negative keywords:   -20 each (meeting, agenda, job, etc.)
    """
    from urllib.parse import urlparse

    combined = f"{url} {title} {snippet}".lower()
    url_lower = url.lower()
    hostname = (urlparse(url).hostname or "").lower()
    score = 0

    # --- Layer 1: Base keyword scoring ---
    for kw in ["rate", "schedule", "tariff", "water bill", "fee schedule",
                "rate structure", "charges", "pricing", "rate study"]:
        if kw in combined:
            score += 15

    # --- Layer 2: Domain authority ---
    if ".gov" in hostname:
        score += 15
    elif ".org" in hostname:
        score += 10
    elif ".us" in hostname:
        score += 8

    if any(agg in hostname for agg in _AGGREGATOR_DOMAINS):
        score -= 25

    # --- Layer 3: Utility/city name in domain ---
    if utility_name and hostname:
        util_slug = re.sub(r"[^a-z0-9]", "", utility_name.lower())
        domain_slug = re.sub(r"[^a-z0-9]", "", hostname.replace("www.", ""))

        if len(util_slug) > 5 and util_slug in domain_slug:
            score += 25
        else:
            # Check significant words (>4 chars, not generic water terms)
            generic = {"water", "city", "town", "county", "district",
                       "authority", "department", "service", "system",
                       "utility", "utilities", "board"}
            words = [w.lower() for w in utility_name.split()
                     if len(w) > 4 and w.lower() not in generic]
            if any(w in hostname for w in words):
                score += 15

    if city and hostname:
        city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
        if len(city_slug) > 3 and city_slug in hostname:
            score += 15

    # --- Layer 4: PDF + rate keyword bonus ---
    if url_lower.endswith(".pdf"):
        if any(kw in combined for kw in ["rate", "schedule", "fee", "tariff"]):
            score += 20

    # --- Layer 5: URL path pattern bonus (Sprint 22) ---
    score += _score_url_path(url)

    # --- Layer 6: Freshness bonus/penalty (Sprint 22) ---
    score += _score_url_freshness(title, snippet, url)

    # --- Layer 7: Negative keyword penalties ---
    for neg in ["meeting", "agenda", "minutes", "news", "press release",
                "election", "job", "career", "bid", "rfp"]:
        if neg in combined:
            score -= 20

    return max(0, min(100, score))


class DiscoveryAgent(BaseAgent):
    """Discovers water rate page URLs for PWSIDs.

    Sprint 24: Uses Serper.dev (Google Search API) instead of SearXNG.
    Writes top 3 candidates with discovery_rank tagging. No LLM scoring.
    """

    agent_name = "discovery"

    def __init__(self):
        """Initialize with Serper client (lazy — created on first search)."""
        self._serper_client = None

    def _get_serper_client(self):
        """Lazy-init the Serper client (avoids import errors when key is missing)."""
        if self._serper_client is None:
            from utility_api.search.serper_client import SerperSearchClient
            self._serper_client = SerperSearchClient()
        return self._serper_client

    def run(
        self,
        pwsid: str,
        utility_name: str | None = None,
        state: str | None = None,
        search_delay: float | None = None,
        **kwargs,
    ) -> dict:
        """Discover rate page URLs for a PWSID.

        Parameters
        ----------
        pwsid : str
            EPA PWSID to discover URLs for.
        utility_name : str, optional
            Utility name (looked up from SDWIS if not provided).
        state : str, optional
            State code (derived from PWSID if not provided).
        search_delay : float, optional
            Seconds between search queries. Defaults to config value (0.2s).

        Returns
        -------
        dict
            pwsid, urls_found, urls_written, top_candidates, queries_sent.
        """
        if search_delay is None:
            search_delay = _DISCOVERY_CONFIG.get("inter_query_delay", 0.2)
        schema = settings.utility_schema

        # Fetch full metadata from SDWIS + CWS
        meta = _get_system_metadata(pwsid)
        utility_name = utility_name or meta["pws_name"] or pwsid
        state = state or meta["state_code"] or pwsid[:2]
        county = meta.get("county")
        owner_type = meta.get("owner_type")

        # Sprint 16: domain_guess_only flag skips search entirely
        domain_guess_only = kwargs.get("domain_guess_only", False)
        # Sprint 22: skip_domain_guess flag — domain guesser is a separate pipeline
        skip_domain_guess = kwargs.get("skip_domain_guess", False)

        # Log the expanded name for debugging
        expanded = expand_utility_name(utility_name, county)
        if expanded != utility_name:
            logger.info(f"DiscoveryAgent: {utility_name} → {expanded} ({pwsid}, {state}, county={county})")
        else:
            logger.info(f"DiscoveryAgent: {utility_name} ({pwsid}, {state}, county={county})")

        # Sprint 16: Try domain guessing first (free, instant, no rate limit)
        # Sprint 22: skip if domain guesser already ran (separate pipeline)
        domain_guess_urls = 0
        if county and not skip_domain_guess:
            from utility_api.ops.domain_guesser import DomainGuesser
            guesser = DomainGuesser()
            guesses = guesser.guess_urls(pwsid, utility_name, county, state, owner_type)
            # Write homepage candidates to registry
            homepage_guesses = [g for g in guesses if g["method"] == "domain_guess_homepage"]
            if homepage_guesses:
                with engine.connect() as conn:
                    for g in homepage_guesses:
                        result = conn.execute(text(f"""
                            INSERT INTO {schema}.scrape_registry
                                (pwsid, url, url_source, content_type, status, notes)
                            VALUES
                                (:pwsid, :url, 'domain_guess', 'html', 'pending',
                                 :notes)
                            ON CONFLICT (pwsid, url) DO NOTHING
                        """), {
                            "pwsid": pwsid,
                            "url": g["url"],
                            "notes": f"Domain guess: {g['domain']}",
                        })
                        if result.rowcount > 0:
                            domain_guess_urls += 1
                            logger.info(f"  Domain guess → {g['url']}")
                    if domain_guess_urls > 0:
                        conn.execute(text(f"""
                            UPDATE {schema}.pwsid_coverage
                            SET scrape_status = 'url_discovered'
                            WHERE pwsid = :pwsid AND scrape_status = 'not_attempted'
                        """), {"pwsid": pwsid})
                    conn.commit()

        if domain_guess_only:
            self.log_run(
                status="success",
                rows_affected=domain_guess_urls,
                notes=f"{utility_name}: domain guess only, {domain_guess_urls} URLs",
            )
            return {
                "pwsid": pwsid,
                "urls_found": domain_guess_urls,
                "urls_written": domain_guess_urls,
                "top_candidates": [],
                "queries_sent": [],
                "method": "domain_guess_only",
            }

        # Build and run search queries (Serper)
        queries = build_search_queries(utility_name, state, county, owner_type)
        all_candidates = []
        seen_urls = set()
        raw_result_count = 0
        city = meta.get("city") or ""
        diagnostic = kwargs.get("diagnostic", False)

        client = self._get_serper_client()

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

                score = score_url_relevance(
                    url, r["title"], r["snippet"],
                    utility_name, city, state,
                )

                all_candidates.append({
                    "url": url,
                    "title": r["title"],
                    "snippet": r["snippet"],
                    "score": score,
                    "query": query,
                })

            time.sleep(search_delay)

        # Sort by score, compute funnel stats
        all_candidates.sort(key=lambda c: c["score"], reverse=True)
        score_threshold = _DISCOVERY_CONFIG.get("url_score_threshold", 45)
        above_threshold = [c for c in all_candidates if c["score"] > score_threshold]
        near_misses = [c for c in all_candidates if 15 <= c["score"] <= score_threshold]
        below_threshold = [c for c in all_candidates if c["score"] < 15]

        # Take up to 3 URLs above threshold with rank tagging
        urls_per = _DISCOVERY_CONFIG.get("urls_per_pwsid", 3)
        top_candidates = above_threshold[:urls_per]

        logger.info(
            f"  Funnel: {raw_result_count} raw → {len(seen_urls)} deduped → "
            f"{len(above_threshold)} above {score_threshold} → {len(near_misses)} near-miss → "
            f"{len(top_candidates)} written"
        )

        # Diagnostic mode: log near-misses for threshold tuning
        if diagnostic and near_misses:
            for nm in near_misses[:5]:
                logger.info(
                    f"  NEAR-MISS [{nm['score']:3d}] "
                    f"{nm['title'][:50]} → {nm['url'][:60]}"
                )

        # Write to scrape_registry with rank tagging
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
                            (:pwsid, :url, 'serper', :query,
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
                        logger.info(f"  → [rank={rank_idx} score={c['score']}] {c['url'][:80]}")

                # Update searxng_status (column name retained for compatibility;
                # tracks "has been searched via any search engine")
                if urls_written > 0:
                    conn.execute(text(f"""
                        UPDATE {schema}.pwsid_coverage
                        SET searxng_status = 'url_found'
                        WHERE pwsid = :pwsid
                    """), {"pwsid": pwsid})

                conn.commit()
        else:
            # No URLs above threshold — mark as searched with no hits
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.pwsid_coverage
                    SET searxng_status = 'searched_no_hits'
                    WHERE pwsid = :pwsid AND searxng_status = 'not_attempted'
                """), {"pwsid": pwsid})
                conn.commit()

        # Always mark search attempted (prevents infinite re-queuing)
        self._mark_searched(pwsid, schema)

        # Log full scoring funnel to search_log with ranked URLs
        self._log_search(
            pwsid=pwsid,
            schema=schema,
            search_engine="serper",
            queries_run=len(queries),
            raw_results_count=raw_result_count,
            deduped_count=len(seen_urls),
            above_threshold_count=len(above_threshold),
            near_miss_count=len(near_misses),
            below_threshold_count=len(below_threshold),
            written_count=urls_written,
            best_score=top_candidates[0]["score"] if top_candidates else 0,
            best_url=top_candidates[0]["url"] if top_candidates else None,
            top_candidates=top_candidates,
        )

        # Log to pipeline_runs for visibility
        self.log_run(
            status="success" if urls_written > 0 else "no_results",
            rows_affected=urls_written,
            notes=(
                f"{utility_name} ({state}): "
                f"{raw_result_count} raw → {len(seen_urls)} dedup → "
                f"{len(above_threshold)} scored >{score_threshold} → {urls_written} written"
            ),
        )

        return {
            "pwsid": pwsid,
            "urls_found": len(all_candidates),
            "urls_written": urls_written,
            "top_candidates": top_candidates,
            "queries_sent": queries,
        }

    @staticmethod
    def _mark_searched(pwsid: str, schema: str) -> None:
        """Record that this PWSID was searched, regardless of outcome.

        Prevents infinite re-queuing of PWSIDs with no web presence.
        The orchestrator respects a 30-day re-search window.
        """
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.pwsid_coverage
                    SET search_attempted_at = NOW()
                    WHERE pwsid = :pwsid
                """), {"pwsid": pwsid})
                conn.commit()
        except Exception as e:
            logger.debug(f"  search_attempted_at update failed: {e}")

    @staticmethod
    def _log_search(
        pwsid: str,
        schema: str,
        search_engine: str,
        queries_run: int,
        raw_results_count: int,
        deduped_count: int,
        above_threshold_count: int,
        near_miss_count: int,
        below_threshold_count: int,
        written_count: int,
        best_score: float,
        best_url: str | None,
        top_candidates: list[dict] | None = None,
    ) -> None:
        """Log the full scoring funnel to search_log with ranked URL tracking.

        Sprint 24: Added search_engine, url_rank_1/2/3, score_rank_1/2/3
        for per-rank parse success analysis.
        """
        # Extract ranked URLs for the search_log row
        candidates = top_candidates or []
        rank_data = {}
        for i in range(1, 4):
            if i <= len(candidates):
                rank_data[f"url_rank_{i}"] = candidates[i - 1]["url"]
                rank_data[f"score_rank_{i}"] = candidates[i - 1]["score"]
            else:
                rank_data[f"url_rank_{i}"] = None
                rank_data[f"score_rank_{i}"] = None

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.search_log
                        (pwsid, search_engine, queries_run, raw_results_count,
                         deduped_count, above_threshold_count, near_miss_count,
                         below_threshold_count, written_count,
                         best_score, best_url,
                         url_rank_1, score_rank_1,
                         url_rank_2, score_rank_2,
                         url_rank_3, score_rank_3)
                    VALUES
                        (:pwsid, :engine, :queries, :raw, :dedup,
                         :above, :near, :below, :written,
                         :score, :url,
                         :url_rank_1, :score_rank_1,
                         :url_rank_2, :score_rank_2,
                         :url_rank_3, :score_rank_3)
                """), {
                    "pwsid": pwsid,
                    "engine": search_engine,
                    "queries": queries_run,
                    "raw": raw_results_count,
                    "dedup": deduped_count,
                    "above": above_threshold_count,
                    "near": near_miss_count,
                    "below": below_threshold_count,
                    "written": written_count,
                    "score": best_score,
                    "url": best_url,
                    **rank_data,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"  search_log write failed: {e}")

#!/usr/bin/env python3
"""
Rate Page URL Discovery

Purpose:
    For each CWS utility in target states, discover the URL of their
    water rate schedule page using web search. Stores discovered URLs
    in the water_rates table as staging rows (parse_confidence='pending').

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - httpx
    - beautifulsoup4
    - sqlalchemy

Usage:
    from utility_api.ingest.rate_discovery import discover_rate_urls
    discover_rate_urls(pwsids=["VA0071010"], dry_run=True)

Notes:
    - Uses DuckDuckGo HTML search (no API key required)
    - Search query: "{utility_name} {state} water rates"
    - Rate-limits requests to avoid being blocked (2s between searches)
    - Idempotent: skips PWSIDs that already have a source_url
    - Can be run incrementally on subsets of PWSIDs

Data Sources:
    - Input: utility.cws_boundaries (pwsid, pws_name, state_code)
    - Output: discovered URLs printed / stored for scraping step
"""

import hashlib
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine

# Rate-related keywords for scoring search results
RATE_KEYWORDS = [
    "water rate", "water rates", "water billing",
    "rate schedule", "rate structure", "water charges",
    "water pricing", "water tariff", "water fee",
    "monthly bill", "service charge", "volumetric rate",
    "ccf", "per gallon", "water cost",
]

# Domains to deprioritize (aggregators, not primary sources)
DEPRIORITY_DOMAINS = [
    "wikipedia.org", "facebook.com", "twitter.com", "youtube.com",
    "yelp.com", "bbb.org", "linkedin.com", "indeed.com",
]

SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


@dataclass
class RatePageCandidate:
    """A candidate URL for a utility rate page."""

    url: str
    title: str
    snippet: str
    score: float = 0.0
    domain: str = ""


@dataclass
class DiscoveryResult:
    """Result of URL discovery for one utility."""

    pwsid: str
    utility_name: str
    state_code: str
    best_url: str | None = None
    best_title: str | None = None
    candidates: list[RatePageCandidate] = field(default_factory=list)
    search_query: str = ""
    error: str | None = None


def _score_candidate(candidate: RatePageCandidate) -> float:
    """Score a search result based on rate-keyword relevance.

    Parameters
    ----------
    candidate : RatePageCandidate
        Search result to score.

    Returns
    -------
    float
        Relevance score (higher = more likely a rate page).
    """
    text_lower = f"{candidate.title} {candidate.snippet}".lower()
    score = 0.0

    # Keyword matches
    for kw in RATE_KEYWORDS:
        if kw in text_lower:
            score += 2.0

    # Bonus for .gov or .org domains (likely utility sites)
    domain = candidate.domain.lower()
    if domain.endswith(".gov"):
        score += 3.0
    elif domain.endswith(".org"):
        score += 1.5

    # Penalty for aggregator/social domains
    for bad in DEPRIORITY_DOMAINS:
        if bad in domain:
            score -= 10.0

    # Bonus for URL path containing rate-related terms
    url_lower = candidate.url.lower()
    for term in ["rate", "billing", "water-rate", "tariff", "fee-schedule"]:
        if term in url_lower:
            score += 2.0

    # Bonus for PDF links (often official rate schedules)
    if url_lower.endswith(".pdf"):
        score += 1.5

    candidate.score = score
    return score


def _extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return ""


# Default SearXNG URL — overridden by config/agent_config.yaml discovery.searxng_url
def _get_searxng_url() -> str:
    """Read SearXNG URL from agent config, with fallback."""
    try:
        import yaml
        config_path = Path(__file__).parents[3] / "config" / "agent_config.yaml"
        if config_path.exists():
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            return cfg.get("discovery", {}).get("searxng_url", "http://localhost:8889/search")
    except Exception:
        pass
    return "http://localhost:8889/search"

SEARXNG_URL = _get_searxng_url()


def _search_searxng(query: str, max_results: int = 10) -> list[RatePageCandidate]:
    """Search via local SearXNG instance (meta-search across multiple engines).

    Parameters
    ----------
    query : str
        Search query string.
    max_results : int
        Maximum number of results to return.

    Returns
    -------
    list[RatePageCandidate]
        Parsed and scored search results.
    """
    params = {
        "q": query,
        "format": "json",
        "categories": "general",
        "language": "en",
    }

    try:
        with httpx.Client(timeout=15) as client:
            response = client.get(SEARXNG_URL, params=params)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"SearXNG search failed for '{query}': {e}")
        # Fall back to DuckDuckGo direct if SearXNG is down
        return _search_duckduckgo_fallback(query, max_results)

    data = response.json()
    candidates = []

    for result in data.get("results", []):
        href = result.get("url", "")
        title = result.get("title", "")
        snippet = result.get("content", "")

        if not href:
            continue

        candidate = RatePageCandidate(
            url=href,
            title=title,
            snippet=snippet,
            domain=_extract_domain(href),
        )
        _score_candidate(candidate)
        candidates.append(candidate)

        if len(candidates) >= max_results:
            break

    # Sort by score descending
    candidates.sort(key=lambda c: c.score, reverse=True)

    # If SearXNG returned 0 results, try DuckDuckGo fallback (rate limit likely)
    if not candidates:
        logger.info(f"SearXNG returned 0 results for '{query}' — trying DuckDuckGo fallback")
        return _search_duckduckgo_fallback(query, max_results)

    return candidates


def _search_duckduckgo_fallback(query: str, max_results: int = 10) -> list[RatePageCandidate]:
    """Fallback: search DuckDuckGo HTML directly if SearXNG is unavailable.

    Parameters
    ----------
    query : str
        Search query string.
    max_results : int
        Maximum number of results to return.

    Returns
    -------
    list[RatePageCandidate]
        Parsed search results.
    """
    url = "https://html.duckduckgo.com/html/"
    params = {"q": query}

    try:
        with httpx.Client(headers=SEARCH_HEADERS, timeout=30, follow_redirects=True) as client:
            response = client.post(url, data=params)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"DuckDuckGo fallback also failed for '{query}': {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    candidates = []

    for result in soup.select(".result"):
        title_el = result.select_one(".result__title a")
        snippet_el = result.select_one(".result__snippet")

        if not title_el:
            continue

        href = title_el.get("href", "")
        if "uddg=" in href:
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            href = qs.get("uddg", [href])[0]

        title = title_el.get_text(strip=True)
        snippet = snippet_el.get_text(strip=True) if snippet_el else ""

        candidate = RatePageCandidate(
            url=href,
            title=title,
            snippet=snippet,
            domain=_extract_domain(href),
        )
        _score_candidate(candidate)
        candidates.append(candidate)

        if len(candidates) >= max_results:
            break

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates


def discover_rate_url(
    pwsid: str,
    utility_name: str,
    state_code: str,
    county: str | None = None,
) -> DiscoveryResult:
    """Discover the rate page URL for a single utility.

    Parameters
    ----------
    pwsid : str
        EPA PWSID.
    utility_name : str
        Utility name from CWS boundaries.
    state_code : str
        Two-letter state code.
    county : str | None
        County name (optional, improves search specificity).

    Returns
    -------
    DiscoveryResult
        Discovery result with best URL and all candidates.
    """
    # Clean utility name for search
    name_clean = re.sub(r"\s+", " ", utility_name).strip()
    # Remove common suffixes that add noise
    for suffix in ["Water Department", "Water Dept", "Water System", "Water Authority"]:
        if name_clean.upper().endswith(suffix.upper()):
            # Keep the suffix — it helps disambiguate
            break

    # Build search query
    parts = [name_clean, state_code, "water rates"]
    if county:
        parts.insert(1, county)
    query = " ".join(parts)

    result = DiscoveryResult(
        pwsid=pwsid,
        utility_name=utility_name,
        state_code=state_code,
        search_query=query,
    )

    candidates = _search_searxng(query)
    result.candidates = candidates

    if candidates and candidates[0].score > 0:
        result.best_url = candidates[0].url
        result.best_title = candidates[0].title
        logger.info(
            f"  {pwsid} ({name_clean}): {candidates[0].url} "
            f"[score={candidates[0].score:.1f}]"
        )
    else:
        result.error = "No relevant results found"
        logger.warning(f"  {pwsid} ({name_clean}): no rate page found")

    return result


def discover_rate_urls(
    pwsids: list[str] | None = None,
    state_filter: list[str] | None = None,
    limit: int | None = None,
    delay_seconds: float = 2.0,
    dry_run: bool = False,
) -> list[DiscoveryResult]:
    """Discover rate page URLs for multiple utilities.

    Parameters
    ----------
    pwsids : list[str] | None
        Specific PWSIDs to search. If None, queries from DB.
    state_filter : list[str] | None
        Filter to these states (e.g., ["VA", "CA"]).
    limit : int | None
        Max number of utilities to search.
    delay_seconds : float
        Delay between searches to avoid rate limiting.
    dry_run : bool
        If True, print results but don't write to DB.

    Returns
    -------
    list[DiscoveryResult]
        Discovery results for each utility.
    """
    schema = settings.utility_schema

    # Get target utilities from DB
    with engine.connect() as conn:
        if pwsids:
            placeholders = ", ".join(f"'{p}'" for p in pwsids)
            query = text(f"""
                SELECT c.pwsid, c.pws_name, c.state_code, c.county_served
                FROM {schema}.cws_boundaries c
                WHERE c.pwsid IN ({placeholders})
                ORDER BY c.state_code, c.pws_name
            """)
        else:
            # Get MDWD utilities (those with financial data — our Sprint 3 target)
            state_clause = ""
            if state_filter:
                state_list = ", ".join(f"'{s}'" for s in state_filter)
                state_clause = f"AND c.state_code IN ({state_list})"

            query = text(f"""
                SELECT c.pwsid, c.pws_name, c.state_code, c.county_served
                FROM {schema}.cws_boundaries c
                INNER JOIN {schema}.mdwd_financials m ON m.pwsid = c.pwsid
                WHERE c.pws_name IS NOT NULL
                {state_clause}
                ORDER BY c.state_code, c.pws_name
            """)

        rows = conn.execute(query).fetchall()

    # Skip utilities that already have a rate URL
    with engine.connect() as conn:
        existing = conn.execute(
            text(f"SELECT DISTINCT pwsid FROM {schema}.water_rates WHERE source_url IS NOT NULL")
        ).fetchall()
        existing_pwsids = {r[0] for r in existing}

    targets = [(r[0], r[1], r[2], r[3]) for r in rows if r[0] not in existing_pwsids]

    if limit:
        targets = targets[:limit]

    logger.info(f"Discovering rate URLs for {len(targets)} utilities "
                f"({len(existing_pwsids)} already have URLs)")

    results = []
    for i, (pwsid, name, state, county) in enumerate(targets):
        logger.info(f"[{i + 1}/{len(targets)}] Searching: {name} ({state})")
        result = discover_rate_url(pwsid, name, state, county)
        results.append(result)

        if not dry_run and result.best_url:
            # Store discovery as a pending rate record
            _store_discovery(result)

        if i < len(targets) - 1:
            time.sleep(delay_seconds)

    # Summary
    found = sum(1 for r in results if r.best_url)
    logger.info(f"\nDiscovery complete: {found}/{len(results)} URLs found")

    return results


def _store_discovery(result: DiscoveryResult) -> None:
    """Store a discovery result as a pending water_rates row.

    Parameters
    ----------
    result : DiscoveryResult
        Discovery result to store.
    """
    schema = settings.utility_schema
    with engine.connect() as conn:
        # Store discovery result in scrape_registry (Phase 3: no longer staging in water_rates)
        conn.execute(
            text(f"""
                INSERT INTO {schema}.scrape_registry (pwsid, url, url_source, status,
                    discovery_query, notes)
                VALUES (:pwsid, :url, 'searxng', 'pending',
                    :query, :notes)
                ON CONFLICT (pwsid, url)
                DO UPDATE SET notes = EXCLUDED.notes,
                              discovery_query = EXCLUDED.discovery_query,
                              updated_at = NOW()
            """),
            {
                "pwsid": result.pwsid,
                "url": result.best_url,
                "query": result.search_query,
                "notes": f"Discovered via search: {result.search_query}",
            },
        )
        conn.commit()

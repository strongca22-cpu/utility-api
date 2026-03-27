#!/usr/bin/env python3
"""
Discovery Agent

Purpose:
    Finds rate page URLs for PWSIDs that have no known URL. Searches
    via SearXNG, scores results for relevance (keyword heuristic first,
    optional Haiku fallback for ambiguous cases), and writes candidates
    to scrape_registry.

    LLM usage: optional, ~20% of calls (Haiku for ambiguous scores only).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-25 (Sprint 14.5: configurable SearXNG URL, VPS-based discovery)

Dependencies:
    - requests (SearXNG API)
    - anthropic (optional, Haiku for ambiguous URL scoring)
    - sqlalchemy

Usage:
    from utility_api.agents.discovery import DiscoveryAgent
    result = DiscoveryAgent().run(pwsid='VA4760100')

Notes:
    - Does NOT scrape or parse — only discovers and records URLs
    - Writes to scrape_registry with status='pending'
    - Updates pwsid_coverage.scrape_status to 'url_discovered'
    - Keyword heuristic handles ~80% of cases without LLM
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
    """Generate 3-7 targeted search queries using all available metadata.

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
        Up to 7 search query strings for SearXNG.
    """
    queries = []

    # Expand the SDWIS name using county context
    expanded = expand_utility_name(utility_name, county)
    best_name = expanded if expanded != utility_name else utility_name

    # Query 1: Expanded name + "water rates"
    queries.append(f'"{best_name}" water rates {state}')

    # Query 2: County-based search (if we have county data)
    if county:
        queries.append(f"{county} County water rates {state}")

    # Query 3: Original SDWIS name (if different from expanded)
    if expanded != utility_name:
        queries.append(f'"{utility_name}" water rate schedule')

    # Query 4: PDF rate schedule search
    queries.append(f'{best_name} rate schedule {state} filetype:pdf')

    # Query 5: County government water department (local/mixed owners only)
    if county and owner_type in ("L", "M", None):
        queries.append(f"{county} {state} water department rates fees")

    # For federal systems (military bases), add installation-specific query
    if owner_type == "F":
        queries.append(f'"{utility_name}" utility rates')

    # Query 6: CCR search — Consumer Confidence Reports are often hosted on
    # utility websites, so finding a CCR reveals the utility's domain.
    # (Sprint 15 toolkit integration)
    from datetime import date
    current_year = date.today().year
    queries.append(
        f'"{best_name}" "consumer confidence report" {current_year}'
    )

    # Query 7: Government site operator — for local/state-owned utilities,
    # rate pages are often on .gov domains. The site: operator may or may
    # not work through SearXNG depending on search engine backends.
    # (Sprint 15 toolkit integration)
    if owner_type in ("L", "S", "M", None):
        queries.append(f'site:.gov "{best_name}" water rates')

    # Query 8: County water authority pattern (Sprint 21)
    # Many gap-state utilities are county water authorities
    if county:
        queries.append(f'{county} County water authority rates {state}')

    # Query 9: Consumer-oriented "pay your bill" query (Sprint 21)
    # Some utilities publish rates on billing pages, not formal rate schedules
    queries.append(f'"{best_name}" monthly water bill {state}')

    return queries[:10]


# --- URL Relevance Scoring (v2 — Sprint 21) ---

# Domains that are never water utility rate pages
_AGGREGATOR_DOMAINS = frozenset([
    "facebook.com", "twitter.com", "youtube.com", "linkedin.com",
    "reddit.com", "nextdoor.com", "yelp.com", "wikipedia.org",
    "patch.com", "google.com", "bing.com", "yahoo.com",
    "amazon.com", "ebay.com", "instagram.com", "tiktok.com",
])


def score_url_relevance(
    url: str,
    title: str,
    snippet: str,
    utility_name: str = "",
    city: str = "",
    state: str = "",
) -> int:
    """Score 0-100 using layered heuristics. No LLM needed for most cases.

    Layers:
        Base keywords:       0-60  (rate, schedule, tariff, etc.)
        Domain authority:    0-15  (.gov/.org boost, aggregator penalty)
        Utility/city match:  0-25  (utility or city name in domain)
        PDF + rate keyword:  0-20  (PDF with rate-related path/content)
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

    # --- Layer 5: Negative keyword penalties ---
    for neg in ["meeting", "agenda", "minutes", "news", "press release",
                "election", "job", "career", "bid", "rfp"]:
        if neg in combined:
            score -= 20

    return max(0, min(100, score))


def score_with_llm_fallback(
    url: str, title: str, snippet: str,
    utility_name: str, state: str,
    city: str = "",
) -> int:
    """Keyword score first. Haiku for anything in the ambiguous zone (15-60)."""
    keyword_score = score_url_relevance(
        url, title, snippet, utility_name, city, state
    )

    if keyword_score > 60 or keyword_score < 15:
        return keyword_score  # confident high or clearly irrelevant

    # Ambiguous — ask Haiku
    try:
        from anthropic import Anthropic
        client = Anthropic()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": (
                    f"Score this URL's relevance as a water utility rate page (0-100).\n"
                    f"Utility: {utility_name}, {state}\n"
                    f"URL: {url}\n"
                    f"Title: {title}\n"
                    f"Snippet: {snippet}\n\n"
                    f"Respond with ONLY a number 0-100."
                ),
            }],
        )
        return int(response.content[0].text.strip())
    except Exception:
        return keyword_score  # fallback to keyword on any error


# --- SearXNG Search ---

def _searxng_search(query: str, max_results: int = 10) -> list[dict]:
    """Run a SearXNG search and return results.

    Uses the searxng_url from config/agent_config.yaml (discovery section).
    Defaults to http://localhost:8889/search (VPS via SSH tunnel).
    """
    import requests

    searxng_url = _DISCOVERY_CONFIG.get("searxng_url", "http://localhost:8889/search")
    timeout = 15

    try:
        r = requests.get(
            searxng_url,
            params={"q": query, "format": "json", "categories": "general"},
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])[:max_results]
        return [{"url": r.get("url", ""), "title": r.get("title", ""),
                 "snippet": r.get("content", "")} for r in results]
    except Exception as e:
        logger.debug(f"SearXNG search failed for '{query}': {e}")
        return []


class DiscoveryAgent(BaseAgent):
    """Discovers water rate page URLs for PWSIDs."""

    agent_name = "discovery"

    def run(
        self,
        pwsid: str,
        utility_name: str | None = None,
        state: str | None = None,
        use_llm: bool = True,
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
        use_llm : bool
            Whether to use Haiku for ambiguous URL scoring.
        search_delay : float, optional
            Seconds between search queries. Defaults to config value (8s).

        Returns
        -------
        dict
            pwsid, urls_found, urls_written.
        """
        if search_delay is None:
            search_delay = _DISCOVERY_CONFIG.get("delay_between_queries", 8.0)
        schema = settings.utility_schema

        # Fetch full metadata from SDWIS + CWS
        meta = _get_system_metadata(pwsid)
        utility_name = utility_name or meta["pws_name"] or pwsid
        state = state or meta["state_code"] or pwsid[:2]
        county = meta.get("county")
        owner_type = meta.get("owner_type")

        # Sprint 16: domain_guess_only flag skips SearXNG entirely
        domain_guess_only = kwargs.get("domain_guess_only", False)

        # Log the expanded name for debugging
        expanded = expand_utility_name(utility_name, county)
        if expanded != utility_name:
            logger.info(f"DiscoveryAgent: {utility_name} → {expanded} ({pwsid}, {state}, county={county})")
        else:
            logger.info(f"DiscoveryAgent: {utility_name} ({pwsid}, {state}, county={county})")

        # Sprint 16: Try domain guessing first (free, instant, no rate limit)
        domain_guess_urls = 0
        if county:
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

        # Build and run search queries (SearXNG)
        queries = build_search_queries(utility_name, state, county, owner_type)
        all_candidates = []
        seen_urls = set()
        raw_result_count = 0
        city = meta.get("city") or ""
        diagnostic = kwargs.get("diagnostic", False)

        for query in queries:
            results = _searxng_search(query)
            raw_result_count += len(results)
            for r in results:
                url = r["url"]
                if url in seen_urls or not url.startswith("http"):
                    continue
                seen_urls.add(url)

                if use_llm:
                    score = score_with_llm_fallback(
                        url, r["title"], r["snippet"],
                        utility_name, state, city,
                    )
                else:
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
        above_threshold = [c for c in all_candidates if c["score"] > 50]
        near_misses = [c for c in all_candidates if 15 <= c["score"] <= 50]
        below_threshold = [c for c in all_candidates if c["score"] < 15]

        # Sprint 21: Cap at 1 URL per PWSID (top result is almost always correct)
        top_candidates = above_threshold[:1]

        logger.info(
            f"  Funnel: {raw_result_count} raw → {len(seen_urls)} deduped → "
            f"{len(above_threshold)} above 50 → {len(near_misses)} near-miss → "
            f"{len(top_candidates)} written"
        )

        # Diagnostic mode: log near-misses for threshold tuning
        if diagnostic and near_misses:
            for nm in near_misses[:5]:
                logger.info(
                    f"  NEAR-MISS [{nm['score']:3d}] "
                    f"{nm['title'][:50]} → {nm['url'][:60]}"
                )

        # Write to scrape_registry
        urls_written = 0
        if top_candidates:
            with engine.connect() as conn:
                for c in top_candidates:
                    content_type = "pdf" if c["url"].lower().endswith(".pdf") else "html"
                    result = conn.execute(text(f"""
                        INSERT INTO {schema}.scrape_registry
                            (pwsid, url, url_source, discovery_query,
                             content_type, status)
                        VALUES
                            (:pwsid, :url, 'searxng', :query,
                             :ctype, 'pending')
                        ON CONFLICT (pwsid, url) DO NOTHING
                    """), {
                        "pwsid": pwsid,
                        "url": c["url"],
                        "query": c["query"],
                        "ctype": content_type,
                    })
                    if result.rowcount > 0:
                        urls_written += 1
                        logger.info(f"  → [{c['score']}] {c['url'][:80]}")

                # Update pwsid_coverage.scrape_status
                if urls_written > 0:
                    conn.execute(text(f"""
                        UPDATE {schema}.pwsid_coverage
                        SET scrape_status = 'url_discovered'
                        WHERE pwsid = :pwsid AND scrape_status = 'not_attempted'
                    """), {"pwsid": pwsid})

                conn.commit()

        # Fix 2: Always mark search attempted (prevents infinite re-queuing)
        self._mark_searched(pwsid, schema)

        # Fix 10: Log full scoring funnel to search_log
        self._log_search(
            pwsid=pwsid,
            schema=schema,
            queries_run=len(queries),
            raw_results_count=raw_result_count,
            deduped_count=len(seen_urls),
            above_threshold_count=len(above_threshold),
            near_miss_count=len(near_misses),
            below_threshold_count=len(below_threshold),
            written_count=urls_written,
            best_score=top_candidates[0]["score"] if top_candidates else 0,
            best_url=top_candidates[0]["url"] if top_candidates else None,
        )

        # Fix 3: Log to pipeline_runs for visibility
        self.log_run(
            status="success" if urls_written > 0 else "no_results",
            rows_affected=urls_written,
            notes=(
                f"{utility_name} ({state}): "
                f"{raw_result_count} raw → {len(seen_urls)} dedup → "
                f"{len(above_threshold)} scored >50 → {urls_written} written"
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
        queries_run: int,
        raw_results_count: int,
        deduped_count: int,
        above_threshold_count: int,
        near_miss_count: int,
        below_threshold_count: int,
        written_count: int,
        best_score: float,
        best_url: str | None,
    ) -> None:
        """Log the full scoring funnel to search_log for threshold tuning."""
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.search_log
                        (pwsid, queries_run, raw_results_count, deduped_count,
                         above_threshold_count, near_miss_count,
                         below_threshold_count, written_count,
                         best_score, best_url)
                    VALUES
                        (:pwsid, :queries, :raw, :dedup,
                         :above, :near, :below, :written,
                         :score, :url)
                """), {
                    "pwsid": pwsid,
                    "queries": queries_run,
                    "raw": raw_results_count,
                    "dedup": deduped_count,
                    "above": above_threshold_count,
                    "near": near_miss_count,
                    "below": below_threshold_count,
                    "written": written_count,
                    "score": best_score,
                    "url": best_url,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"  search_log write failed: {e}")

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
Modified: 2026-03-24

Dependencies:
    - requests (SearXNG API)
    - anthropic (optional, Haiku for ambiguous URL scoring)

Usage:
    from utility_api.agents.discovery import DiscoveryAgent
    result = DiscoveryAgent().run(pwsid='VA4760100')

Notes:
    - Does NOT scrape or parse — only discovers and records URLs
    - Writes to scrape_registry with status='pending'
    - Updates pwsid_coverage.scrape_status to 'url_discovered'
    - Keyword heuristic handles ~80% of cases without LLM
"""

import time

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine


# --- Search Query Construction ---

def build_search_queries(utility_name: str, state: str) -> list[str]:
    """Generate targeted search queries for rate page discovery."""
    queries = [
        f'"{utility_name}" water rate schedule',
        f'"{utility_name}" schedule of rates fees charges',
        f'"{utility_name}" water rates {state} filetype:pdf',
    ]
    name_lower = utility_name.lower()
    if "water" not in name_lower:
        queries.append(f'"{utility_name}" water department rates')
    if "authority" in name_lower or "district" in name_lower:
        queries.append(f'"{utility_name}" rate schedule')
    return queries


# --- URL Relevance Scoring ---

def score_url_relevance(url: str, title: str, snippet: str) -> int:
    """Score 0-100 using keyword heuristics. No LLM needed for most cases."""
    score = 0
    combined = f"{url} {title} {snippet}".lower()

    # Positive signals
    for kw in ["rate", "schedule", "tariff", "water bill", "fee schedule",
                "rate structure", "charges", "pricing", "rate study"]:
        if kw in combined:
            score += 15

    # Strong positive — PDF rate schedules
    if url.lower().endswith(".pdf"):
        if any(kw in combined for kw in ["rate", "schedule", "fee", "tariff"]):
            score += 20

    # Negative signals
    for neg in ["meeting", "agenda", "minutes", "news", "press release",
                "election", "job", "career", "bid", "rfp"]:
        if neg in combined:
            score -= 20

    return max(0, min(100, score))


def score_with_llm_fallback(
    url: str, title: str, snippet: str,
    utility_name: str, state: str,
) -> int:
    """Keyword score first. Haiku only for ambiguous cases (score 30-60)."""
    keyword_score = score_url_relevance(url, title, snippet)

    if keyword_score > 60 or keyword_score < 30:
        return keyword_score  # confident, no LLM needed

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
    """Run a SearXNG search and return results."""
    import requests

    try:
        r = requests.get(
            "http://localhost:8888/search",
            params={"q": query, "format": "json", "categories": "general"},
            timeout=15,
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
        search_delay: float = 2.0,
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
        search_delay : float
            Seconds between search queries.

        Returns
        -------
        dict
            pwsid, urls_found, urls_written.
        """
        schema = settings.utility_schema

        # Look up utility info if not provided
        if not utility_name or not state:
            with engine.connect() as conn:
                row = conn.execute(text(f"""
                    SELECT pws_name, state_code FROM {schema}.pwsid_coverage
                    WHERE pwsid = :pwsid
                """), {"pwsid": pwsid}).fetchone()
                if row:
                    utility_name = utility_name or row.pws_name
                    state = state or row.state_code
                else:
                    state = state or pwsid[:2]
                    utility_name = utility_name or pwsid

        logger.info(f"DiscoveryAgent: {utility_name} ({pwsid}, {state})")

        # Build and run search queries
        queries = build_search_queries(utility_name, state)
        all_candidates = []
        seen_urls = set()

        for query in queries:
            results = _searxng_search(query)
            for r in results:
                url = r["url"]
                if url in seen_urls or not url.startswith("http"):
                    continue
                seen_urls.add(url)

                if use_llm:
                    score = score_with_llm_fallback(
                        url, r["title"], r["snippet"], utility_name, state
                    )
                else:
                    score = score_url_relevance(url, r["title"], r["snippet"])

                all_candidates.append({
                    "url": url,
                    "title": r["title"],
                    "snippet": r["snippet"],
                    "score": score,
                    "query": query,
                })

            time.sleep(search_delay)

        # Sort by score, take top candidates (score > 50)
        all_candidates.sort(key=lambda c: c["score"], reverse=True)
        top_candidates = [c for c in all_candidates if c["score"] > 50][:3]

        logger.info(f"  Found {len(all_candidates)} URLs, {len(top_candidates)} scored >50")

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

        self.log_run(
            status="success",
            rows_affected=urls_written,
            notes=f"{utility_name}: {len(all_candidates)} found, {urls_written} written",
        )

        return {
            "pwsid": pwsid,
            "urls_found": len(all_candidates),
            "urls_written": urls_written,
            "top_candidates": top_candidates,
        }

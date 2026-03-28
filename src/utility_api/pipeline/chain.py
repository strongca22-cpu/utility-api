#!/usr/bin/env python3
"""
Unified Scrape -> Parse -> BestEstimate Chain

Purpose:
    Single implementation of the scrape -> parse -> best_estimate pipeline.
    All pipeline entry points should call scrape_and_parse() instead of
    manually chaining ScrapeAgent -> ParseAgent. This eliminates the class
    of bugs where different scripts chain the agents differently (the root
    cause of Sprint 23).

Author: AI-Generated
Created: 2026-03-28
Modified: 2026-03-28

Dependencies:
    - utility_api.agents.scrape
    - utility_api.agents.parse
    - utility_api.agents.best_estimate

Usage:
    from utility_api.pipeline.chain import scrape_and_parse

    # Single URL
    result = scrape_and_parse(pwsid='VA4760100', skip_best_estimate=True)

    # Specific registry entry
    result = scrape_and_parse(pwsid='VA4760100', registry_id=12345)

    # Batch: skip best_estimate, run once per state at end
    for pwsid in batch:
        result = scrape_and_parse(pwsid=pwsid, skip_best_estimate=True)
    BestEstimateAgent().run(state='VA')

Notes:
    - ScrapeAgent persists scraped_text to DB (Sprint 23 Fix 1)
    - ParseAgent reads from DB if raw_text not in memory (Sprint 23 Fix 1)
    - BestEstimate is batched per state unless skip_best_estimate=True
"""

from loguru import logger

from utility_api.agents.parse import ParseAgent
from utility_api.agents.scrape import ScrapeAgent


def scrape_and_parse(
    pwsid: str,
    url: str | None = None,
    registry_id: int | None = None,
    skip_best_estimate: bool = False,
    max_depth: int | None = None,
) -> dict:
    """Atomic scrape -> parse -> best_estimate chain.

    All pipeline entry points should call this instead of manually
    chaining ScrapeAgent -> ParseAgent.

    Parameters
    ----------
    pwsid : str
        EPA PWSID.
    url : str, optional
        Specific URL to scrape. If not provided, scrapes all pending
        URLs for the PWSID.
    registry_id : int, optional
        Specific scrape_registry entry to process.
    skip_best_estimate : bool
        If True, caller handles BestEstimate batching (recommended
        for batch processing — run once per state at end).
    max_depth : int, optional
        Override deep crawl depth for ScrapeAgent.

    Returns
    -------
    dict
        success, parse_results (list), pwsid, url.
    """
    scrape = ScrapeAgent()
    parse = ParseAgent()

    # Step 1: Scrape (persists text to DB via Sprint 23 Fix 1)
    scrape_kwargs = {}
    if registry_id is not None:
        scrape_kwargs["registry_id"] = registry_id
    if pwsid:
        scrape_kwargs["pwsid"] = pwsid
    if max_depth is not None:
        scrape_kwargs["max_depth"] = max_depth

    scrape_result = scrape.run(**scrape_kwargs)

    if not scrape_result or not scrape_result.get("raw_texts"):
        return {"success": False, "error": "scrape_failed", "pwsid": pwsid, "url": url}

    # Step 2: Parse each scraped text
    results = []
    for text_entry in scrape_result["raw_texts"]:
        parse_result = parse.run(
            pwsid=pwsid,
            raw_text=text_entry["text"],
            content_type=text_entry.get("content_type", "html"),
            source_url=text_entry.get("url", url),
            registry_id=text_entry.get("registry_id", registry_id),
            skip_best_estimate=True,  # always batch — handled below
        )
        results.append(parse_result)

    any_success = any(r.get("success") for r in results)

    # Step 3: BestEstimate (unless caller is batching)
    if any_success and not skip_best_estimate:
        try:
            from utility_api.agents.best_estimate import BestEstimateAgent

            state = pwsid[:2]
            BestEstimateAgent().run(state=state)
        except Exception as e:
            logger.debug(f"  Best estimate update skipped: {e}")

    return {
        "success": any_success,
        "parse_results": results,
        "pwsid": pwsid,
        "url": url,
    }

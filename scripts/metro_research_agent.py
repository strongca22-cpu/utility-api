#!/usr/bin/env python3
"""
Metro Research Agent

Purpose:
    Uses Claude API with web_search tool to find water utility rate page URLs.
    Processes utilities in batches of 10. Parses YAML results from the last
    text block in the interleaved response (text / tool_use / tool_result).

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - anthropic
    - pyyaml
    - utility_api (local package, for .env loading)

Usage:
    from scripts.metro_research_agent import research_metro, research_batch
    results = research_metro(context)  # context from metro_template_generator

Notes:
    - Model: claude-sonnet-4-20250514 (not Opus — URL finding is search, not reasoning)
    - Batch size: 10 utilities per API call
    - Pacing: 2 second sleep between API calls
    - Response format: interleaved text/tool_use/tool_result blocks;
      YAML results are in the LAST text block after all searches complete
    - Requires ANTHROPIC_API_KEY in environment (loaded from .env)

Configuration:
    - ANTHROPIC_API_KEY environment variable (loaded by utility_api.config)
"""

import re
import sys
import time
from pathlib import Path

import yaml
from loguru import logger

# Ensure project src is importable — triggers .env loading for ANTHROPIC_API_KEY
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import utility_api.config  # noqa: F401 — side effect: loads .env into os.environ

import anthropic

# Constants
MODEL = "claude-sonnet-4-20250514"
BATCH_SIZE = 10
MAX_TOKENS = 4000
SLEEP_BETWEEN_CALLS = 2

SYSTEM_PROMPT = """You are a water utility rate page researcher. For each utility listed, use web search to find the URL of their current water rate schedule or rate information page.

Rules:
- Search for the ACTUAL RATE PAGE with dollar amounts, tier structures, or fee schedules
- Do NOT return general "about our water service" pages — find the rates/fees page specifically
- For city utilities, try: "{city name} water rates" or "{city name} utility rates"
- For water districts, try: "{district name} rates" or "{district name} rate schedule"
- If the utility is served by a larger regional provider, find that provider's rate page
- Prefer .gov and .org domains
- If no rate page exists online after searching, say so honestly
- Return ONLY URLs you confirmed exist via search results

Return results as valid YAML. For each utility use this exact format:

- pwsid: "XX1234567"
  url: "https://exact-url-to-rate-page"
  confidence: high
  notes: "Found on city website rates page"

If you cannot find a rate page:

- pwsid: "XX1234567"
  url: null
  confidence: none
  notes: "No rate page found - utility website has no published rates"

Confidence levels:
- high: URL goes directly to a page with rate/fee dollar amounts
- medium: URL goes to a utility page that likely links to rates (may need one click)
- low: URL is a general city/utility site, rates may be buried
- none: no rate page found"""


def _chunk(items: list, size: int) -> list[list]:
    """Split a list into chunks of the given size."""
    return [items[i : i + size] for i in range(0, len(items), size)]


def _extract_yaml_from_response(response) -> list[dict]:
    """Extract YAML results from the last text block in the API response.

    The response contains interleaved content blocks: text, tool_use (search
    calls), and tool_result (search results). The final YAML with discovered
    URLs is in the LAST text block after all searches complete.
    """
    # Filter for text blocks only, take the last one
    text_blocks = [
        block.text for block in response.content if block.type == "text"
    ]

    if not text_blocks:
        logger.warning("No text blocks in API response")
        return []

    raw_text = text_blocks[-1]

    # Strip markdown fences if present
    cleaned = re.sub(r"```ya?ml\s*\n?", "", raw_text)
    cleaned = re.sub(r"```\s*$", "", cleaned)
    cleaned = cleaned.strip()

    # Find the YAML list (starts with "- pwsid:")
    yaml_match = re.search(r"(- pwsid:.*)", cleaned, re.DOTALL)
    if yaml_match:
        cleaned = yaml_match.group(1)

    try:
        results = yaml.safe_load(cleaned)
    except yaml.YAMLError as e:
        logger.warning(f"YAML parse error: {e}")
        logger.debug(f"Raw text was:\n{raw_text[:500]}")
        return []

    if not isinstance(results, list):
        logger.warning(f"Expected list from YAML, got {type(results).__name__}")
        return []

    # Normalize: ensure every result has expected keys
    normalized = []
    for r in results:
        if not isinstance(r, dict) or "pwsid" not in r:
            continue
        normalized.append({
            "pwsid": str(r.get("pwsid", "")),
            "url": r.get("url"),
            "confidence": str(r.get("confidence", "none")),
            "notes": str(r.get("notes", "")),
        })

    return normalized


def research_batch(utilities: list[dict], metro_name: str) -> list[dict]:
    """Research up to 10 utilities in one API call.

    Args:
        utilities: List of utility dicts (pwsid, pws_name, city, county, state, population).
        metro_name: Metro area name for context.

    Returns:
        List of result dicts with pwsid, url, confidence, notes.
    """
    client = anthropic.Anthropic()

    # Build the user message with utility details
    utility_descriptions = []
    for u in utilities:
        pop = u.get("population", 0)
        desc = (
            f"- PWSID: {u['pwsid']}, Name: {u['pws_name']}, "
            f"City: {u['city']}, County: {u['county']}, "
            f"State: {u['state']}, Pop: {pop:,}"
        )
        utility_descriptions.append(desc)

    user_message = (
        f"Find water rate page URLs for these utilities in the "
        f"{metro_name} metro area.\n"
        f"Search for each one individually.\n\n"
        + "\n".join(utility_descriptions)
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": user_message}],
    )

    # Log token usage for cost tracking
    usage = response.usage
    logger.debug(
        f"  API usage: input={usage.input_tokens} output={usage.output_tokens}"
    )

    results = _extract_yaml_from_response(response)

    # Warn if we got fewer results than utilities sent
    if len(results) < len(utilities):
        logger.warning(
            f"  Got {len(results)} results for {len(utilities)} utilities "
            f"— some may have been skipped by the model"
        )

    return results


def research_metro(context: dict) -> list[dict]:
    """Research all utilities in a metro context, batched.

    Args:
        context: Metro context dict from generate_metro_context().

    Returns:
        Combined list of all result dicts.
    """
    utilities = context["utilities"]
    metro_name = context["metro_name"]
    all_results = []

    batches = _chunk(utilities, BATCH_SIZE)
    logger.info(
        f"Researching {len(utilities)} utilities in {len(batches)} batches"
    )

    for i, batch in enumerate(batches, 1):
        pop_range = (
            f"{batch[-1]['population']:,} - {batch[0]['population']:,}"
        )
        logger.info(
            f"  Batch {i}/{len(batches)}: {len(batch)} utilities "
            f"(pop range: {pop_range})"
        )

        results = research_batch(batch, metro_name)
        all_results.extend(results)

        found = sum(1 for r in results if r.get("url"))
        logger.info(f"  Found {found}/{len(batch)} URLs")

        # Polite pacing between calls
        if i < len(batches):
            time.sleep(SLEEP_BETWEEN_CALLS)

    found_total = sum(1 for r in all_results if r.get("url"))
    logger.info(
        f"Research complete: {found_total}/{len(all_results)} URLs found "
        f"across {len(batches)} batches"
    )

    return all_results


if __name__ == "__main__":
    # Quick test with 2 Portland utilities
    test_utilities = [
        {
            "pwsid": "OR4100657",
            "pws_name": "PORTLAND WATER BUREAU",
            "city": "PORTLAND",
            "county": "Multnomah",
            "state": "OR",
            "population": 650000,
        },
        {
            "pwsid": "OR4100697",
            "pws_name": "TUALATIN VALLEY WATER DISTRICT",
            "city": "BEAVERTON",
            "county": "Washington",
            "state": "OR",
            "population": 230000,
        },
    ]

    print("Testing research_batch with 2 Portland utilities...")
    print("(This will make 1 API call with web search — ~$0.15-0.20)\n")

    results = research_batch(test_utilities, "Portland-Vancouver-Hillsboro")
    for r in results:
        url_display = r.get("url", "none")
        if url_display and len(url_display) > 60:
            url_display = url_display[:60] + "..."
        print(
            f"  {r['pwsid']} | {r.get('confidence', '?'):6s} | {url_display}"
        )

#!/usr/bin/env python3
"""
Metro Research Agent

Purpose:
    Uses Claude API with web_search tool to find water utility rate page URLs.
    Supports two modes:
    - Immediate: synchronous API calls, one batch at a time (original)
    - Batch: submits all requests to the Message Batches API (50% cheaper,
      24hr SLA, results collected separately)

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - anthropic
    - pyyaml
    - utility_api (local package, for .env loading)

Usage:
    # Immediate mode
    from scripts.metro_research_agent import research_metro
    results = research_metro(context)

    # Batch mode
    from scripts.metro_research_agent import submit_batch_request, collect_batch_results
    batch_id = submit_batch_request(all_requests)
    results = collect_batch_results(batch_id)

Notes:
    - Model: claude-sonnet-4-20250514 (not Opus — URL finding is search, not reasoning)
    - Batch size: 5 utilities per API call
    - Batch API: 50% cost reduction, 24hr SLA (usually minutes)
    - Response format: interleaved text/tool_use/tool_result blocks;
      YAML results are in the LAST text block after all searches complete
    - Requires ANTHROPIC_API_KEY in environment (loaded from .env)

Configuration:
    - ANTHROPIC_API_KEY environment variable (loaded by utility_api.config)
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
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
BATCH_SIZE = 5
MAX_TOKENS = 16000
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

    # Handle various response shapes:
    # - list of dicts (expected)
    # - dict with a key containing a list (e.g., {"results": [...]})
    # - str (model returned prose instead of YAML)
    if isinstance(results, dict):
        # Try to find a list value inside the dict
        for key, val in results.items():
            if isinstance(val, list) and val and isinstance(val[0], dict):
                results = val
                break
        else:
            # Single dict — might be one result wrapped
            if "pwsid" in results:
                results = [results]
            else:
                logger.warning(
                    f"Got dict without recognizable structure. "
                    f"Keys: {list(results.keys())[:5]}"
                )
                logger.debug(f"Raw text was:\n{raw_text[:500]}")
                return []

    if not isinstance(results, list):
        logger.warning(f"Expected list from YAML, got {type(results).__name__}")
        logger.debug(f"Raw text was:\n{raw_text[:500]}")
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

    # Retry with exponential backoff for transient errors (overloaded, rate limit)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": user_message}],
            )
            break
        except (anthropic.APIStatusError, anthropic.APIConnectionError) as e:
            wait = 2 ** (attempt + 1)  # 2, 4, 8 seconds
            if attempt < max_retries - 1:
                logger.warning(
                    f"  API error (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                logger.error(f"  API error after {max_retries} attempts: {e}. Skipping batch.")
                return []

    # Log token usage and stop reason for diagnostics
    usage = response.usage
    logger.debug(
        f"  API usage: input={usage.input_tokens} output={usage.output_tokens} "
        f"stop_reason={response.stop_reason}"
    )

    if response.stop_reason == "max_tokens":
        logger.warning("  Response truncated (hit max_tokens) — YAML may be incomplete")

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


# ---------------------------------------------------------------------------
# Batch API mode — submit all requests at once, collect results later
# 50% cheaper than immediate mode, 24hr SLA (usually minutes)
# ---------------------------------------------------------------------------

BATCH_DIR = PROJECT_ROOT / "data" / "metro_batches"


def _build_user_message(utilities: list[dict], metro_name: str) -> str:
    """Build the user message for a batch of utilities."""
    utility_descriptions = []
    for u in utilities:
        pop = u.get("population", 0)
        desc = (
            f"- PWSID: {u['pwsid']}, Name: {u['pws_name']}, "
            f"City: {u['city']}, County: {u['county']}, "
            f"State: {u['state']}, Pop: {pop:,}"
        )
        utility_descriptions.append(desc)

    return (
        f"Find water rate page URLs for these utilities in the "
        f"{metro_name} metro area.\n"
        f"Search for each one individually.\n\n"
        + "\n".join(utility_descriptions)
    )


def build_batch_requests(metro_contexts: list[dict]) -> list[dict]:
    """Build Batch API request objects from metro contexts.

    Args:
        metro_contexts: List of dicts, each with 'metro_name', 'metro_id',
                       and 'utilities' (already filtered/truncated).

    Returns:
        List of batch request dicts with custom_id and params.
    """
    requests = []

    for ctx in metro_contexts:
        metro_id = ctx["metro_id"]
        metro_name = ctx["metro_name"]
        utilities = ctx["utilities"]
        batches = _chunk(utilities, BATCH_SIZE)

        for batch_idx, batch in enumerate(batches):
            custom_id = f"{metro_id}__batch_{batch_idx:03d}"
            user_message = _build_user_message(batch, metro_name)

            requests.append({
                "custom_id": custom_id,
                "params": {
                    "model": MODEL,
                    "max_tokens": MAX_TOKENS,
                    "system": SYSTEM_PROMPT,
                    "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                    "messages": [{"role": "user", "content": user_message}],
                },
            })

    return requests


def submit_batch_request(
    requests: list[dict],
    metro_ids: list[str],
) -> str:
    """Submit requests to the Anthropic Message Batches API.

    Args:
        requests: List of batch request dicts from build_batch_requests().
        metro_ids: List of metro IDs included (for metadata).

    Returns:
        Batch ID string.
    """
    client = anthropic.Anthropic()

    logger.info(f"Submitting {len(requests)} requests to Batch API...")

    batch = client.messages.batches.create(requests=requests)
    batch_id = batch.id

    logger.info(f"  Batch submitted: {batch_id}")
    logger.info(f"  Status: {batch.processing_status}")

    # Save metadata for later collection
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    metadata = {
        "batch_id": batch_id,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "metro_ids": metro_ids,
        "total_requests": len(requests),
        "request_ids": [r["custom_id"] for r in requests],
        "status": batch.processing_status,
        "cost_estimate_usd": len(requests) * 0.38,  # ~$0.75/batch at 50% discount
    }

    metadata_path = BATCH_DIR / f"{batch_id}.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"  Metadata saved: {metadata_path}")
    logger.info(
        f"  Estimated cost: ${metadata['cost_estimate_usd']:.2f} "
        f"({len(requests)} requests × ~$0.38 batch rate)"
    )

    return batch_id


def check_batch_status(batch_id: str) -> dict:
    """Check status of a submitted batch.

    Returns:
        Dict with status, counts, and batch_id.
    """
    client = anthropic.Anthropic()
    batch = client.messages.batches.retrieve(batch_id)

    status = {
        "batch_id": batch_id,
        "processing_status": batch.processing_status,
        "created_at": str(batch.created_at),
        "request_counts": {
            "processing": batch.request_counts.processing,
            "succeeded": batch.request_counts.succeeded,
            "errored": batch.request_counts.errored,
            "canceled": batch.request_counts.canceled,
            "expired": batch.request_counts.expired,
        },
    }

    total = (
        batch.request_counts.processing
        + batch.request_counts.succeeded
        + batch.request_counts.errored
        + batch.request_counts.canceled
        + batch.request_counts.expired
    )
    succeeded = batch.request_counts.succeeded

    logger.info(
        f"Batch {batch_id}: {batch.processing_status} "
        f"({succeeded}/{total} succeeded, "
        f"{batch.request_counts.errored} errored, "
        f"{batch.request_counts.processing} processing)"
    )

    return status


def collect_batch_results(batch_id: str) -> dict[str, list[dict]]:
    """Download and parse results from a completed batch.

    Args:
        batch_id: The batch ID from submit_batch_request().

    Returns:
        Dict mapping metro_id to list of result dicts (pwsid, url, confidence, notes).
    """
    client = anthropic.Anthropic()

    # Check status first
    batch = client.messages.batches.retrieve(batch_id)
    if batch.processing_status != "ended":
        logger.warning(
            f"Batch {batch_id} is still {batch.processing_status}. "
            f"Cannot collect results yet."
        )
        return {}

    logger.info(
        f"Collecting results from batch {batch_id} "
        f"({batch.request_counts.succeeded} succeeded, "
        f"{batch.request_counts.errored} errored)"
    )

    # Stream results
    results_by_metro: dict[str, list[dict]] = {}
    succeeded = 0
    failed = 0
    total_urls = 0

    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id
        # Parse metro_id from custom_id: "denver__batch_003"
        metro_id = custom_id.rsplit("__batch_", 1)[0]

        if metro_id not in results_by_metro:
            results_by_metro[metro_id] = []

        if result.result.type == "succeeded":
            response = result.result.message
            parsed = _extract_yaml_from_response(response)
            results_by_metro[metro_id].extend(parsed)
            found = sum(1 for r in parsed if r.get("url"))
            total_urls += found
            succeeded += 1

            if parsed:
                logger.debug(f"  {custom_id}: {found}/{len(parsed)} URLs")
            else:
                logger.debug(f"  {custom_id}: no parseable results")
        else:
            failed += 1
            error_type = result.result.type
            logger.warning(f"  {custom_id}: {error_type}")

    logger.info(
        f"Collection complete: {succeeded} succeeded, {failed} failed, "
        f"{total_urls} URLs found across {len(results_by_metro)} metros"
    )

    # Save raw results for reference
    results_path = BATCH_DIR / f"{batch_id}_results.json"
    serializable = {
        metro_id: results
        for metro_id, results in results_by_metro.items()
    }
    with open(results_path, "w") as f:
        json.dump(serializable, f, indent=2)
    logger.info(f"  Results saved: {results_path}")

    return results_by_metro


def list_batches() -> None:
    """List all batch metadata files."""
    if not BATCH_DIR.exists():
        print("No batches submitted yet.")
        return

    batch_files = sorted(BATCH_DIR.glob("*.json"))
    # Exclude results files
    batch_files = [f for f in batch_files if not f.stem.endswith("_results")]

    if not batch_files:
        print("No batches submitted yet.")
        return

    print(f"\n{'Batch ID':<35} {'Submitted':<22} {'Requests':>8} {'Est. Cost':>10} {'Metros'}")
    print("-" * 100)
    for f in batch_files:
        with open(f) as fp:
            meta = json.load(fp)
        submitted = meta.get("submitted_at", "?")[:19]
        metros = ", ".join(meta.get("metro_ids", []))
        cost = f"${meta.get('cost_estimate_usd', 0):.2f}"
        print(
            f"{meta.get('batch_id', '?'):<35} {submitted:<22} "
            f"{meta.get('total_requests', '?'):>8} {cost:>10} {metros}"
        )
    print()


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

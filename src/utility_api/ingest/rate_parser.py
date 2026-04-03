#!/usr/bin/env python3
"""
Claude API Rate Parser

Purpose:
    Send scraped utility rate page text to Claude API for structured
    extraction of water rate schedules. Returns parsed tier structure,
    fixed charges, and metadata in a structured format.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - anthropic (Anthropic Python SDK)

Usage:
    from utility_api.ingest.rate_parser import parse_rate_text
    result = parse_rate_text(page_text, utility_name="City of Richmond")

Notes:
    - Uses Claude API (single request mode for development; Batch API later)
    - Structured JSON output via system prompt + tool_use
    - parse_confidence reflects extraction quality: high/medium/low/failed
    - Handles: flat rates, uniform rates, increasing block (tiered) rates
    - Does NOT handle: seasonal rates, budget-based rates (flagged for review)
    - ANTHROPIC_API_KEY must be set in environment or .env

Configuration:
    - ANTHROPIC_API_KEY: Required. Set in .env or environment.
    - Default model: claude-sonnet-4-20250514 (fast, cheap, good for extraction)

Data Sources:
    - Input: Scraped text from rate_scraper
    - Output: Structured rate data for rate_schedules table
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

from loguru import logger


# Rate extraction prompt — the core of the LLM pipeline
SYSTEM_PROMPT = """You are a water utility rate analyst. Your job is to extract structured
water rate information from utility website text.

You will receive text scraped from a water utility's rate page. Extract the RESIDENTIAL
water rate structure and return a JSON object with the following fields.

Rules:
- Extract RESIDENTIAL rates only (not commercial, industrial, or irrigation)
- WATER ONLY: Many utility pages list both water and sewer/wastewater charges together.
  Extract ONLY the water supply charges. Ignore all sewer, wastewater, stormwater,
  reclaimed water, and solid waste fees. If a page shows a combined "water & sewer" bill
  total, do NOT use that number — find the water-only components (base charge + volumetric
  rate for water service). If you truly cannot separate water from sewer charges, set
  parse_confidence to "low" and explain in notes what you found.
- Rates may appear in legal/ordinance format with section numbering
  (e.g., "Section 52-44(a)(1)"). Ignore the legal formatting and extract the rate
  values. Ordinances often express tiers as "First X gallons: $Y" or
  "All over X gallons: $Y per 1,000 gallons" — these are standard tiered structures.
- Text from PDFs may have garbled table formatting where columns run together on one line
  or split across lines without clear alignment. Look for patterns of tier labels near
  dollar amounts and CCF/gallon limits even if they aren't in a clean table layout.
  Reconstruct the rate structure from context clues — e.g., if you see
  "Tier 1 0-5 CCF $3.50 Tier 2 5-15 CCF $4.75" on a single line, that is two tiers.
- If multiple meter sizes are listed, use the smallest standard residential size (typically 5/8" or 3/4")
- Convert all volumetric rates to $/CCF (1 CCF = 100 cubic feet = 748 gallons)
  - If rates are in $/1,000 gallons: multiply by 0.748 to get $/CCF (since 1 CCF = 748 gal, which is 0.748 thousand gal)
  - If rates are in $/gallon: multiply by 748 to get $/CCF
  - If rates are in $/HCF: that IS $/CCF (HCF = hundred cubic feet = CCF)
- Convert tier limits to CCF:
  - If limits are in gallons: divide by 748 to get CCF
  - If limits are in thousands of gallons (Kgal): multiply by 1.337 to get CCF (1 Kgal = 1.337 CCF)
- If billing is bimonthly or quarterly, normalize fixed_charge_monthly to monthly (divide by 2 or 3)
- List tiers in ascending order of consumption
- For flat/uniform rates (single volumetric price), use tier_1 only with null limit
- rate_structure_type MUST be exactly one of these 6 values:
  - "flat" — single fixed price regardless of consumption (no volumetric charge)
  - "uniform" — fixed charge plus a single volumetric rate (same $/CCF at all volumes)
  - "increasing_block" — volumetric rate increases at higher consumption tiers
  - "decreasing_block" — volumetric rate decreases at higher consumption tiers
  - "budget_based" — rates based on an individualized allocation/budget
  - "seasonal" — rates vary by season (summer/winter). If also tiered, still use "seasonal"
  Do NOT use other values like "tiered", "flat_rate", "tiered_volumetric", etc.
  "tiered" with increasing rates = "increasing_block". Single rate + fixed charge = "uniform".
- If you cannot determine the rate structure, set parse_confidence to "failed" and explain in notes
- If the page text doesn't contain rate information, set parse_confidence to "failed"
- Be precise with numbers — do not round or estimate"""


# Domain blacklist — near-100% parse failure rate across thousands of attempts.
# URLs from these domains are skipped at batch submission time.
# They stay in scrape_registry (never delete data) but are deprioritized.
DOMAIN_BLACKLIST = {
    "www.ny.gov",                           # 143/143 failures (100%) — state portal
    "www.nyc.gov",                          # 130/136 failures (95.6%) — city portal
    "houstonwaterbills.houstontx.gov",      # 133/133 failures (100%) — bill calculator
    "psc.wi.gov",                           # 93/93 failures (100%) — regulatory filings
    "www.louisianawater.com",               # 82/82 failures (100%) — utility portal
    "dam.assets.ohio.gov",                  # 171/176 failures (97.2%) — document archive
}


# JSON field list used in user messages — single source of truth
_JSON_FIELDS = (
    "rate_effective_date, rate_structure_type, billing_frequency, fixed_charge_monthly, "
    "meter_size_inches, tier_1_limit_ccf, tier_1_rate, tier_2_limit_ccf, tier_2_rate, "
    "tier_3_limit_ccf, tier_3_rate, tier_4_limit_ccf, tier_4_rate, parse_confidence, notes"
)

# Retry addendum — prepended to user message on second parse attempt
_RETRY_ADDENDUM = """IMPORTANT: A previous extraction attempt found no rate data. \
Look more carefully for:
- Rates expressed as $/gallon, $/ccf, $/1000 gallons, per unit
- Monthly service charges or base charges
- Water charges listed in a fee schedule or budget document
- Rates that may be embedded in a table or list format
- Look for water rates SEPARATE from sewer/wastewater charges — many pages combine them. \
Extract only the water portion.
- Rates in legal/ordinance format (e.g., "Section 12.04.030") are valid — extract the \
dollar amounts regardless of legal numbering.
- If the text appears to be from a PDF with garbled table formatting, reconstruct the \
rate table from the numbers and keywords present.
- If you can identify a base/fixed monthly charge and a single volumetric rate but not \
a full tier structure, extract what you have as a flat or uniform rate. A partial \
extraction with confidence "medium" is better than a failed extraction.
If you find ANY numeric water charge, extract it even if the full tier structure is unclear.

"""


def build_parse_user_message(
    page_text: str,
    utility_name: str = "",
    state_code: str = "",
    content_type: str = "html",
    retry: bool = False,
) -> str:
    """Build the user message for rate extraction, shared across all code paths.

    Parameters
    ----------
    page_text : str
        Scraped text content (caller is responsible for any truncation).
    utility_name : str
        Utility name for context (omitted if empty).
    state_code : str
        Two-letter state code for context.
    content_type : str
        "html" or "pdf".
    retry : bool
        If True, prepend the retry addendum with more aggressive search hints.

    Returns
    -------
    str
        Complete user message ready to send to the API.
    """
    # Context line
    context_parts = []
    if utility_name:
        context_parts.append(utility_name)
    if state_code:
        context_parts.append(state_code)
    context_line = f"Context: {' | '.join(context_parts)}\n\n" if context_parts else ""

    # Retry preamble
    retry_block = _RETRY_ADDENDUM if retry else ""

    return (
        f"Extract the residential water rate structure from this {content_type} text.\n\n"
        f"{retry_block}"
        f"{context_line}"
        f"--- BEGIN SCRAPED TEXT ---\n"
        f"{page_text}\n"
        f"--- END SCRAPED TEXT ---\n\n"
        f"Return ONLY a valid JSON object (no markdown, no explanation) with these fields:\n"
        f"{_JSON_FIELDS}.\n\n"
        f"If there is no rate information on this page, set parse_confidence to "
        f'"failed" and explain why in notes.'
    )


EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "rate_effective_date": {
            "type": ["string", "null"],
            "description": "Date the rate schedule took effect (YYYY-MM-DD or null if unknown)",
        },
        "rate_structure_type": {
            "type": "string",
            "enum": ["flat", "uniform", "increasing_block", "decreasing_block",
                     "budget_based", "seasonal", "unknown"],
            "description": "Type of rate structure",
        },
        "billing_frequency": {
            "type": ["string", "null"],
            "enum": ["monthly", "bimonthly", "quarterly", None],
        },
        "fixed_charge_monthly": {
            "type": ["number", "null"],
            "description": "Base/service/availability charge normalized to $/month",
        },
        "meter_size_inches": {
            "type": ["number", "null"],
            "description": "Meter size this fixed charge applies to (e.g., 0.625 for 5/8 inch)",
        },
        "tier_1_limit_ccf": {
            "type": ["number", "null"],
            "description": "Tier 1 upper limit in CCF (null for flat/uniform rates)",
        },
        "tier_1_rate": {
            "type": ["number", "null"],
            "description": "Volumetric rate for tier 1 in $/CCF",
        },
        "tier_2_limit_ccf": {"type": ["number", "null"]},
        "tier_2_rate": {"type": ["number", "null"]},
        "tier_3_limit_ccf": {"type": ["number", "null"]},
        "tier_3_rate": {"type": ["number", "null"]},
        "tier_4_limit_ccf": {"type": ["number", "null"]},
        "tier_4_rate": {"type": ["number", "null"]},
        "parse_confidence": {
            "type": "string",
            "enum": ["high", "medium", "low", "failed"],
            "description": (
                "high: clear rate table found and parsed. "
                "medium: rates found but some ambiguity. "
                "low: partial data or significant assumptions. "
                "failed: could not extract rates."
            ),
        },
        "notes": {
            "type": "string",
            "description": "Extraction notes, assumptions, edge cases, or reasons for failure",
        },
    },
    "required": [
        "rate_structure_type", "fixed_charge_monthly",
        "tier_1_rate", "parse_confidence", "notes",
    ],
}

# Default model — Sonnet is fast/cheap for structured extraction
DEFAULT_MODEL = "claude-sonnet-4-20250514"


@dataclass
class ParseResult:
    """Result of parsing rate text with Claude API."""

    # Extracted rate data
    rate_effective_date: str | None = None
    rate_structure_type: str | None = None
    billing_frequency: str | None = None
    fixed_charge_monthly: float | None = None
    meter_size_inches: float | None = None
    tier_1_limit_ccf: float | None = None
    tier_1_rate: float | None = None
    tier_2_limit_ccf: float | None = None
    tier_2_rate: float | None = None
    tier_3_limit_ccf: float | None = None
    tier_3_rate: float | None = None
    tier_4_limit_ccf: float | None = None
    tier_4_rate: float | None = None

    # Metadata
    parse_confidence: str = "failed"
    parse_notes: str = ""
    parse_model: str = ""
    parsed_at: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    error: str | None = None


def _get_anthropic_client():
    """Create an Anthropic client.

    Returns
    -------
    anthropic.Anthropic
        Configured client instance.

    Raises
    ------
    RuntimeError
        If ANTHROPIC_API_KEY is not set.
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic SDK not installed. Run: pip install anthropic"
        )

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Also check .env file
        from utility_api.config import PROJECT_ROOT
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not found. Set it in environment or .env file."
        )

    return anthropic.Anthropic(api_key=api_key)


def parse_rate_text(
    page_text: str,
    utility_name: str = "",
    state_code: str = "",
    model: str | None = None,
) -> ParseResult:
    """Parse water rate information from scraped page text using Claude API.

    Parameters
    ----------
    page_text : str
        Extracted text from a utility rate page.
    utility_name : str
        Utility name (included in prompt for context).
    state_code : str
        State code (included in prompt for context).
    model : str | None
        Claude model ID. Defaults to Sonnet.

    Returns
    -------
    ParseResult
        Parsed rate structure with confidence and metadata.
    """
    model = model or DEFAULT_MODEL

    if not page_text or len(page_text.strip()) < 50:
        return ParseResult(
            parse_confidence="failed",
            parse_notes="Input text too short or empty",
            parse_model=model,
        )

    # Build the user message via shared builder
    user_message = build_parse_user_message(
        page_text, utility_name=utility_name, state_code=state_code,
    )

    try:
        client = _get_anthropic_client()

        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": "{"},
            ],
        )

        # Extract the text response — prefill with "{" so prepend it back
        response_text = "{" + response.content[0].text

        # Strip markdown code fences if present
        if response_text.startswith("```"):
            response_text = response_text.split("\n", 1)[1].rsplit("```", 1)[0]

        # Parse JSON
        data = json.loads(response_text)

        result = ParseResult(
            rate_effective_date=data.get("rate_effective_date"),
            rate_structure_type=data.get("rate_structure_type"),
            billing_frequency=data.get("billing_frequency"),
            fixed_charge_monthly=data.get("fixed_charge_monthly"),
            meter_size_inches=data.get("meter_size_inches"),
            tier_1_limit_ccf=data.get("tier_1_limit_ccf"),
            tier_1_rate=data.get("tier_1_rate"),
            tier_2_limit_ccf=data.get("tier_2_limit_ccf"),
            tier_2_rate=data.get("tier_2_rate"),
            tier_3_limit_ccf=data.get("tier_3_limit_ccf"),
            tier_3_rate=data.get("tier_3_rate"),
            tier_4_limit_ccf=data.get("tier_4_limit_ccf"),
            tier_4_rate=data.get("tier_4_rate"),
            parse_confidence=data.get("parse_confidence", "low"),
            parse_notes=data.get("notes", ""),
            parse_model=model,
            parsed_at=datetime.now(timezone.utc).isoformat(),
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )

        logger.info(
            f"Parsed {utility_name}: {result.rate_structure_type} "
            f"[{result.parse_confidence}] "
            f"(tokens: {result.input_tokens}+{result.output_tokens})"
        )
        return result

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for {utility_name}: {e}")
        return ParseResult(
            parse_confidence="failed",
            parse_notes=f"Claude response was not valid JSON: {e}",
            parse_model=model,
            parsed_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        logger.error(f"Claude API error for {utility_name}: {e}")
        return ParseResult(
            parse_confidence="failed",
            parse_notes=f"API error: {e}",
            parse_model=model,
            error=str(e),
        )

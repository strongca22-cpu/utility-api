#!/usr/bin/env python3
"""
Water Rate Ingest Orchestrator

Purpose:
    End-to-end pipeline for discovering, scraping, parsing, and storing
    water rate data for community water systems. Orchestrates:
    1. URL discovery (web search for rate pages)
    2. Web scraping (fetch + extract text)
    3. Claude API parsing (structured rate extraction)
    4. Bill calculation (tier structure → dollar amounts)
    5. Database storage (water_rates table)

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - httpx, beautifulsoup4 (scraping)
    - anthropic (Claude API)
    - sqlalchemy (database)

Usage:
    # CLI
    ua-ingest rates --state VA --limit 10
    ua-ingest rates --pwsid VA0071010

    # Python
    from utility_api.ingest.rates import run_rate_ingest
    run_rate_ingest(state_filter=["VA"], limit=10)

Notes:
    - Pipeline is resumable: skips utilities that already have parsed rates
    - Each step rate-limits to avoid being blocked (configurable delays)
    - Results are stored incrementally (one DB write per utility)
    - Failed parses are stored with parse_confidence='failed' for review
    - ANTHROPIC_API_KEY must be set in environment or .env

Data Sources:
    - Input: utility.cws_boundaries + utility.mdwd_financials (target list)
    - Input: Web search results → utility rate page URLs → scraped text
    - Output: utility.water_rates table
"""

import time
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_calculator import calculate_bills_from_parse
from utility_api.ingest.rate_discovery import discover_rate_url
from utility_api.ingest.rate_parser import parse_rate_text
from utility_api.ingest.rate_scraper import scrape_rate_page

# Common fractional meter sizes → decimal inches
_METER_SIZE_MAP = {
    "5/8": 0.625,
    "3/4": 0.75,
    "5/8 x 3/4": 0.625,
    "1": 1.0,
    "1 1/2": 1.5,
    "2": 2.0,
}


def _coerce_meter_size(value) -> float | None:
    """Coerce meter size to float, handling fractional strings like '5/8'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().strip('"').strip("'")
    if s in _METER_SIZE_MAP:
        return _METER_SIZE_MAP[s]
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _get_target_utilities(
    pwsids: list[str] | None = None,
    state_filter: list[str] | None = None,
    limit: int | None = None,
    skip_existing: bool = True,
) -> list[dict]:
    """Get target utilities for rate ingest.

    Parameters
    ----------
    pwsids : list[str] | None
        Specific PWSIDs to target.
    state_filter : list[str] | None
        Filter to these states.
    limit : int | None
        Max number of utilities.
    skip_existing : bool
        If True, skip utilities that already have parsed rates.

    Returns
    -------
    list[dict]
        List of utility dicts with pwsid, pws_name, state_code, county_served.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        if pwsids:
            placeholders = ", ".join(f"'{p}'" for p in pwsids)
            rows = conn.execute(text(f"""
                SELECT c.pwsid, c.pws_name, c.state_code, c.county_served
                FROM {schema}.cws_boundaries c
                WHERE c.pwsid IN ({placeholders})
            """)).fetchall()
        else:
            state_clause = ""
            if state_filter:
                state_list = ", ".join(f"'{s}'" for s in state_filter)
                state_clause = f"AND c.state_code IN ({state_list})"

            rows = conn.execute(text(f"""
                SELECT c.pwsid, c.pws_name, c.state_code, c.county_served
                FROM {schema}.cws_boundaries c
                INNER JOIN {schema}.mdwd_financials m ON m.pwsid = c.pwsid
                WHERE c.pws_name IS NOT NULL
                {state_clause}
                ORDER BY c.state_code, c.pws_name
            """)).fetchall()

        # Get already-parsed PWSIDs
        if skip_existing:
            existing = conn.execute(text(f"""
                SELECT DISTINCT pwsid FROM {schema}.water_rates
                WHERE parse_confidence IN ('high', 'medium')
            """)).fetchall()
            existing_set = {r[0] for r in existing}
        else:
            existing_set = set()

    utilities = [
        {"pwsid": r[0], "pws_name": r[1], "state_code": r[2], "county_served": r[3]}
        for r in rows
        if r[0] not in existing_set
    ]

    if limit:
        utilities = utilities[:limit]

    return utilities


def _store_rate_record(
    pwsid: str,
    utility_name: str,
    state_code: str,
    county: str | None,
    source_url: str,
    raw_text_hash: str,
    parse_result,
    bill_5ccf: float | None,
    bill_10ccf: float | None,
) -> None:
    """Store a parsed rate record in the database.

    Parameters
    ----------
    pwsid : str
        EPA PWSID.
    utility_name : str
        Utility name.
    state_code : str
        State code.
    county : str | None
        County name.
    source_url : str
        URL of scraped rate page.
    raw_text_hash : str
        SHA-256 of scraped text.
    parse_result : ParseResult
        Parsed rate data from Claude API.
    bill_5ccf : float | None
        Calculated bill at 5 CCF.
    bill_10ccf : float | None
        Calculated bill at 10 CCF.
    """
    schema = settings.utility_schema
    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        # Delete existing scraped record for this PWSID + effective date (upsert pattern)
        conn.execute(text(f"""
            DELETE FROM {schema}.water_rates
            WHERE pwsid = :pwsid
            AND source = 'scraped_llm'
            AND (rate_effective_date = :eff_date OR rate_effective_date IS NULL)
        """), {
            "pwsid": pwsid,
            "eff_date": parse_result.rate_effective_date,
        })

        conn.execute(text(f"""
            INSERT INTO {schema}.water_rates (
                pwsid, utility_name, state_code, county,
                rate_effective_date, rate_structure_type, rate_class, billing_frequency,
                fixed_charge_monthly, meter_size_inches,
                tier_1_limit_ccf, tier_1_rate,
                tier_2_limit_ccf, tier_2_rate,
                tier_3_limit_ccf, tier_3_rate,
                tier_4_limit_ccf, tier_4_rate,
                bill_5ccf, bill_10ccf,
                source, source_url, raw_text_hash,
                parse_confidence, parse_model, parse_notes,
                scraped_at, parsed_at
            ) VALUES (
                :pwsid, :name, :state, :county,
                :eff_date, :struct_type, 'residential', :billing_freq,
                :fixed_charge, :meter_size,
                :t1_limit, :t1_rate,
                :t2_limit, :t2_rate,
                :t3_limit, :t3_rate,
                :t4_limit, :t4_rate,
                :bill_5, :bill_10,
                'scraped_llm', :url, :text_hash,
                :confidence, :model, :notes,
                :scraped_at, :parsed_at
            )
        """), {
            "pwsid": pwsid,
            "name": utility_name,
            "state": state_code,
            "county": county,
            "eff_date": parse_result.rate_effective_date,
            "struct_type": parse_result.rate_structure_type,
            "billing_freq": parse_result.billing_frequency,
            "fixed_charge": parse_result.fixed_charge_monthly,
            "meter_size": _coerce_meter_size(parse_result.meter_size_inches),
            "t1_limit": parse_result.tier_1_limit_ccf,
            "t1_rate": parse_result.tier_1_rate,
            "t2_limit": parse_result.tier_2_limit_ccf,
            "t2_rate": parse_result.tier_2_rate,
            "t3_limit": parse_result.tier_3_limit_ccf,
            "t3_rate": parse_result.tier_3_rate,
            "t4_limit": parse_result.tier_4_limit_ccf,
            "t4_rate": parse_result.tier_4_rate,
            "bill_5": bill_5ccf,
            "bill_10": bill_10ccf,
            "url": source_url,
            "text_hash": raw_text_hash,
            "confidence": parse_result.parse_confidence,
            "model": parse_result.parse_model,
            "notes": parse_result.parse_notes,
            "scraped_at": now,
            "parsed_at": parse_result.parsed_at,
        })
        conn.commit()


def _load_curated_urls(url_file: str | None = None) -> dict[str, str]:
    """Load curated rate page URLs from a YAML file.

    Parameters
    ----------
    url_file : str | None
        Path to YAML file mapping pwsid → url.

    Returns
    -------
    dict[str, str]
        Mapping of pwsid → url.
    """
    if url_file is None:
        return {}

    from pathlib import Path

    import yaml

    path = Path(url_file)
    if not path.exists():
        logger.warning(f"URL file not found: {url_file}")
        return {}

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    # Filter to entries with actual URLs (not empty/commented)
    urls = {k: v for k, v in data.items() if v and isinstance(v, str) and v.strip()}
    logger.info(f"Loaded {len(urls)} curated URLs from {url_file}")
    return urls


def run_rate_ingest(
    pwsids: list[str] | None = None,
    state_filter: list[str] | None = None,
    limit: int | None = None,
    search_delay: float = 2.0,
    scrape_delay: float = 1.5,
    dry_run: bool = False,
    skip_existing: bool = True,
    url_file: str | None = None,
    max_cost_usd: float | None = None,
) -> dict:
    """Run the full rate ingest pipeline.

    Steps:
    1. Get target utilities from DB
    2. For each utility: use curated URL or discover URL → scrape → parse → calculate → store

    Parameters
    ----------
    pwsids : list[str] | None
        Specific PWSIDs to target.
    state_filter : list[str] | None
        Filter to these states (e.g., ["VA", "CA"]).
    limit : int | None
        Max number of utilities to process.
    search_delay : float
        Delay between web searches (seconds).
    scrape_delay : float
        Delay between page scrapes (seconds).
    dry_run : bool
        If True, print results but don't write to DB.
    skip_existing : bool
        If True, skip utilities with existing high/medium parses.
    url_file : str | None
        Path to YAML file with curated pwsid → url mappings.
        If provided, uses curated URL instead of web search for matching PWSIDs.
    max_cost_usd : float | None
        Hard cap on estimated API cost in USD. Pipeline stops when exceeded.
        Uses Sonnet pricing: $3/M input + $15/M output tokens.

    Returns
    -------
    dict
        Summary stats: total, discovered, scraped, parsed, failed.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== Water Rate Ingest Starting ===")

    # Load curated URLs if provided
    curated_urls = _load_curated_urls(url_file)

    # Step 1: Get targets
    if curated_urls and not pwsids:
        # If url_file provided without specific pwsids, target those in the file
        pwsids = list(curated_urls.keys())

    utilities = _get_target_utilities(pwsids, state_filter, limit, skip_existing)
    logger.info(f"Target utilities: {len(utilities)}")

    if not utilities:
        logger.info("No utilities to process")
        return {"total": 0, "discovered": 0, "scraped": 0, "parsed_ok": 0, "failed": 0}

    stats = {
        "total": len(utilities),
        "discovered": 0,
        "scraped": 0,
        "parsed_ok": 0,
        "failed": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "estimated_cost_usd": 0.0,
    }

    # Sonnet pricing: $3/M input, $15/M output
    COST_PER_INPUT_TOKEN = 3.0 / 1_000_000
    COST_PER_OUTPUT_TOKEN = 15.0 / 1_000_000

    for i, util in enumerate(utilities):
        pwsid = util["pwsid"]
        name = util["pws_name"]
        state = util["state_code"]
        county = util["county_served"]

        logger.info(f"\n[{i + 1}/{len(utilities)}] {name} ({pwsid}, {state})")

        # Step 2: Discover URL (use curated if available)
        if pwsid in curated_urls:
            rate_url = curated_urls[pwsid]
            logger.info(f"  Using curated URL: {rate_url}")
            # Registry: log curated URL
            from utility_api.ops.registry_writer import log_discovery
            log_discovery(pwsid, rate_url, url_source="curated")
        else:
            discovery = discover_rate_url(pwsid, name, state, county)
            rate_url = discovery.best_url
            # Registry: log discovered URL
            if rate_url:
                from utility_api.ops.registry_writer import log_discovery
                log_discovery(
                    pwsid, rate_url, url_source="searxng",
                    discovery_query=getattr(discovery, "search_query", None),
                )

        if not rate_url:
            logger.warning(f"  No URL found for {name}")
            stats["failed"] += 1
            if not dry_run:
                # Store failed discovery
                from utility_api.ingest.rate_parser import ParseResult
                _store_rate_record(
                    pwsid, name, state, county,
                    source_url="",
                    raw_text_hash="",
                    parse_result=ParseResult(
                        parse_confidence="failed",
                        parse_notes="URL discovery failed: no rate page found",
                        parse_model="",
                    ),
                    bill_5ccf=None,
                    bill_10ccf=None,
                )
            time.sleep(search_delay)
            continue

        stats["discovered"] += 1
        time.sleep(search_delay)

        # Step 3: Scrape
        scrape = scrape_rate_page(rate_url)

        # Registry: log fetch outcome
        from utility_api.ops.registry_writer import log_fetch
        log_fetch(
            pwsid, rate_url,
            http_status=getattr(scrape, "status_code", None),
            content_hash=getattr(scrape, "text_hash", None),
            content_length=getattr(scrape, "char_count", None),
            error=scrape.error if scrape.error else None,
        )

        if scrape.error and not scrape.is_pdf:
            logger.warning(f"  Scrape failed: {scrape.error}")
            stats["failed"] += 1
            if not dry_run:
                from utility_api.ingest.rate_parser import ParseResult
                _store_rate_record(
                    pwsid, name, state, county,
                    source_url=rate_url,
                    raw_text_hash="",
                    parse_result=ParseResult(
                        parse_confidence="failed",
                        parse_notes=f"Scrape failed: {scrape.error}",
                        parse_model="",
                    ),
                    bill_5ccf=None,
                    bill_10ccf=None,
                )
            time.sleep(scrape_delay)
            continue

        stats["scraped"] += 1
        time.sleep(scrape_delay)

        if dry_run:
            logger.info(f"  [DRY RUN] Would parse {len(scrape.text)} chars from {rate_url}")
            continue

        # Step 4: Parse with Claude API
        parse = parse_rate_text(scrape.text, utility_name=name, state_code=state)

        # Track API cost
        stats["total_input_tokens"] += parse.input_tokens
        stats["total_output_tokens"] += parse.output_tokens
        call_cost = (parse.input_tokens * COST_PER_INPUT_TOKEN
                     + parse.output_tokens * COST_PER_OUTPUT_TOKEN)
        stats["estimated_cost_usd"] += call_cost
        logger.info(f"  API cost: ${call_cost:.4f} (cumulative: ${stats['estimated_cost_usd']:.4f})")

        # Registry: log parse outcome
        from utility_api.ops.registry_writer import log_parse
        parse_result = "success" if parse.parse_confidence in ("high", "medium") else "failed"
        log_parse(
            pwsid, rate_url,
            parse_result=parse_result,
            parse_confidence=parse.parse_confidence,
            parse_cost_usd=call_cost,
            parse_model=parse.parse_model,
        )

        if max_cost_usd and stats["estimated_cost_usd"] >= max_cost_usd:
            logger.warning(
                f"  ⚠ Cost cap reached: ${stats['estimated_cost_usd']:.2f} >= ${max_cost_usd:.2f}. "
                f"Stopping pipeline."
            )
            # Still store this last result before stopping
            bill_5, bill_10 = calculate_bills_from_parse(parse)
            _store_rate_record(
                pwsid, name, state, county,
                source_url=rate_url, raw_text_hash=scrape.text_hash,
                parse_result=parse, bill_5ccf=bill_5, bill_10ccf=bill_10,
            )
            if parse.parse_confidence in ("high", "medium"):
                stats["parsed_ok"] += 1
            else:
                stats["failed"] += 1
            break

        # Step 5: Calculate bills
        bill_5, bill_10 = calculate_bills_from_parse(parse)

        # Step 6: Store
        _store_rate_record(
            pwsid, name, state, county,
            source_url=rate_url,
            raw_text_hash=scrape.text_hash,
            parse_result=parse,
            bill_5ccf=bill_5,
            bill_10ccf=bill_10,
        )

        if parse.parse_confidence in ("high", "medium"):
            stats["parsed_ok"] += 1
            logger.info(
                f"  ✓ {parse.rate_structure_type} | "
                f"fixed=${parse.fixed_charge_monthly or 0:.2f} | "
                f"bill@5CCF=${bill_5 or 0:.2f} | bill@10CCF=${bill_10 or 0:.2f} | "
                f"[{parse.parse_confidence}]"
            )
        else:
            stats["failed"] += 1
            logger.warning(f"  ✗ Parse {parse.parse_confidence}: {parse.parse_notes[:80]}")

    # Log pipeline run
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(f"\n=== Rate Ingest Complete ({elapsed:.0f}s) ===")
    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  URLs discovered: {stats['discovered']}")
    logger.info(f"  Pages scraped: {stats['scraped']}")
    logger.info(f"  Parsed OK: {stats['parsed_ok']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  API tokens: {stats['total_input_tokens']}in + {stats['total_output_tokens']}out")
    logger.info(f"  Estimated cost: ${stats['estimated_cost_usd']:.4f}")

    if not dry_run:
        schema = settings.utility_schema
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.pipeline_runs
                    (step_name, started_at, finished_at, row_count, status, notes)
                VALUES (:step, :started, NOW(), :count, 'success', :notes)
            """), {
                "step": "rate-parse",
                "started": started,
                "count": stats["parsed_ok"],
                "notes": (
                    f"discovered={stats['discovered']}, scraped={stats['scraped']}, "
                    f"parsed_ok={stats['parsed_ok']}, failed={stats['failed']}"
                ),
            })
            conn.commit()

    return stats

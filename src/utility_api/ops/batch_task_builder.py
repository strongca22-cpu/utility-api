#!/usr/bin/env python3
"""
Batch Task Builder

Purpose:
    Builds parse task lists from scrape_registry for Anthropic Batch API
    submission. Supports three strategies for how many URLs per PWSID
    are submitted:

    - shotgun:    ALL viable URLs per PWSID (fast, ~18% cost overhead)
    - cascade:    Only the next untried rank for each PWSID (cheapest, multi-day)
    - rank1_only: Only the best-scored URL per PWSID (cheapest single-pass)

    Strategy is configurable via config/agent_config.yaml (batch_api.default_strategy)
    and overridable per-call.

    Used by all batch submission scripts:
    - submit_discovery_batch.py
    - submit_cascade_batch.py
    - resubmit_truncated_batch.py
    - run_scenario_a.py

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - sqlalchemy
    - utility_api.agents.discovery (score_url_relevance)
    - utility_api.utils.content_scoring (compute_content_boost)

Usage:
    from utility_api.ops.batch_task_builder import build_parse_tasks

    # Use config default (currently "shotgun")
    tasks = build_parse_tasks()

    # Override strategy
    tasks = build_parse_tasks(strategy="cascade")
    tasks = build_parse_tasks(strategy="rank1_only", min_pop=3000)

    # Filter to specific PWSIDs
    tasks = build_parse_tasks(pwsids=["TX1010013", "CA3010092"])
"""

from pathlib import Path

import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.agents.discovery import score_url_relevance
from utility_api.config import settings, PROJECT_ROOT
from utility_api.db import engine
from utility_api.utils.content_scoring import compute_content_boost

VALID_STRATEGIES = ("shotgun", "cascade", "rank1_only")


def _load_default_strategy() -> str:
    """Load default batch strategy from config."""
    config_path = PROJECT_ROOT / "config" / "agent_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("batch_api", {}).get("default_strategy", "shotgun")
    return "shotgun"


def build_parse_tasks(
    strategy: str | None = None,
    min_pop: int = 0,
    pwsids: list[str] | None = None,
    min_rank: int = 1,
    max_rank: int = 5,
    exclude_attempted: bool = False,
    min_text_len: int = 200,
) -> list[dict]:
    """Build parse tasks from scrape_registry for batch submission.

    Parameters
    ----------
    strategy : str, optional
        "shotgun", "cascade", or "rank1_only". Defaults to config value.
    min_pop : int
        Minimum population filter.
    pwsids : list[str], optional
        Restrict to specific PWSIDs. If None, all gap PWSIDs.
    min_rank : int
        Minimum discovery_rank to include (default 1). Set to 2 to skip
        rank 1 (e.g., for cascade round 2 after rank 1 failed).
    max_rank : int
        Maximum discovery_rank to include (default 5).
    exclude_attempted : bool
        If True, exclude URLs where last_parse_result is not NULL
        (already attempted via direct API or prior batch).
    min_text_len : int
        Minimum scraped_text length to consider viable.

    Returns
    -------
    list[dict]
        Parse tasks: [{pwsid, raw_text, content_type, source_url, registry_id}]
        Sorted by population descending.
    """
    if strategy is None:
        strategy = _load_default_strategy()

    if strategy not in VALID_STRATEGIES:
        raise ValueError(f"Invalid strategy '{strategy}'. Must be one of: {VALID_STRATEGIES}")

    schema = settings.utility_schema

    # Build filters
    pwsid_filter = ""
    params = {"src": "serper", "llm": "scraped_llm", "min_pop": min_pop,
              "min_rank": min_rank, "max_rank": max_rank, "min_text": min_text_len}

    if pwsids:
        pwsid_filter = "AND sr.pwsid = ANY(:pwsid_list)"
        params["pwsid_list"] = pwsids

    attempted_filter = ""
    if exclude_attempted:
        attempted_filter = "AND sr.last_parse_result IS NULL"

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT sr.id as registry_id, sr.pwsid, sr.scraped_text,
                   sr.content_type, sr.url as source_url,
                   sr.discovery_rank, sr.discovery_score,
                   LENGTH(sr.scraped_text) as text_len,
                   c.population_served, c.state_code, c.pws_name
            FROM {schema}.scrape_registry sr
            JOIN {schema}.cws_boundaries c ON sr.pwsid = c.pwsid
            WHERE sr.url_source = :src
              AND sr.discovery_rank >= :min_rank
              AND sr.discovery_rank <= :max_rank
              AND sr.scraped_text IS NOT NULL
              AND LENGTH(sr.scraped_text) > :min_text
              AND c.population_served >= :min_pop
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = sr.pwsid AND rs.source_key = :llm
              )
              {pwsid_filter}
              {attempted_filter}
            ORDER BY c.population_served DESC, sr.pwsid, sr.discovery_rank
        """), params).fetchall()

    if not rows:
        logger.info(f"No viable URLs found (strategy={strategy}, min_pop={min_pop})")
        return []

    # Group by PWSID
    by_pwsid = {}
    for r in rows:
        by_pwsid.setdefault(r.pwsid, []).append(r)

    # Apply strategy
    parse_tasks = []

    for pwsid, urls in by_pwsid.items():
        # Re-score with content boost
        scored = []
        for u in urls:
            snippet = (u.scraped_text or "")[:200]
            base = score_url_relevance(
                url=u.source_url, title="", snippet=snippet,
                utility_name=u.pws_name or "", state=u.state_code or "",
            )
            boost = compute_content_boost(u.scraped_text or "")
            score = min(base + boost, 100)
            if score >= 30:
                scored.append((score, u))

        scored.sort(key=lambda x: -x[0])

        if not scored:
            continue

        if strategy == "rank1_only":
            selected = [scored[0]]
        elif strategy == "cascade":
            # Take only the highest-scored untried URL
            selected = [scored[0]]
        elif strategy == "shotgun":
            selected = scored
        else:
            selected = [scored[0]]

        for score, u in selected:
            raw_text = u.scraped_text

            # Section extraction for multi-area PDFs
            try:
                from utility_api.ingest.rate_scraper import extract_service_area_section
                section = extract_service_area_section(raw_text, u.pws_name or "")
                if section:
                    raw_text = section
            except Exception:
                pass

            parse_tasks.append({
                "pwsid": pwsid,
                "raw_text": raw_text[:45000],
                "content_type": u.content_type or "html",
                "source_url": u.source_url or "",
                "registry_id": u.registry_id,
            })

    # Stats
    unique_pwsids = len(set(t["pwsid"] for t in parse_tasks))
    logger.info(
        f"Built {len(parse_tasks)} parse tasks for {unique_pwsids} PWSIDs "
        f"(strategy={strategy}, min_rank={min_rank}, max_rank={max_rank})"
    )

    return parse_tasks

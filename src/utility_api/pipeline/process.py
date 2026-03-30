#!/usr/bin/env python3
"""
Cascade Process Pipeline — process_pwsid()

Purpose:
    Orchestrates the full discovery-to-parse cascade for a single PWSID:

    1. Gather all URLs for the PWSID from scrape_registry (Serper top 3)
    2. Deep crawl each starting URL proactively (15 fetches each, 45 total)
    3. Re-score all URLs (originals + deep crawl children) with scoring v2
    4. Parse rank #1 → if fail → rank #2 → if fail → rank #3 (max 3 attempts)
    5. Log full cascade diagnostics to discovery_diagnostics table

    This is the shared processing function called by:
    - serper_bulk_discovery.py (--process flag)
    - Daily cron processing
    - Metro scan pipeline
    - Ad-hoc reprocessing of failed PWSIDs

    Does NOT replace DiscoveryAgent (search+score) or ScrapeAgent/ParseAgent.
    Orchestrates them.

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Dependencies:
    - utility_api.agents.scrape (ScrapeAgent — deep crawl)
    - utility_api.agents.parse (ParseAgent — LLM parse)
    - utility_api.agents.discovery (score_url_relevance — re-scoring)
    - utility_api.agents.best_estimate (BestEstimateAgent — post-parse)
    - PostgreSQL with utility schema (migration 020)

Usage:
    from utility_api.pipeline.process import process_pwsid

    # Single PWSID
    result = process_pwsid('CO0116001', skip_best_estimate=True)

    # Batch (best_estimate batched per state at end)
    states = set()
    for pwsid in pwsids:
        result = process_pwsid(pwsid, skip_best_estimate=True)
        if result['parse_success']:
            states.add(pwsid[:2])
    for st in states:
        BestEstimateAgent().run(state=st)

Notes:
    - Deep crawl is PROACTIVE on all starting URLs, not just thin content
    - Parse cascade stops on first success (max 3 attempts)
    - All results persisted to discovery_diagnostics table
    - Every URL, every crawl, every parse attempt is tracked
"""

from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


def process_pwsid(
    pwsid: str,
    skip_best_estimate: bool = False,
    max_starting_urls: int = 3,
    max_parse_attempts: int = 3,
    score_threshold: int = 30,
) -> dict:
    """Run the full cascade pipeline for one PWSID.

    Parameters
    ----------
    pwsid : str
        EPA PWSID to process.
    skip_best_estimate : bool
        If True, caller handles BestEstimate batching.
    max_starting_urls : int
        Max Serper URLs to deep crawl (default 3).
    max_parse_attempts : int
        Max parse attempts before giving up (default 3).
    score_threshold : int
        Minimum re-score to attempt parsing (default 30).

    Returns
    -------
    dict
        parse_success, winning_url, winning_rank, winning_source,
        parse_attempts, total_candidates, diagnostics_id.
    """
    schema = settings.utility_schema

    # ---------------------------------------------------------------
    # Step 1: Gather starting URLs from scrape_registry
    # ---------------------------------------------------------------
    starting_urls = _get_starting_urls(pwsid, schema, max_starting_urls)

    if not starting_urls:
        logger.info(f"process_pwsid({pwsid}): no starting URLs in registry")
        return {
            "parse_success": False,
            "winning_url": None,
            "winning_rank": None,
            "winning_source": None,
            "parse_attempts": 0,
            "total_candidates": 0,
            "diagnostics_id": None,
        }

    logger.info(
        f"process_pwsid({pwsid}): {len(starting_urls)} starting URLs"
    )

    # ---------------------------------------------------------------
    # Step 2: Deep crawl each starting URL proactively
    # ---------------------------------------------------------------
    all_candidates = []
    total_fetches = 0

    for start in starting_urls:
        crawl_results = _deep_crawl_url(
            pwsid=pwsid,
            registry_id=start["id"],
            url=start["url"],
        )
        total_fetches += crawl_results["fetch_count"]

        # The starting URL itself is always a candidate
        all_candidates.append({
            "registry_id": start["id"],
            "url": start["url"],
            "url_source": start["url_source"],
            "discovery_rank": start["discovery_rank"],
            "text_len": crawl_results["final_text_len"],
            "scraped_text": crawl_results["final_text"],
            "content_type": crawl_results["content_type"],
            "is_deep_crawl_result": crawl_results["deep_crawled"],
        })

        # Add deep crawl children as separate candidates
        for child in crawl_results["children"]:
            all_candidates.append({
                "registry_id": child["registry_id"],
                "url": child["url"],
                "url_source": "deep_crawl",
                "discovery_rank": start["discovery_rank"],  # parent's rank
                "text_len": child["text_len"],
                "scraped_text": child["scraped_text"],
                "content_type": child["content_type"],
                "is_deep_crawl_result": True,
            })

    deep_crawl_children = sum(
        1 for c in all_candidates if c["url_source"] == "deep_crawl"
    )

    logger.info(
        f"  {len(starting_urls)} starting + {deep_crawl_children} deep crawl "
        f"= {len(all_candidates)} total candidates, {total_fetches} fetches"
    )

    # ---------------------------------------------------------------
    # Step 3: Re-score all candidates with scoring v2
    # ---------------------------------------------------------------
    meta = _get_system_metadata(pwsid, schema)
    utility_name = meta.get("pws_name", "")
    city = meta.get("city", "")
    state = meta.get("state_code", pwsid[:2])

    from utility_api.agents.discovery import score_url_relevance

    for cand in all_candidates:
        # For re-scoring, we use URL + whatever title/snippet we can construct
        # from the scraped text (first 200 chars as proxy snippet)
        snippet_proxy = (cand["scraped_text"] or "")[:200]
        cand["rescore"] = score_url_relevance(
            url=cand["url"],
            title="",  # no title stored post-scrape
            snippet=snippet_proxy,
            utility_name=utility_name,
            city=city,
            state=state,
        )

    # Sort by re-score descending
    all_candidates.sort(key=lambda c: c["rescore"], reverse=True)

    above_threshold = [c for c in all_candidates if c["rescore"] >= score_threshold]

    logger.info(
        f"  Re-scored: {len(above_threshold)}/{len(all_candidates)} "
        f"above threshold ({score_threshold})"
    )
    for i, c in enumerate(all_candidates[:5]):
        logger.info(
            f"    #{i+1} [{c['rescore']:3d}] {c['url_source']:12s} "
            f"rank={c['discovery_rank']} {c['text_len']:>6,} chars "
            f"{c['url'][:70]}"
        )

    # ---------------------------------------------------------------
    # Step 4: Cascade parse — try top candidates until one succeeds
    # ---------------------------------------------------------------
    from utility_api.agents.parse import ParseAgent

    parse_agent = ParseAgent()
    parse_attempts = 0
    parse_success = False
    winning_rank = None
    winning_url = None
    winning_source = None
    winning_discovery_rank = None
    winning_score = None
    total_parse_cost = 0.0

    # Track parse result per candidate for diagnostics
    for cand in all_candidates:
        cand["parsed"] = False
        cand["parse_result"] = None

    parseable = [c for c in above_threshold if c["text_len"] and c["text_len"] > 100]

    for rank_idx, cand in enumerate(parseable):
        if parse_attempts >= max_parse_attempts:
            break
        if parse_success:
            break

        parse_attempts += 1
        logger.info(
            f"  Parse attempt {parse_attempts}/{max_parse_attempts}: "
            f"[{cand['rescore']}] {cand['url'][:70]}"
        )

        try:
            result = parse_agent.run(
                pwsid=pwsid,
                raw_text=cand["scraped_text"],
                content_type=cand.get("content_type", "html"),
                source_url=cand["url"],
                registry_id=cand["registry_id"],
                skip_best_estimate=True,  # always batch
            )

            cand["parsed"] = True
            cost = result.get("cost_usd", 0.0) if result else 0.0
            total_parse_cost += cost

            if result and result.get("success"):
                parse_success = True
                winning_rank = rank_idx + 1
                winning_url = cand["url"]
                winning_source = cand["url_source"]
                winning_discovery_rank = cand["discovery_rank"]
                winning_score = cand["rescore"]
                cand["parse_result"] = "success"
                logger.info(
                    f"    SUCCESS at rank #{winning_rank} "
                    f"(source={winning_source}, "
                    f"discovery_rank={winning_discovery_rank})"
                )
            else:
                cand["parse_result"] = "failed"
                logger.info(f"    Failed — trying next candidate")

        except Exception as e:
            cand["parsed"] = True
            cand["parse_result"] = "error"
            logger.warning(f"    Parse error: {e}")

    if not parse_success:
        logger.info(
            f"  All {parse_attempts} parse attempts failed for {pwsid}"
        )

    # ---------------------------------------------------------------
    # Step 5: BestEstimate (unless caller batches)
    # ---------------------------------------------------------------
    if parse_success and not skip_best_estimate:
        try:
            from utility_api.agents.best_estimate import BestEstimateAgent
            BestEstimateAgent().run(state=state)
        except Exception as e:
            logger.debug(f"  BestEstimate update skipped: {e}")

    # ---------------------------------------------------------------
    # Step 6: Persist diagnostics
    # ---------------------------------------------------------------
    candidate_details = [
        {
            "url": c["url"],
            "source": c["url_source"],
            "discovery_rank": c["discovery_rank"],
            "rescore": c["rescore"],
            "text_len": c["text_len"],
            "parsed": c["parsed"],
            "parse_result": c["parse_result"],
            "content_type": c.get("content_type"),
        }
        for c in all_candidates
    ]

    diagnostics_id = _save_diagnostics(
        pwsid=pwsid,
        schema=schema,
        starting_urls=len(starting_urls),
        deep_crawl_children=deep_crawl_children,
        total_candidates=len(all_candidates),
        candidates_above_threshold=len(above_threshold),
        parse_attempts=parse_attempts,
        parse_success=parse_success,
        winning_rank=winning_rank,
        winning_url=winning_url,
        winning_source=winning_source,
        winning_discovery_rank=winning_discovery_rank,
        winning_score=winning_score,
        candidate_details=candidate_details,
        total_parse_cost_usd=total_parse_cost,
        total_fetches=total_fetches,
    )

    return {
        "parse_success": parse_success,
        "winning_url": winning_url,
        "winning_rank": winning_rank,
        "winning_source": winning_source,
        "winning_discovery_rank": winning_discovery_rank,
        "parse_attempts": parse_attempts,
        "total_candidates": len(all_candidates),
        "deep_crawl_children": deep_crawl_children,
        "total_fetches": total_fetches,
        "diagnostics_id": diagnostics_id,
    }


# ==================================================================
# Internal helpers
# ==================================================================


def _get_starting_urls(pwsid: str, schema: str, max_urls: int) -> list[dict]:
    """Get Serper URLs for this PWSID, ordered by discovery_rank."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, url, url_source, discovery_rank, discovery_score,
                   last_content_length, scraped_text, content_type
            FROM {schema}.scrape_registry
            WHERE pwsid = :pwsid
              AND url_source = 'serper'
            ORDER BY COALESCE(discovery_rank, 999), discovery_score DESC NULLS LAST
            LIMIT :max_urls
        """), {"pwsid": pwsid, "max_urls": max_urls}).fetchall()

    return [
        {
            "id": r.id,
            "url": r.url,
            "url_source": r.url_source,
            "discovery_rank": r.discovery_rank,
            "discovery_score": r.discovery_score,
            "text_len": r.last_content_length,
            "scraped_text": r.scraped_text,
            "content_type": r.content_type,
        }
        for r in rows
    ]


def _deep_crawl_url(
    pwsid: str,
    registry_id: int,
    url: str,
) -> dict:
    """Deep crawl a single URL proactively (not just on thin content).

    Uses ScrapeAgent.run() with the specific registry_id, which triggers
    the built-in multi-level deep crawl. The key change from the default
    pipeline: we call this for EVERY starting URL, not just ones that
    already have status='pending'.

    Returns
    -------
    dict
        final_text, final_text_len, deep_crawled, content_type,
        fetch_count, children (list of newly discovered URLs).
    """
    schema = settings.utility_schema

    # Snapshot existing deep_crawl URLs for this PWSID before crawling
    with engine.connect() as conn:
        existing_deep = set(
            r[0] for r in conn.execute(text(f"""
                SELECT url FROM {schema}.scrape_registry
                WHERE pwsid = :pwsid AND url_source = 'deep_crawl'
            """), {"pwsid": pwsid}).fetchall()
        )

    # Ensure this registry entry is in a fetchable state so ScrapeAgent
    # will pick it up. If it's already 'active' with scraped_text, the
    # ScrapeAgent's normal flow won't re-fetch — we need to force it.
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT status, scraped_text, last_content_length
            FROM {schema}.scrape_registry WHERE id = :id
        """), {"id": registry_id}).fetchone()

    needs_fetch = (
        row is None
        or row.status in ("pending", "pending_retry")
        or row.scraped_text is None
        or (row.last_content_length or 0) < 100
    )

    if needs_fetch:
        # Let ScrapeAgent do the fetch + deep crawl in one pass
        from utility_api.agents.scrape import ScrapeAgent
        scrape = ScrapeAgent()
        result = scrape.run(registry_id=registry_id, max_depth=3)
        raw_texts = result.get("raw_texts", [])

        if raw_texts:
            entry = raw_texts[0]
            final_text = entry.get("text", "")
            deep_crawled = entry.get("deep_crawled", False)
            content_type = entry.get("content_type", "html")
            # Estimate fetch count: 1 initial + deep crawl fetches
            fetch_count = 1 + (3 if deep_crawled else 0)
        else:
            final_text = ""
            deep_crawled = False
            content_type = "html"
            fetch_count = 1
    else:
        # Already fetched — run deep crawl proactively if text is thin
        final_text = row.scraped_text or ""
        content_type = "html"
        deep_crawled = False
        fetch_count = 0

        from utility_api.agents.scrape import ScrapeAgent
        scrape = ScrapeAgent()

        # Always attempt deep crawl for proactive discovery, even if
        # content isn't thin. But use thin-content check to decide depth.
        if len(final_text) > 100:
            from utility_api.ingest.rate_scraper import scrape_rate_page

            # Re-fetch to get raw HTML for link extraction
            try:
                fresh = scrape_rate_page(url)
                fetch_count += 1
                raw_html = getattr(fresh, "raw_html", None) or (fresh.text or "")
                is_pdf = getattr(fresh, "is_pdf", False)

                if not is_pdf:
                    # Crawl up to 3 levels deep proactively
                    current_url = url
                    current_text = fresh.text or final_text
                    current_html = raw_html

                    for depth in range(1, 4):
                        if fetch_count >= 15:
                            break

                        deeper = scrape._follow_best_links(
                            base_url=current_url,
                            page_html=current_html,
                            pwsid=pwsid,
                            max_links=3,
                            level=depth,
                            fetch_count=fetch_count,
                        )

                        if not deeper:
                            break

                        fetch_count += deeper.get("_fetches", 1)
                        current_url = deeper["url"]
                        current_text = deeper["text"]
                        current_html = deeper.get("raw_html") or deeper["text"]
                        deep_crawled = True

                        if deeper.get("is_pdf", False):
                            break

                        if not scrape._is_thin_content(current_text):
                            break

                    # Update the registry entry with the best text found
                    if deep_crawled and current_text and len(current_text) > len(final_text):
                        final_text = current_text
                        with engine.connect() as conn:
                            conn.execute(text(f"""
                                UPDATE {schema}.scrape_registry SET
                                    scraped_text = :text,
                                    last_content_length = :length,
                                    updated_at = NOW()
                                WHERE id = :id
                            """), {
                                "text": final_text,
                                "length": len(final_text),
                                "id": registry_id,
                            })
                            conn.commit()

            except Exception as e:
                logger.debug(f"  Proactive deep crawl failed for {url[:60]}: {e}")

    # Find newly discovered deep_crawl children
    children = []
    existing_list = list(existing_deep) if existing_deep else []
    with engine.connect() as conn:
        new_deep = conn.execute(text(f"""
            SELECT id, url, last_content_length, scraped_text, content_type
            FROM {schema}.scrape_registry
            WHERE pwsid = :pwsid
              AND url_source = 'deep_crawl'
              AND url != ALL(:existing)
        """), {"pwsid": pwsid, "existing": existing_list}).fetchall()

    for r in new_deep:
        children.append({
            "registry_id": r.id,
            "url": r.url,
            "text_len": r.last_content_length or 0,
            "scraped_text": r.scraped_text,
            "content_type": r.content_type or "html",
        })

    return {
        "final_text": final_text,
        "final_text_len": len(final_text) if final_text else 0,
        "deep_crawled": deep_crawled,
        "content_type": content_type,
        "fetch_count": fetch_count,
        "children": children,
    }


def _get_system_metadata(pwsid: str, schema: str) -> dict:
    """Fetch SDWIS metadata for scoring context."""
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT s.pws_name, s.state_code, s.city,
                   c.county_served, s.owner_type_code
            FROM {schema}.sdwis_systems s
            LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = s.pwsid
            WHERE s.pwsid = :pwsid
        """), {"pwsid": pwsid}).fetchone()

    if not row:
        return {"pws_name": "", "state_code": pwsid[:2], "city": ""}

    return {
        "pws_name": row.pws_name or "",
        "state_code": row.state_code or pwsid[:2],
        "city": row.city or "",
        "county": row.county_served,
        "owner_type": row.owner_type_code,
    }


def _save_diagnostics(
    pwsid: str,
    schema: str,
    **kwargs,
) -> int | None:
    """Persist cascade diagnostics to discovery_diagnostics table."""
    import json

    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                INSERT INTO {schema}.discovery_diagnostics
                    (pwsid, run_at, starting_urls, deep_crawl_children,
                     total_candidates, candidates_above_threshold,
                     parse_attempts, parse_success, winning_rank,
                     winning_url, winning_source, winning_discovery_rank,
                     winning_score, candidate_details,
                     total_parse_cost_usd, total_fetches)
                VALUES
                    (:pwsid, NOW(), :starting_urls, :deep_crawl_children,
                     :total_candidates, :candidates_above_threshold,
                     :parse_attempts, :parse_success, :winning_rank,
                     :winning_url, :winning_source, :winning_discovery_rank,
                     :winning_score, CAST(:candidate_details AS jsonb),
                     :total_parse_cost_usd, :total_fetches)
                RETURNING id
            """), {
                "pwsid": pwsid,
                "starting_urls": kwargs["starting_urls"],
                "deep_crawl_children": kwargs["deep_crawl_children"],
                "total_candidates": kwargs["total_candidates"],
                "candidates_above_threshold": kwargs["candidates_above_threshold"],
                "parse_attempts": kwargs["parse_attempts"],
                "parse_success": kwargs["parse_success"],
                "winning_rank": kwargs.get("winning_rank"),
                "winning_url": kwargs.get("winning_url"),
                "winning_source": kwargs.get("winning_source"),
                "winning_discovery_rank": kwargs.get("winning_discovery_rank"),
                "winning_score": kwargs.get("winning_score"),
                "candidate_details": json.dumps(kwargs["candidate_details"]),
                "total_parse_cost_usd": kwargs.get("total_parse_cost_usd", 0.0),
                "total_fetches": kwargs.get("total_fetches", 0),
            })
            row = result.fetchone()
            conn.commit()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"  Failed to save diagnostics for {pwsid}: {e}")
        return None

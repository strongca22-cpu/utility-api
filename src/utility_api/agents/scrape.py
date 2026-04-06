#!/usr/bin/env python3
"""
Scrape Agent

Purpose:
    Fetches content from URLs in the scrape_registry. Wraps existing
    rate_scraper.py functionality with registry coordination — reads
    pending URLs, fetches content, updates registry with outcomes.

    Sprint 16: Deep crawl capability — when initial page is thin (landing
    page without rate data), extracts links, scores for rate-relevance,
    and follows top candidates on the same domain.

    Sprint 17: Improved thin-content detection — checks for actual rate
    numbers ($/unit patterns), not just keywords. Corporate landing pages
    that discuss rates but link to tariff PDFs are now correctly classified
    as thin. Deep crawl now uses raw HTML for link extraction.

    Sprint 17b: Multi-level deep crawl — configurable depth (default 3).
    Level 1 uses broad navigation scoring (water/utility/departments).
    Level 2+ uses rate-focused scoring. Replaces single-level crawl to
    handle government homepages where rates are 2-4 clicks deep.

    This agent does NOT use an LLM. No `anthropic` import.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-30 (Sprint 24b: Playwright escalation for thin high-confidence pages)

Dependencies:
    - sqlalchemy
    - requests, playwright, pymupdf (via rate_scraper.py)
    - beautifulsoup4 (for deep crawl link extraction)

Usage:
    from utility_api.agents.scrape import ScrapeAgent
    result = ScrapeAgent().run(pwsid='VA4760100')
    result = ScrapeAgent().run(pwsid='VA4760100', max_depth=5)  # deeper diagnostic

Notes:
    - Reads from scrape_registry (Sprint 13 change — Sprint 12 only wrote)
    - Returns raw text in memory for ParseAgent to consume
    - Retry logic: 403 → exponential backoff, 404 → dead, 5xx → 6h retry
    - Reuses existing rate_scraper.py functions — does not rewrite them
    - Deep crawl: multi-level, follows 1-3 same-domain links per level
    - Max depth configurable via config/agent_config.yaml or run() kwarg
    - Sprint 23: persists scraped_text to scrape_registry for decoupled parse
"""

from datetime import datetime, timedelta, timezone

import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine

# Hard cap on total HTTP requests per utility — prevents runaway crawling
MAX_FETCHES_PER_UTILITY = 15


def _load_scrape_config() -> dict:
    """Load scrape section from config/agent_config.yaml."""
    config_path = PROJECT_ROOT / "config" / "agent_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("scrape", {})
    return {}


class ScrapeAgent(BaseAgent):
    """Fetches URLs from scrape_registry and returns raw text."""

    agent_name = "scrape"

    def run(
        self,
        registry_id: int | None = None,
        pwsid: str | None = None,
        max_depth: int | None = None,
        **kwargs,
    ) -> dict:
        """Fetch content from URLs in scrape_registry.

        Parameters
        ----------
        registry_id : int, optional
            Fetch a specific registry entry by ID.
        pwsid : str, optional
            Fetch all pending URLs for this PWSID.
        max_depth : int, optional
            Max deep crawl depth. Default from config (3).

        Returns
        -------
        dict
            urls_fetched, succeeded, failed, raw_texts (list of dicts).
        """
        from utility_api.ingest.rate_scraper import scrape_rate_page

        schema = settings.utility_schema

        # Load config-driven defaults
        scrape_config = _load_scrape_config()
        if max_depth is None:
            max_depth = scrape_config.get("deep_crawl_max_depth", 3)

        # Get URLs to fetch
        with engine.connect() as conn:
            if registry_id:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash,
                           retry_count, url_source
                    FROM {schema}.scrape_registry
                    WHERE id = :id
                """), {"id": registry_id}).fetchall()
            elif pwsid:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash,
                           retry_count, url_source
                    FROM {schema}.scrape_registry
                    WHERE pwsid = :pwsid
                      AND status IN ('pending', 'pending_retry')
                    ORDER BY created_at ASC
                """), {"pwsid": pwsid}).fetchall()
            else:
                logger.warning("ScrapeAgent: no registry_id or pwsid specified")
                return {"urls_fetched": 0, "succeeded": 0, "failed": 0, "raw_texts": []}

        if not rows:
            logger.info("ScrapeAgent: no pending URLs to fetch")
            return {"urls_fetched": 0, "succeeded": 0, "failed": 0, "raw_texts": []}

        logger.info(f"ScrapeAgent: {len(rows)} URLs to fetch (max_depth={max_depth})")

        succeeded = 0
        failed = 0
        raw_texts = []

        for row in rows:
            url = row.url
            logger.info(f"  Fetching: {url[:80]}")

            try:
                scrape_result = scrape_rate_page(url)
            except Exception as e:
                logger.warning(f"  Scrape exception: {e}")
                self._update_registry_failure(row.id, row.retry_count, str(e))
                failed += 1
                continue

            if scrape_result.error and not scrape_result.text:
                logger.warning(f"  Scrape failed: {scrape_result.error}")
                http_status = getattr(scrape_result, "status_code", None)
                self._update_registry_failure(
                    row.id, row.retry_count, scrape_result.error, http_status
                )
                failed += 1
                continue

            # Success — update registry
            content_hash = getattr(scrape_result, "text_hash", None)
            content_changed = (
                content_hash != row.last_content_hash
                if row.last_content_hash and content_hash
                else True
            )
            char_count = len(scrape_result.text) if scrape_result.text else 0

            # Only persist text if it's valid UTF-8 (skip binary docx/xlsx/zip)
            persistable_text = scrape_result.text
            if persistable_text and "\x00" in persistable_text:
                logger.debug(f"  Skipping text persistence (binary content)")
                persistable_text = None

            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_fetch_at = :now,
                        last_http_status = :status,
                        last_content_hash = :hash,
                        last_content_length = :length,
                        scraped_text = :scraped_text,
                        status = 'active',
                        notes = NULL,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": datetime.now(timezone.utc),
                    "status": getattr(scrape_result, "status_code", 200),
                    "hash": content_hash,
                    "length": char_count,
                    "scraped_text": persistable_text,
                    "id": row.id,
                })
                conn.commit()

            # --- Playwright escalation for thin high-confidence pages ---
            # If httpx returned HTTP 200 but thin content (<2000 chars) on a
            # high-confidence URL (Serper, curated, metro_research), retry with
            # Playwright in case JS rendering was needed. If still thin after
            # Playwright, extract rate-relevant links and follow the best one.
            http_status = getattr(scrape_result, "status_code", 200)
            is_pdf = getattr(scrape_result, "is_pdf", False)
            url_source = getattr(row, "url_source", None)
            escalation_note = None

            if (
                not is_pdf
                and http_status == 200
                and char_count < 2000
                and char_count > 0
                and self._is_high_confidence_url(url_source)
            ):
                from utility_api.ingest.rate_scraper import _scrape_with_playwright

                logger.info(
                    f"  Thin content ({char_count} chars) on high-confidence "
                    f"URL ({url_source}) — retrying with Playwright"
                )

                try:
                    pw_result = _scrape_with_playwright(url)
                    pw_text = pw_result.text or ""
                    pw_html = getattr(pw_result, "raw_html", None) or pw_text

                    if len(pw_text) > char_count * 1.5 and len(pw_text) > 500:
                        # Playwright recovered JS content
                        logger.info(
                            f"  Playwright recovered {len(pw_text):,} chars "
                            f"(was {char_count})"
                        )
                        scrape_result = pw_result
                        char_count = len(pw_text)
                        escalation_note = "playwright_reason=thin_js_recovered"

                        # Persist the better content
                        with engine.connect() as conn:
                            conn.execute(text(f"""
                                UPDATE {schema}.scrape_registry SET
                                    scraped_text = :text,
                                    last_content_length = :length,
                                    notes = COALESCE(notes, '') || ' ' || :note,
                                    updated_at = :now
                                WHERE id = :id
                            """), {
                                "text": pw_text,
                                "length": char_count,
                                "note": escalation_note,
                                "now": datetime.now(timezone.utc),
                                "id": row.id,
                            })
                            conn.commit()
                    else:
                        # Still thin after Playwright — try navigation links
                        logger.info(
                            f"  Still thin after Playwright "
                            f"({len(pw_text)} chars) — checking for nav links"
                        )
                        escalation_note = "playwright_reason=thin_still_thin"

                        # Use the better HTML source for link extraction
                        nav_html = pw_html if len(pw_html) > len(
                            getattr(scrape_result, "raw_html", "") or ""
                        ) else (getattr(scrape_result, "raw_html", None) or "")

                        rate_links = self._extract_rate_links(nav_html, url)
                        if rate_links:
                            logger.info(
                                f"  Found {len(rate_links)} rate-relevant "
                                f"links — following best"
                            )
                            best_link = rate_links[0]
                            logger.info(
                                f"    -> {best_link['text'][:40]} "
                                f"({best_link['url'][:60]})"
                            )

                            from utility_api.ingest.rate_scraper import scrape_rate_page as _fetch
                            try:
                                nav_result = _fetch(best_link["url"])
                                nav_text = nav_result.text or ""
                                if len(nav_text) > 500:
                                    logger.info(
                                        f"  Nav crawl recovered "
                                        f"{len(nav_text):,} chars"
                                    )
                                    scrape_result = nav_result
                                    char_count = len(nav_text)
                                    escalation_note = (
                                        f"nav_crawl={best_link['url'][:80]} "
                                        f"nav_crawl_success=true"
                                    )

                                    # Persist nav-crawled content
                                    with engine.connect() as conn:
                                        conn.execute(text(f"""
                                            UPDATE {schema}.scrape_registry SET
                                                scraped_text = :text,
                                                last_content_length = :length,
                                                notes = COALESCE(notes, '') || ' ' || :note,
                                                updated_at = :now
                                            WHERE id = :id
                                        """), {
                                            "text": nav_text,
                                            "length": char_count,
                                            "note": escalation_note,
                                            "now": datetime.now(timezone.utc),
                                            "id": row.id,
                                        })
                                        conn.commit()

                                    # Register the child URL
                                    self._register_nav_crawl_url(
                                        pwsid=row.pwsid,
                                        url=best_link["url"],
                                        parent_registry_id=row.id,
                                    )
                                else:
                                    logger.debug(
                                        f"    Nav link also thin "
                                        f"({len(nav_text)} chars)"
                                    )
                                    escalation_note = (
                                        f"nav_crawl={best_link['url'][:80]} "
                                        f"nav_crawl_success=false"
                                    )
                            except Exception as e:
                                logger.debug(f"    Nav crawl fetch error: {e}")
                        else:
                            logger.debug("  No rate-relevant links found on thin page")

                except Exception as e:
                    logger.debug(f"  Playwright escalation error: {e}")
                    escalation_note = f"playwright_reason=error:{str(e)[:50]}"

                # Persist escalation note if not already done
                if escalation_note and "nav_crawl_success" not in (escalation_note or "") and "recovered" not in (escalation_note or ""):
                    with engine.connect() as conn:
                        conn.execute(text(f"""
                            UPDATE {schema}.scrape_registry SET
                                notes = COALESCE(notes, '') || ' ' || :note,
                                updated_at = :now
                            WHERE id = :id
                        """), {
                            "note": escalation_note,
                            "now": datetime.now(timezone.utc),
                            "id": row.id,
                        })
                        conn.commit()

            # --- Multi-level deep crawl ---
            # Start with the initial page. If thin, crawl deeper up to max_depth.
            current_url = url
            current_text = scrape_result.text
            current_html = getattr(scrape_result, "raw_html", None) or scrape_result.text
            current_is_pdf = getattr(scrape_result, "is_pdf", False)
            deep_crawled = False
            fetch_count = 1  # count initial fetch

            if not current_is_pdf and char_count > 100:
                for depth in range(1, max_depth + 1):
                    if fetch_count >= MAX_FETCHES_PER_UTILITY:
                        logger.warning(f"  Depth {depth}: hit fetch limit ({MAX_FETCHES_PER_UTILITY}) — stopping")
                        break

                    if not self._is_thin_content(current_text):
                        logger.info(f"  Depth {depth - 1}: found rate content ({len(current_text):,} chars) — stopping")
                        break

                    logger.info(f"  Depth {depth}: thin content ({len(current_text):,} chars) — crawling deeper")

                    deeper = self._follow_best_links(
                        base_url=current_url,
                        page_html=current_html,
                        pwsid=row.pwsid,
                        max_links=3,
                        level=depth,
                        fetch_count=fetch_count,
                    )

                    if not deeper:
                        logger.info(f"  Depth {depth}: no followable links found — stopping")
                        break

                    fetch_count += deeper.get("_fetches", 1)
                    current_url = deeper["url"]
                    current_text = deeper["text"]
                    current_html = deeper.get("raw_html") or deeper["text"]
                    current_is_pdf = deeper.get("is_pdf", False)
                    deep_crawled = True

                    logger.info(
                        f"  Depth {depth}: followed → {current_url[:70]} "
                        f"({len(current_text):,} chars)"
                    )

                    # If we found a PDF, stop — PDFs don't have followable links
                    if current_is_pdf:
                        logger.info(f"  Depth {depth}: found PDF — stopping")
                        break

                # Log final state
                final_thin = self._is_thin_content(current_text)
                logger.info(
                    f"  Final: url={current_url[:70]}, "
                    f"chars={len(current_text):,}, "
                    f"thin={final_thin}, fetches={fetch_count}"
                )

            char_count = len(current_text) if current_text else 0

            # Sprint 23: persist final text (may be from deep crawl) back
            # to the original registry entry so ParseAgent can read it later
            if deep_crawled and current_text:
                with engine.connect() as conn:
                    conn.execute(text(f"""
                        UPDATE {schema}.scrape_registry SET
                            scraped_text = :scraped_text,
                            last_content_length = :length,
                            updated_at = :now
                        WHERE id = :id
                    """), {
                        "scraped_text": current_text,
                        "length": char_count,
                        "now": datetime.now(timezone.utc),
                        "id": row.id,
                    })
                    conn.commit()

            # Mark exhausted URLs as dead after 2 failed attempts.
            # Prevents infinite retry loops on 0-char PDFs, broken sites, etc.
            if char_count < 100:
                current_retry = getattr(row, "retry_count", 0) or 0
                new_retry = current_retry + 1
                if new_retry >= 2:
                    with engine.connect() as conn:
                        conn.execute(text(f"""
                            UPDATE {schema}.scrape_registry SET
                                status = 'dead',
                                retry_count = :retry,
                                notes = COALESCE(notes, '') || ' exhausted_after_' || :retry || '_attempts',
                                updated_at = :now
                            WHERE id = :id
                        """), {
                            "retry": new_retry,
                            "now": datetime.now(timezone.utc),
                            "id": row.id,
                        })
                        conn.commit()
                    logger.info(f"  Marked dead after {new_retry} attempts ({char_count} chars)")
                else:
                    with engine.connect() as conn:
                        conn.execute(text(f"""
                            UPDATE {schema}.scrape_registry SET
                                retry_count = :retry,
                                updated_at = :now
                            WHERE id = :id
                        """), {
                            "retry": new_retry,
                            "now": datetime.now(timezone.utc),
                            "id": row.id,
                        })
                        conn.commit()

            raw_texts.append({
                "registry_id": row.id,
                "pwsid": row.pwsid,
                "url": current_url,
                "text": current_text,
                "content_type": "pdf" if current_is_pdf else "html",
                "content_changed": content_changed,
                "char_count": char_count,
                "deep_crawled": deep_crawled,
            })
            succeeded += 1
            logger.info(f"  ✓ {char_count:,} chars, changed={content_changed}"
                        f"{' (deep crawl)' if deep_crawled else ''}")

        self.log_run(
            status="success" if succeeded > 0 else "failed",
            rows_affected=succeeded,
            notes=f"Fetched {succeeded}/{len(rows)}, failed {failed}",
        )

        return {
            "urls_fetched": len(rows),
            "succeeded": succeeded,
            "failed": failed,
            "raw_texts": raw_texts,
        }

    def fetch_single_url(self, url: str) -> dict | None:
        """Fetch a single URL and return its text content.

        Lightweight fetch for backfill/re-fetch scenarios where we just
        need the text without registry coordination or deep crawl.

        Returns
        -------
        dict or None
            {'text': str, 'content_type': str, 'status_code': int} on success.
        """
        from utility_api.ingest.rate_scraper import scrape_rate_page

        try:
            result = scrape_rate_page(url)
        except Exception as e:
            logger.debug(f"fetch_single_url failed: {e}")
            return None

        if result.error and not result.text:
            return None

        return {
            "text": result.text,
            "content_type": "pdf" if getattr(result, "is_pdf", False) else "html",
            "status_code": getattr(result, "status_code", 200),
        }

    # --- Deep Crawl Methods ---

    _RATE_INDICATORS = [
        "per 1,000", "$/1000", "$/1,000", "ccf", "rate schedule",
        "volumetric", "tier 1", "tier 2", "fixed charge", "base charge",
        "gallons", "monthly bill", "service charge", "water rate",
        "rate per", "per unit", "minimum bill", "usage charge",
    ]

    def _is_thin_content(self, text: str) -> bool:
        """Heuristic: does this page likely contain actual rate data?

        Returns True if the page is too short, lacks rate-specific keywords,
        or has rate keywords but no actual rate numbers (corporate landing page).

        The key insight: a page that TALKS ABOUT rates (keywords present) but
        doesn't CONTAIN rates (no precise dollar amounts like $22.65, $0.88724)
        is a landing page that links to the real rate schedule.
        """
        import re

        if not text:
            return True
        if len(text) < 2000:
            return True  # Too short to contain a rate schedule

        text_lower = text.lower()
        keyword_matches = sum(1 for kw in self._RATE_INDICATORS if kw in text_lower)
        if keyword_matches < 2:
            return True  # Content doesn't look rate-related at all

        # Count precise dollar amounts ($X.XX with 2+ decimals).
        # Rate prices: $22.65, $0.88724, $3.42 — always have 2+ decimal places.
        # News figures: $1.4 billion, $10 per month, $608M — 0-1 decimals.
        precise_dollars = len(re.findall(r'\$\d{1,3}\.\d{2,5}', text))

        if precise_dollars >= 1:
            return False  # Has rate-like dollar amounts — substantive enough

        # Has rate keywords but no precise dollar amounts → landing page
        logger.debug(
            f"  Thin content: {keyword_matches} rate keywords, "
            f"{precise_dollars} precise dollar amounts"
        )
        return True

    def _follow_best_links(
        self,
        base_url: str,
        page_html: str,
        pwsid: str,
        max_links: int = 3,
        level: int = 2,
        fetch_count: int = 0,
    ) -> dict | None:
        """Extract links from page, score for rate relevance, follow top candidates.

        Only follows links on the same domain. Returns the best page found:
        - If a substantive page (passes _is_thin_content) is found, returns it
        - Otherwise returns the best candidate seen (most chars with rate keywords),
          since a rate-adjacent page is still better than a homepage for parsing
        - Returns None only if no deeper pages could be fetched at all

        When a deeper URL is found, inserts a NEW scrape_registry row for it
        (the original landing page entry is preserved).

        Parameters
        ----------
        level : int
            Crawl depth level (1 = from homepage, 2+ = from intermediate pages).
            Controls link scoring strategy.
        fetch_count : int
            Running count of HTTP requests made so far for this utility.
        """
        from urllib.parse import urljoin, urlparse

        from bs4 import BeautifulSoup

        from utility_api.ingest.rate_scraper import scrape_rate_page

        base_domain = self._get_base_domain(base_url)

        try:
            soup = BeautifulSoup(page_html, "html.parser")
        except Exception:
            return None

        # Collect and score links
        scored_links = []
        seen_hrefs = set()

        for a_tag in soup.find_all("a", href=True):
            href = urljoin(base_url, a_tag["href"])
            parsed = urlparse(href)

            # Only same base-domain links (allows subdomains like water.city.gov)
            if self._get_base_domain(href) != base_domain:
                continue

            # Skip non-page resources
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in
                   (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
                    ".zip", ".rar", ".gz", ".7z",
                    ".doc", ".docx", ".xls", ".xlsx", ".pptx",
                    ".mp4", ".mp3", ".wav", ".avi")):
                continue

            # Deduplicate
            canonical = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
            if canonical in seen_hrefs or canonical == base_url:
                continue
            seen_hrefs.add(canonical)

            link_text = a_tag.get_text(strip=True).lower()
            score = self._score_link(path_lower, link_text, level=level)
            if score > 0:
                scored_links.append((score, href, link_text))

        # Sort by score descending, take top N
        scored_links.sort(reverse=True)

        if not scored_links:
            return None

        # Track the best candidate even if still "thin" — a rate-adjacent
        # page is better than the original homepage for parsing.
        best_candidate = None
        fetches_this_level = 0

        for score, href, link_text in scored_links[:max_links]:
            if fetch_count + fetches_this_level >= MAX_FETCHES_PER_UTILITY:
                break

            logger.debug(f"    L{level} crawl trying: [{score}] {href[:80]} ({link_text[:40]})")

            try:
                result = scrape_rate_page(href)
                fetches_this_level += 1
            except Exception as e:
                logger.debug(f"    L{level} crawl fetch failed: {e}")
                fetches_this_level += 1
                continue

            if result.error and not result.text:
                continue

            fetched_text = result.text or ""
            raw_html = getattr(result, "raw_html", None) or fetched_text
            candidate = {
                "url": href,
                "text": fetched_text,
                "raw_html": raw_html,
                "is_pdf": getattr(result, "is_pdf", False),
                "char_count": len(fetched_text),
                "_result": result,  # keep for registration
                "_link_score": score,
                "_fetches": fetches_this_level,
            }

            if not self._is_thin_content(fetched_text):
                # Found substantive content — register and return immediately
                self._register_deep_url(
                    pwsid=pwsid,
                    deep_url=href,
                    original_url=base_url,
                    result=result,
                )
                del candidate["_result"]
                return candidate

            # Track best fallback: prefer higher link score, then more content
            if best_candidate is None or (
                score > best_candidate["_link_score"]
                or (score == best_candidate["_link_score"]
                    and len(fetched_text) > best_candidate["char_count"])
            ):
                best_candidate = candidate

        # No substantive page found — return best candidate if it's an
        # improvement over the original (more content or rate-relevant URL)
        if best_candidate and best_candidate["char_count"] > 200:
            logger.info(
                f"  L{level} crawl: no substantive page, using best candidate "
                f"({best_candidate['char_count']:,} chars): "
                f"{best_candidate['url'][:80]}"
            )
            self._register_deep_url(
                pwsid=pwsid,
                deep_url=best_candidate["url"],
                original_url=base_url,
                result=best_candidate["_result"],
            )
            del best_candidate["_result"]
            best_candidate["_fetches"] = fetches_this_level
            return best_candidate

        return None

    @staticmethod
    def _get_base_domain(url: str) -> str:
        """Extract base domain for same-site comparison.

        Allows subdomain traversal (water.city.gov == www.city.gov)
        while blocking external sites (norton.com != city.gov).

        Handles .gov, .org, .com and state TLDs like .va.us.
        """
        from urllib.parse import urlparse

        hostname = (urlparse(url).hostname or "").lower().lstrip("www.")
        parts = hostname.split(".")
        if len(parts) >= 3 and parts[-1] == "us" and len(parts[-2]) == 2:
            # State TLD: roanoke.va.us
            return ".".join(parts[-3:])
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return hostname

    @staticmethod
    def _score_link(href_lower: str, link_text: str, level: int = 2) -> int:
        """Score a link's likelihood of leading to rate content.

        Parameters
        ----------
        level : int
            Crawl depth level.
            Level 1: broad navigation — find water/utility department from homepage.
            Level 2+: rate-focused — find the actual rate/fee schedule page.
        """
        score = 0
        combined = f"{href_lower} {link_text}"

        if level == 1:
            # ==============================================================
            # LEVEL 1: BROAD NAVIGATION
            # Goal: find the water/utility department page from a homepage
            # Be VERY generous — overscraping is preferred to missing pages
            # ==============================================================

            # Direct water/utility signals (strong)
            for kw in ["water", "utility", "utilities", "public works",
                        "public utilities", "sewer", "wastewater",
                        "stormwater", "drinking water"]:
                if kw in combined:
                    score += 25

            # Department/organizational navigation (moderate)
            for kw in ["departments", "department", "divisions", "division",
                        "services", "resident", "residents", "customer",
                        "infrastructure", "operations", "public services",
                        "environmental", "environment", "resources",
                        "community", "government", "about", "our services"]:
                if kw in combined:
                    score += 10

            # Path-based signals (strong — URL structure reveals site hierarchy)
            for kw in ["/water", "/utility", "/utilities", "/public-works",
                        "/departments", "/divisions", "/services",
                        "/residents", "/customer", "/public-utilities",
                        "/infrastructure", "/environmental", "/dpw",
                        "/pw", "/dpu", "/dwu"]:
                if kw in href_lower:
                    score += 15

            # Rate-adjacent signals (if rates are directly linked from homepage)
            for kw in ["rate", "fee", "billing", "tariff", "charges",
                        "schedule of", "pay bill", "pay my bill",
                        "account", "payment"]:
                if kw in combined:
                    score += 20

            # Penalties — clearly wrong departments
            for kw in ["police", "fire", "court", "courts", "clerk",
                        "election", "elections", "voter", "voting",
                        "parks", "recreation", "library", "museum",
                        "planning", "zoning", "building permits",
                        "human resources", "hr", "careers", "jobs",
                        "employment", "job openings",
                        "animal", "animal control", "shelter",
                        "tax", "taxes", "assessor", "property tax",
                        "sheriff", "jail", "corrections",
                        "health department", "social services",
                        "school", "schools", "education",
                        "transit", "transportation", "bus",
                        "cemetery", "golf", "pool",
                        "news", "press", "press release", "blog",
                        "calendar", "events", "meeting", "agenda",
                        "minutes", "video", "photo", "gallery",
                        "sitemap", "accessibility", "privacy",
                        "login", "sign in", "register",
                        "facebook", "twitter", "instagram", "youtube",
                        "linkedin", "mailto:"]:
                if kw in combined:
                    score -= 20

        else:
            # ==============================================================
            # LEVEL 2+: RATE-FOCUSED
            # Goal: find the actual rate/fee schedule page
            # Still broad — we want to find rates even if oddly labeled
            # ==============================================================

            # Direct rate signals (strong)
            for kw in ["rate", "rates", "fee", "fees", "tariff", "tariffs",
                        "schedule", "billing", "charges", "pricing",
                        "cost of water", "water cost", "how much",
                        "what does water cost", "rate schedule",
                        "fee schedule", "rate structure",
                        "current rates", "rate table",
                        "schedule of rates", "rates effective",
                        "residential rates", "commercial rates",
                        "water charges", "sewer charges",
                        "monthly charge", "service charge",
                        "base charge", "usage charge",
                        "volumetric", "tiered", "tier"]:
                if kw in combined:
                    score += 20

            # Specificity boost (very strong)
            for kw in ["tariff", "current rates", "rate table",
                        "schedule of rates", "fee schedule", "rates effective",
                        "water rate schedule", "sewer rate schedule",
                        "rate ordinance", "rate resolution"]:
                if kw in combined:
                    score += 25

            # PDF links with rate keywords (strongest signal)
            if href_lower.endswith(".pdf"):
                if any(kw in combined for kw in ["tariff", "rate", "schedule",
                                                  "fee", "charge"]):
                    score += 30
                elif score > 0:
                    score += 15  # Any PDF on a rate-adjacent page

            # Water/utility context (moderate — confirms we're in the right section)
            for kw in ["water", "utility", "utilities", "service",
                        "customer", "residential", "commercial",
                        "account", "billing", "payment",
                        "consumption", "usage", "meter",
                        "connection", "hookup", "tap"]:
                if kw in combined:
                    score += 5

            # Navigation deeper into rate section (moderate)
            for kw in ["details", "more information", "view rates",
                        "see rates", "rate information", "learn about rates",
                        "rate details", "full schedule"]:
                if kw in combined:
                    score += 15

            # Penalties — wrong content
            for kw in ["meeting", "agenda", "minutes", "news", "press",
                        "press release", "blog", "announcement",
                        "job", "career", "bid", "rfp", "procurement",
                        "contact", "contact us", "staff directory",
                        "ccr", "water quality report", "consumer confidence",
                        "petition", "case", "hearing", "testimony",
                        "docket", "proceeding", "filing",
                        "annual report", "rate case",
                        "conservation", "rebate", "incentive",
                        "assistance", "hardship", "low income",
                        "start service", "stop service", "transfer",
                        "outage", "emergency", "boil water",
                        "construction", "project", "capital improvement",
                        "faq", "frequently asked"]:
                if kw in combined:
                    score -= 15

            # Generic link text penalty (only if it's the ENTIRE link text)
            stripped = link_text.strip().lower()
            if stripped in ("here", "click here", "learn more", "read more",
                            "view more", "download", "see more", "details",
                            "more", "continue", "next"):
                score -= 15

        return max(0, score)

    # Rate-relevant keywords for deep crawl registration filter
    _RATE_URL_KEYWORDS = (
        "rate", "fee", "tariff", "billing", "water", "utility",
        "schedule", "charge", "service", "customer", "price",
    )

    def _register_deep_url(
        self,
        pwsid: str,
        deep_url: str,
        original_url: str,
        result,
    ) -> None:
        """Insert a new scrape_registry row for a deeper URL found via crawl.

        Only registers URLs that are on the same base domain AND have
        rate-relevant keywords in the path. This prevents junk entries
        like norton.com, paris.fr, etc.
        """
        from urllib.parse import urlparse

        # Quality gate: same base domain
        if self._get_base_domain(deep_url) != self._get_base_domain(original_url):
            return

        # Quality gate: rate-relevant URL path or PDF
        path_lower = urlparse(deep_url).path.lower()
        is_pdf = path_lower.endswith(".pdf")
        has_keyword = any(kw in path_lower for kw in self._RATE_URL_KEYWORDS)
        if not is_pdf and not has_keyword:
            return

        schema = settings.utility_schema
        now = datetime.now(timezone.utc)
        content_hash = getattr(result, "text_hash", None)
        char_count = len(result.text) if result.text else 0
        content_type = "pdf" if getattr(result, "is_pdf", False) else "html"

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.scrape_registry
                        (pwsid, url, url_source, content_type, status,
                         last_fetch_at, last_http_status, last_content_hash,
                         last_content_length, scraped_text, notes)
                    VALUES
                        (:pwsid, :url, 'deep_crawl', :ctype, 'active',
                         :now, :http_status, :hash, :length,
                         :scraped_text, :notes)
                    ON CONFLICT (pwsid, url) DO UPDATE SET
                        last_fetch_at = EXCLUDED.last_fetch_at,
                        last_http_status = EXCLUDED.last_http_status,
                        last_content_hash = EXCLUDED.last_content_hash,
                        last_content_length = EXCLUDED.last_content_length,
                        scraped_text = EXCLUDED.scraped_text,
                        status = 'active',
                        updated_at = NOW()
                """), {
                    "pwsid": pwsid,
                    "url": deep_url,
                    "ctype": content_type,
                    "now": now,
                    "http_status": getattr(result, "status_code", 200),
                    "hash": content_hash,
                    "length": char_count,
                    "scraped_text": result.text if result.text else None,
                    "notes": f"Deep crawl from {original_url[:120]}",
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Deep crawl registry write failed: {e}")

    def _update_registry_failure(
        self, registry_id: int, retry_count: int,
        error: str, http_status: int | None = None,
    ) -> None:
        """Update scrape_registry on fetch failure with retry logic."""
        schema = settings.utility_schema
        now = datetime.now(timezone.utc)

        if http_status == 404:
            status = "dead"
            retry_after = None
        elif http_status == 403:
            new_retry_count = retry_count + 1
            if new_retry_count > 5:
                status = "dead"
                retry_after = None
            else:
                status = "pending_retry"
                retry_after = now + timedelta(days=min(2 ** new_retry_count, 30))
        elif http_status and http_status >= 500:
            status = "pending_retry"
            retry_after = now + timedelta(hours=6)
        else:
            status = "pending_retry"
            retry_after = now + timedelta(days=1)

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_fetch_at = :now,
                        last_http_status = :http_status,
                        status = :status,
                        retry_after = :retry_after,
                        retry_count = retry_count + 1,
                        notes = :error,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": now,
                    "http_status": http_status,
                    "status": status,
                    "retry_after": retry_after,
                    "error": error[:500] if error else None,
                    "id": registry_id,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Registry failure update failed: {e}")

    @staticmethod
    def _is_high_confidence_url(url_source: str | None) -> bool:
        """Is this URL from a source that warrants Playwright escalation?

        Serper, curated, and metro_research URLs are high-confidence — Google
        or a human thinks this is a rate page. Domain-guessed and deep_crawl
        URLs are speculative and don't warrant the extra fetch.
        """
        return url_source in (
            "serper", "curated", "curated_portland", "metro_research",
            "searxng", "state_directory",
        )

    def _extract_rate_links(self, html: str, base_url: str) -> list[dict]:
        """Find links on a thin page that point to rate-related content.

        Returns up to 3 rate-relevant links, sorted by score descending.
        """
        from urllib.parse import urljoin

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        if not html or len(html) < 50:
            return []

        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return []

        base_domain = self._get_base_domain(base_url)

        rate_keywords = [
            "rate", "fee", "tariff", "schedule", "billing",
            "charge", "price", "cost", "water bill",
        ]

        # Strong rate-document signals: filenames/text patterns that almost
        # always indicate an actual rate schedule (vs. a news article that
        # happens to mention "rate"). These get a large positive boost so
        # real rate documents rank above generic keyword matches.
        strong_signals = [
            "rate schedule", "rate sheet", "tariff", "fee schedule",
            "water rates", "sewer rates", "rates and charges",
            "utility rates", "water utility rate",
        ]

        # Mild negative signals: link types that are usually noise on a
        # rate page (news articles, calendar events). Kept small (-15) so
        # they're outweighed by any genuine rate signal — strategic plans,
        # meeting minutes, etc. CAN legitimately contain rate data.
        soft_negative_paths = [
            "/news/", "/blog/", "/press/", "/calendar/",
        ]

        candidates = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            link_text = link.get_text(strip=True).lower()
            href_lower = href.lower()

            full_url = urljoin(base_url, href)

            # Must be same domain
            if self._get_base_domain(full_url) != base_domain:
                continue

            # Skip anchors, mailto, tel
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue

            # Score by rate-keyword presence
            link_score = 0
            for kw in rate_keywords:
                if kw in link_text:
                    link_score += 20
                if kw in href_lower:
                    link_score += 15

            # Strong document signal bonus — phrases that indicate a rate
            # schedule, not just text that mentions "rate"
            for signal in strong_signals:
                if signal in link_text:
                    link_score += 50
                if signal in href_lower:
                    link_score += 40

            # PDF bonus (rate schedules are commonly published as PDFs)
            if href_lower.endswith(".pdf") and link_score > 0:
                link_score += 30  # was 10 — PDFs are strong rate document candidates

            # Mild penalty for obvious noise paths (news, blog, calendar).
            # Small enough that a real rate signal still wins.
            for seg in soft_negative_paths:
                if seg in href_lower:
                    link_score -= 15
                    break

            if link_score > 0:
                candidates.append({
                    "url": full_url,
                    "text": link_text[:100],
                    "score": link_score,
                })

        # Deduplicate by URL
        seen = set()
        unique = []
        for c in candidates:
            if c["url"] not in seen:
                seen.add(c["url"])
                unique.append(c)

        unique.sort(key=lambda c: c["score"], reverse=True)
        return unique[:3]

    def _register_nav_crawl_url(
        self, pwsid: str, url: str, parent_registry_id: int
    ):
        """Register a URL found via navigation link extraction."""
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.scrape_registry
                        (pwsid, url, url_source, status, notes)
                    VALUES (:pwsid, :url, 'nav_crawl', 'active', :notes)
                    ON CONFLICT (pwsid, url) DO NOTHING
                """), {
                    "pwsid": pwsid,
                    "url": url,
                    "notes": f"Nav crawl from registry_id={parent_registry_id}",
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Nav crawl URL registration failed: {e}")

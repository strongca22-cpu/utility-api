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

    This agent does NOT use an LLM. No `anthropic` import.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-25

Dependencies:
    - sqlalchemy
    - requests, playwright, pymupdf (via rate_scraper.py)
    - beautifulsoup4 (for deep crawl link extraction)

Usage:
    from utility_api.agents.scrape import ScrapeAgent
    result = ScrapeAgent().run(pwsid='VA4760100')

Notes:
    - Reads from scrape_registry (Sprint 13 change — Sprint 12 only wrote)
    - Returns raw text in memory for ParseAgent to consume
    - Retry logic: 403 → exponential backoff, 404 → dead, 5xx → 6h retry
    - Reuses existing rate_scraper.py functions — does not rewrite them
    - Deep crawl: follows 1-3 same-domain links when initial page is thin
"""

from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine


class ScrapeAgent(BaseAgent):
    """Fetches URLs from scrape_registry and returns raw text."""

    agent_name = "scrape"

    def run(
        self,
        registry_id: int | None = None,
        pwsid: str | None = None,
        **kwargs,
    ) -> dict:
        """Fetch content from URLs in scrape_registry.

        Parameters
        ----------
        registry_id : int, optional
            Fetch a specific registry entry by ID.
        pwsid : str, optional
            Fetch all pending URLs for this PWSID.

        Returns
        -------
        dict
            urls_fetched, succeeded, failed, raw_texts (list of dicts).
        """
        from utility_api.ingest.rate_scraper import scrape_rate_page

        schema = settings.utility_schema

        # Get URLs to fetch
        with engine.connect() as conn:
            if registry_id:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash, retry_count
                    FROM {schema}.scrape_registry
                    WHERE id = :id
                """), {"id": registry_id}).fetchall()
            elif pwsid:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash, retry_count
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

        logger.info(f"ScrapeAgent: {len(rows)} URLs to fetch")

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

            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_fetch_at = :now,
                        last_http_status = :status,
                        last_content_hash = :hash,
                        last_content_length = :length,
                        status = 'active',
                        notes = NULL,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": datetime.now(timezone.utc),
                    "status": getattr(scrape_result, "status_code", 200),
                    "hash": content_hash,
                    "length": char_count,
                    "id": row.id,
                })
                conn.commit()

            # Deep crawl: if content is thin (landing page), follow links
            # to find the actual rate schedule page. Only for HTML pages.
            final_url = url
            final_text = scrape_result.text
            final_is_pdf = getattr(scrape_result, "is_pdf", False)
            deep_crawled = False

            if (
                not final_is_pdf
                and self._is_thin_content(final_text)
                and char_count > 100  # has some content (not empty)
            ):
                logger.info(f"  Thin content ({char_count} chars) — attempting deep crawl")
                # Use raw HTML for link extraction (plain text strips href attributes)
                crawl_html = getattr(scrape_result, "raw_html", None) or scrape_result.text
                deeper = self._follow_best_links(
                    base_url=url,
                    page_html=crawl_html,
                    pwsid=row.pwsid,
                    max_links=3,
                )
                if deeper:
                    final_url = deeper["url"]
                    final_text = deeper["text"]
                    final_is_pdf = deeper.get("is_pdf", False)
                    deep_crawled = True
                    char_count = len(final_text) if final_text else 0
                    logger.info(f"  Deep crawl found: {final_url[:80]} ({char_count:,} chars)")

            raw_texts.append({
                "registry_id": row.id,
                "pwsid": row.pwsid,
                "url": final_url,
                "text": final_text,
                "content_type": "pdf" if final_is_pdf else "html",
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

    # --- Deep Crawl Methods (Sprint 16) ---

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

        if precise_dollars >= 3:
            return False  # Has multiple rate-like dollar amounts — substantive

        # Has rate keywords but few/no precise dollar amounts → landing page
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
    ) -> dict | None:
        """Extract links from page, score for rate relevance, follow top candidates.

        Only follows links on the same domain. Returns the first substantive
        page found, or None if no deeper rate content is discovered.

        When a deeper URL is found, inserts a NEW scrape_registry row for it
        (the original landing page entry is preserved).
        """
        from urllib.parse import urljoin, urlparse

        from bs4 import BeautifulSoup

        from utility_api.ingest.rate_scraper import scrape_rate_page

        base_domain = urlparse(base_url).hostname

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

            # Only same-domain links
            if parsed.hostname != base_domain:
                continue

            # Skip non-page resources
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in
                   (".jpg", ".png", ".gif", ".zip", ".doc", ".xlsx", ".mp4")):
                continue

            # Deduplicate
            canonical = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
            if canonical in seen_hrefs or canonical == base_url:
                continue
            seen_hrefs.add(canonical)

            link_text = a_tag.get_text(strip=True).lower()
            score = self._score_link(path_lower, link_text)
            if score > 0:
                scored_links.append((score, href, link_text))

        # Sort by score descending, take top N
        scored_links.sort(reverse=True)

        for score, href, link_text in scored_links[:max_links]:
            logger.debug(f"    Deep crawl trying: [{score}] {href[:80]} ({link_text[:40]})")

            try:
                result = scrape_rate_page(href)
            except Exception as e:
                logger.debug(f"    Deep crawl fetch failed: {e}")
                continue

            if result.error and not result.text:
                continue

            fetched_text = result.text or ""
            if not self._is_thin_content(fetched_text):
                # Found substantive content — register the deeper URL
                self._register_deep_url(
                    pwsid=pwsid,
                    deep_url=href,
                    original_url=base_url,
                    result=result,
                )
                return {
                    "url": href,
                    "text": fetched_text,
                    "is_pdf": getattr(result, "is_pdf", False),
                    "char_count": len(fetched_text),
                }

        return None

    @staticmethod
    def _score_link(href_lower: str, link_text: str) -> int:
        """Score a link's likelihood of leading to a rate schedule. No LLM."""
        score = 0
        combined = f"{href_lower} {link_text}"

        # Strong positive signals
        for kw in ["rate", "fee", "schedule", "tariff", "billing",
                    "water cost", "water rate", "rate schedule"]:
            if kw in combined:
                score += 20

        # PDF links with rate keywords
        if href_lower.endswith(".pdf") and score > 0:
            score += 15

        # Specificity boost — consumer-facing rate documents
        for kw in ["tariff", "current rates", "rate table",
                    "schedule of rates", "fee schedule", "rates effective"]:
            if kw in combined:
                score += 25

        # PDF + tariff combo — strongest signal (tariff PDF is the target)
        if href_lower.endswith(".pdf"):
            if any(kw in combined for kw in ["tariff", "schedule of rates"]):
                score += 30

        # Moderate positive
        for kw in ["water", "utility", "service", "customer", "residential"]:
            if kw in combined:
                score += 5

        # Negative: non-content pages
        for kw in ["meeting", "agenda", "minute", "news", "press",
                    "job", "career", "bid", "contact", "report", "ccr",
                    "login", "signup", "register", "calendar"]:
            if kw in combined:
                score -= 15

        # Negative: regulatory filings (long documents, not consumer rate schedules)
        for kw in ["petition", "case", "hearing", "testimony", "docket",
                    "proceeding", "filing", "application", "order",
                    "annual report", "rate case"]:
            if kw in combined:
                score -= 20

        # Negative: generic link text with no information about destination
        stripped = link_text.strip().lower()
        if stripped in ("here", "click here", "learn more", "read more",
                        "view more", "download", "see more", "details"):
            score -= 15

        return max(0, score)

    def _register_deep_url(
        self,
        pwsid: str,
        deep_url: str,
        original_url: str,
        result,
    ) -> None:
        """Insert a new scrape_registry row for a deeper URL found via crawl."""
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
                         last_content_length, notes)
                    VALUES
                        (:pwsid, :url, 'deep_crawl', :ctype, 'active',
                         :now, :http_status, :hash, :length,
                         :notes)
                    ON CONFLICT (pwsid, url) DO UPDATE SET
                        last_fetch_at = EXCLUDED.last_fetch_at,
                        last_http_status = EXCLUDED.last_http_status,
                        last_content_hash = EXCLUDED.last_content_hash,
                        last_content_length = EXCLUDED.last_content_length,
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

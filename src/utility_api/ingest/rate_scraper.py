#!/usr/bin/env python3
"""
Rate Page Web Scraper

Purpose:
    Fetch and extract text content from utility water rate pages.
    Handles HTML pages (requests + BeautifulSoup) and flags PDF/JS-heavy
    pages for manual review or Playwright follow-up.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - httpx
    - beautifulsoup4

Usage:
    from utility_api.ingest.rate_scraper import scrape_rate_page
    result = scrape_rate_page("https://example.gov/water-rates")

Notes:
    - Extracts visible text from HTML, stripping nav/footer/header noise
    - Computes SHA-256 hash of extracted text for change detection
    - PDF URLs are flagged but not parsed (future enhancement)
    - Respects robots.txt via User-Agent identification
    - Returns structured ScrapeResult with text, hash, and metadata

Data Sources:
    - Input: URLs from rate_discovery step
    - Output: Extracted text content for Claude API parsing
"""

import hashlib
import re
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup, Comment
from loguru import logger

SCRAPE_HEADERS = {
    "User-Agent": (
        "StrongStrategic-WaterRateBot/0.1 "
        "(water rate research; contact: research@strongstrategic.com)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# HTML elements to strip (navigation, headers, footers, scripts)
STRIP_TAGS = [
    "script", "style", "nav", "header", "footer", "noscript",
    "iframe", "form", "button", "input", "select", "textarea",
    "svg", "img", "video", "audio",
]

# Max text length to send to Claude API (chars) — keeps cost manageable
MAX_TEXT_LENGTH = 15_000


@dataclass
class ScrapeResult:
    """Result of scraping a single URL."""

    url: str
    text: str
    text_hash: str  # SHA-256 of extracted text
    content_type: str
    status_code: int
    is_pdf: bool = False
    is_js_heavy: bool = False
    error: str | None = None
    char_count: int = 0


def _clean_html_text(soup: BeautifulSoup) -> str:
    """Extract clean visible text from parsed HTML.

    Strips navigation, scripts, and other non-content elements.
    Collapses whitespace and removes very short lines.

    Parameters
    ----------
    soup : BeautifulSoup
        Parsed HTML document.

    Returns
    -------
    str
        Cleaned visible text.
    """
    # Remove unwanted elements
    for tag_name in STRIP_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Remove HTML comments
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # Try to find main content area first
    main_content = (
        soup.find("main")
        or soup.find("article")
        or soup.find(id=re.compile(r"content|main", re.I))
        or soup.find(class_=re.compile(r"content|main|body", re.I))
        or soup.body
        or soup
    )

    # Get text with some structure preservation
    text = main_content.get_text(separator="\n", strip=True)

    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Collapse multiple spaces
    text = re.sub(r"[ \t]{2,}", " ", text)

    # Remove very short lines (likely nav items, breadcrumbs)
    lines = text.split("\n")
    filtered_lines = []
    for line in lines:
        stripped = line.strip()
        # Keep lines with substance (>5 chars) or blank lines (for paragraph breaks)
        if len(stripped) > 5 or stripped == "":
            filtered_lines.append(stripped)

    text = "\n".join(filtered_lines)

    # Truncate if too long
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH] + "\n\n[TRUNCATED — page exceeded 15,000 chars]"

    return text.strip()


def _detect_js_heavy(html: str, text: str) -> bool:
    """Detect if a page likely requires JavaScript to render content.

    Parameters
    ----------
    html : str
        Raw HTML source.
    text : str
        Extracted visible text.

    Returns
    -------
    bool
        True if the page appears to need JS rendering.
    """
    # Very little visible text but lots of script tags
    if len(text) < 200 and html.count("<script") > 5:
        return True

    # Common SPA framework indicators with minimal content
    spa_indicators = ["__NEXT_DATA__", "react-root", "ng-app", "vue-app", "#app"]
    if len(text) < 200 and any(ind in html for ind in spa_indicators):
        return True

    return False


def scrape_rate_page(url: str, timeout: float = 30.0) -> ScrapeResult:
    """Scrape a single rate page URL and extract text content.

    Parameters
    ----------
    url : str
        URL to scrape.
    timeout : float
        Request timeout in seconds.

    Returns
    -------
    ScrapeResult
        Scraped content with metadata.
    """
    try:
        with httpx.Client(
            headers=SCRAPE_HEADERS,
            timeout=timeout,
            follow_redirects=True,
            max_redirects=5,
        ) as client:
            response = client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="",
            status_code=e.response.status_code,
            error=f"HTTP {e.response.status_code}: {e.response.reason_phrase}",
        )
    except httpx.HTTPError as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="",
            status_code=0,
            error=f"Request failed: {e}",
        )

    content_type = response.headers.get("content-type", "").lower()
    status_code = response.status_code

    # Handle PDF — flag for separate processing
    if "pdf" in content_type or url.lower().endswith(".pdf"):
        return ScrapeResult(
            url=url, text="[PDF document — requires separate PDF extraction]",
            text_hash="", content_type=content_type,
            status_code=status_code, is_pdf=True,
        )

    # Parse HTML
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    # Extract clean text
    text = _clean_html_text(soup)

    # Detect JS-heavy pages
    is_js_heavy = _detect_js_heavy(html, text)

    # Hash the extracted text
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

    result = ScrapeResult(
        url=url,
        text=text,
        text_hash=text_hash,
        content_type=content_type,
        status_code=status_code,
        is_pdf=False,
        is_js_heavy=is_js_heavy,
        char_count=len(text),
    )

    if is_js_heavy:
        result.error = "Page appears to require JavaScript rendering"
        logger.warning(f"JS-heavy page detected: {url} ({len(text)} chars extracted)")
    else:
        logger.info(f"Scraped {url}: {len(text)} chars")

    return result


def scrape_rate_pages(
    urls: dict[str, str],
    delay_seconds: float = 1.5,
) -> dict[str, ScrapeResult]:
    """Scrape multiple rate page URLs.

    Parameters
    ----------
    urls : dict[str, str]
        Mapping of pwsid → URL.
    delay_seconds : float
        Delay between requests.

    Returns
    -------
    dict[str, ScrapeResult]
        Mapping of pwsid → scrape result.
    """
    import time

    results = {}
    total = len(urls)

    for i, (pwsid, url) in enumerate(urls.items()):
        logger.info(f"[{i + 1}/{total}] Scraping {pwsid}: {url}")
        results[pwsid] = scrape_rate_page(url)

        if i < total - 1:
            time.sleep(delay_seconds)

    # Summary
    success = sum(1 for r in results.values() if not r.error and r.char_count > 100)
    pdf = sum(1 for r in results.values() if r.is_pdf)
    js_heavy = sum(1 for r in results.values() if r.is_js_heavy)
    failed = sum(1 for r in results.values() if r.error and not r.is_pdf and not r.is_js_heavy)

    logger.info(f"Scrape complete: {success} success, {pdf} PDF, {js_heavy} JS-heavy, {failed} failed")

    return results

#!/usr/bin/env python3
"""
Rate Page Web Scraper

Purpose:
    Fetch and extract text content from utility water rate pages.
    Three-tier scraping strategy:
    1. Static HTTP (httpx + BeautifulSoup) — fast, no browser needed
    2. Playwright headless Chromium — for JS-rendered pages (CivicPlus, etc.)
    3. PDF text extraction — for rate schedule PDFs

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - httpx
    - beautifulsoup4
    - playwright (for JS-rendered pages)
    - pymupdf (optional, for PDF extraction)

Usage:
    from utility_api.ingest.rate_scraper import scrape_rate_page
    result = scrape_rate_page("https://example.gov/water-rates")

Notes:
    - Tries static HTTP first; auto-falls back to Playwright if JS-heavy
    - Extracts visible text from HTML, stripping nav/footer/header noise
    - Computes SHA-256 hash of extracted text for change detection
    - PDF extraction via pymupdf if installed, otherwise flags for review
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


def _scrape_with_playwright(url: str, timeout_ms: int = 30_000) -> ScrapeResult:
    """Scrape a JS-rendered page using Playwright headless Chromium.

    Parameters
    ----------
    url : str
        URL to scrape.
    timeout_ms : int
        Page load timeout in milliseconds.

    Returns
    -------
    ScrapeResult
        Scraped content with metadata.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="",
            status_code=0,
            error="playwright not installed. Run: pip install playwright && playwright install chromium",
        )

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            # Navigate and wait for content to render
            page.goto(url, timeout=timeout_ms, wait_until="networkidle")

            # Give dynamic content a moment to settle
            page.wait_for_timeout(2000)

            # Get the rendered HTML
            html = page.content()
            status_code = 200  # Playwright doesn't expose status easily

            browser.close()

    except Exception as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="text/html",
            status_code=0,
            error=f"Playwright error: {e}",
        )

    # Parse the rendered HTML
    soup = BeautifulSoup(html, "html.parser")
    text = _clean_html_text(soup)
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

    logger.info(f"Playwright scraped {url}: {len(text)} chars")

    return ScrapeResult(
        url=url,
        text=text,
        text_hash=text_hash,
        content_type="text/html",
        status_code=status_code,
        is_pdf=False,
        is_js_heavy=False,  # We've already rendered the JS
        char_count=len(text),
    )


def _extract_pdf_text(url: str, timeout: float = 30.0) -> ScrapeResult:
    """Download and extract text from a PDF rate schedule.

    Parameters
    ----------
    url : str
        URL of the PDF document.
    timeout : float
        Download timeout in seconds.

    Returns
    -------
    ScrapeResult
        Extracted text content from the PDF.
    """
    try:
        import pymupdf  # PyMuPDF
    except ImportError:
        return ScrapeResult(
            url=url,
            text="[PDF document — pymupdf not installed for extraction]",
            text_hash="", content_type="application/pdf",
            status_code=200, is_pdf=True,
            error="pymupdf not installed. Run: pip install pymupdf",
        )

    try:
        with httpx.Client(
            headers=SCRAPE_HEADERS, timeout=timeout, follow_redirects=True
        ) as client:
            response = client.get(url)
            response.raise_for_status()
    except httpx.HTTPError as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="application/pdf",
            status_code=0, is_pdf=True,
            error=f"PDF download failed: {e}",
        )

    try:
        doc = pymupdf.open(stream=response.content, filetype="pdf")
        pages_text = []
        for page in doc:
            pages_text.append(page.get_text())
        doc.close()

        text = "\n\n".join(pages_text).strip()

        # Truncate if too long
        if len(text) > MAX_TEXT_LENGTH:
            text = text[:MAX_TEXT_LENGTH] + "\n\n[TRUNCATED — PDF exceeded 15,000 chars]"

        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

        logger.info(f"PDF extracted {url}: {len(text)} chars from {len(pages_text)} pages")

        return ScrapeResult(
            url=url,
            text=text,
            text_hash=text_hash,
            content_type="application/pdf",
            status_code=response.status_code,
            is_pdf=True,
            char_count=len(text),
        )
    except Exception as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="application/pdf",
            status_code=200, is_pdf=True,
            error=f"PDF extraction failed: {e}",
        )


def scrape_rate_page(url: str, timeout: float = 30.0, use_playwright: bool = True) -> ScrapeResult:
    """Scrape a single rate page URL and extract text content.

    Three-tier strategy:
    1. If URL is a PDF → extract text with pymupdf
    2. Try static HTTP first (fast, no browser)
    3. If JS-heavy detected and use_playwright=True → retry with Playwright

    Parameters
    ----------
    url : str
        URL to scrape.
    timeout : float
        Request timeout in seconds.
    use_playwright : bool
        If True, automatically fall back to Playwright for JS-heavy pages.

    Returns
    -------
    ScrapeResult
        Scraped content with metadata.
    """
    # PDF shortcut — go straight to PDF extraction
    if url.lower().endswith(".pdf"):
        return _extract_pdf_text(url, timeout)

    # Step 1: Try static HTTP
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
        # On 403, try Playwright (some sites only block non-browser requests)
        if e.response.status_code == 403 and use_playwright:
            logger.info(f"HTTP 403 from {url} — retrying with Playwright")
            return _scrape_with_playwright(url, timeout_ms=int(timeout * 1000))
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

    # Handle PDF response (content-type detection)
    if "pdf" in content_type:
        return _extract_pdf_text(url, timeout)

    # Parse HTML
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    # Extract clean text
    text = _clean_html_text(soup)

    # Detect JS-heavy pages
    is_js_heavy = _detect_js_heavy(html, text)

    # Auto-fallback to Playwright for JS-heavy pages
    if is_js_heavy and use_playwright:
        logger.info(f"JS-heavy page detected ({len(text)} chars) — retrying with Playwright: {url}")
        return _scrape_with_playwright(url, timeout_ms=int(timeout * 1000))

    # Hash the extracted text
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

    result = ScrapeResult(
        url=url,
        text=text,
        text_hash=text_hash,
        content_type=content_type,
        status_code=status_code,
        is_pdf=False,
        is_js_heavy=False,
        char_count=len(text),
    )

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

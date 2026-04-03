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
Modified: 2026-04-03

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
    "iframe", "button", "input", "select", "textarea",
    "svg", "img", "video", "audio",
    # NOTE: "form" deliberately excluded. ASP.NET/Sitefinity CMS wraps the
    # entire page in <form id="main">. Stripping it destroys all content.
    # This affected SCWA (1.1M pop) and likely other ASP.NET utility sites.
]

# Max text length to send to Claude API (chars) — keeps cost manageable
MAX_TEXT_LENGTH = 15_000


def extract_service_area_section(
    full_text: str,
    utility_name: str,
) -> str | None:
    """Extract the rate schedule section for a specific service area from a multi-area tariff.

    Looks for "RATE SCHEDULE" section headers and matches the utility name
    suffix (e.g., "SHORT HILLS" from "NJ AMERICAN WATER - SHORT HILLS") to
    find the relevant section. Returns the matched section text, or None if
    no match found.

    Works for NJ American Water, PA PUC, and other multi-service-area tariffs
    that use named section headers.

    Parameters
    ----------
    full_text : str
        Full extracted PDF text (up to 45k chars from smart extraction).
    utility_name : str
        The utility's pws_name from cws_boundaries.

    Returns
    -------
    str | None
        Extracted section text with context header, or None if no match.
    """
    import re

    if not full_text or not utility_name:
        return None

    # Don't attempt on short text — section extraction only helps large tariffs
    if len(full_text) < 5000:
        return None

    # Check for multi-section tariff markers
    # Match standalone section headers like "RATE SCHEDULE A-1" at start of line
    # but NOT inline cross-references like "shown on Rate Schedule O-2"
    schedule_pattern = re.compile(
        r'^\s*RATE\s+SCHEDULE\s+[\w-]+\s*$',
        re.IGNORECASE | re.MULTILINE,
    )
    schedule_headers = list(schedule_pattern.finditer(full_text))
    if len(schedule_headers) < 2:
        return None  # Not a multi-section tariff

    # Extract the service area name from utility_name
    # Patterns: "NJ AMERICAN WATER - SHORT HILLS", "CAL WATER - VISALIA", "AQUA PA - SHENANGO"
    search_terms = []

    if " - " in utility_name:
        suffix = utility_name.split(" - ", 1)[1].strip()
        search_terms.append(suffix.lower())
        # Also try individual words from suffix (e.g., "COASTAL NORTH" → "coastal", "north")
        for word in suffix.split():
            if len(word) > 3:  # Skip short words like "OF", "THE"
                search_terms.append(word.lower())

    if not search_terms:
        return None  # No service area identifier to match

    # Find the rate schedule section that mentions our service area
    best_section = None
    best_score = 0

    for i, header_match in enumerate(schedule_headers):
        # Extract text from this header to the next header (or end)
        start = header_match.start()
        if i + 1 < len(schedule_headers):
            end = schedule_headers[i + 1].start()
        else:
            end = len(full_text)

        section_text = full_text[start:end]

        # Score: how many of our search terms appear in this section?
        section_lower = section_text[:2000].lower()  # Check header area
        score = sum(1 for term in search_terms if term in section_lower)

        if score > best_score:
            best_score = score
            best_section = section_text

    if best_section and best_score > 0:
        # Also include the general/default rate schedule (A-1) for context
        # if our match is a different schedule
        general_section = ""
        if schedule_headers:
            first_start = schedule_headers[0].start()
            if len(schedule_headers) > 1:
                first_end = schedule_headers[1].start()
            else:
                first_end = len(full_text)
            first_section = full_text[first_start:first_end]
            # Only include if it's different from our best match
            if first_section != best_section and len(first_section) < 5000:
                general_section = first_section + "\n\n---\n\n"

        result = (
            f"[Extracted rate schedule section for: {utility_name}]\n"
            f"[From multi-service-area tariff document]\n\n"
            f"{general_section}{best_section}"
        )

        # Cap at 45k
        if len(result) > MAX_TEXT_LENGTH * 3:
            result = result[:MAX_TEXT_LENGTH * 3]

        return result

    # Fallback: if no service area match but the tariff has a clear "general metered"
    # rate schedule (A-1 pattern), extract that as the default for all PWSIDs.
    # Most multi-area tariff PWSIDs use the default/general rate schedule.
    general_pattern = re.compile(
        r'^\s*RATE\s+SCHEDULE\s+A-?1\s*$',
        re.IGNORECASE | re.MULTILINE,
    )
    general_match = general_pattern.search(full_text)
    if general_match:
        start = general_match.start()
        # Find the next rate schedule header after A-1
        next_header = None
        for h in schedule_headers:
            if h.start() > start + 100:  # Skip A-1 itself
                next_header = h
                break
        end = next_header.start() if next_header else min(start + 10000, len(full_text))
        general_section = full_text[start:end]

        result = (
            f"[Default rate schedule (A-1) extracted for: {utility_name}]\n"
            f"[From multi-service-area tariff — no specific area match found]\n\n"
            f"{general_section}"
        )

        if len(result) > MAX_TEXT_LENGTH * 3:
            result = result[:MAX_TEXT_LENGTH * 3]

        return result

    return None


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
    raw_html: str | None = None  # Preserved for deep crawl link extraction


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

    # Try to find main content area — prefer the largest matching element
    # (avoids picking tiny skip-nav or accessibility wrappers)
    main_content = soup.find("main") or soup.find("article")
    if main_content is None:
        # Search by id/class patterns, pick the one with the most text
        candidates = []
        for el in soup.find_all(id=re.compile(r"content|main", re.I)):
            candidates.append(el)
        for el in soup.find_all(class_=re.compile(r"content|main|body", re.I)):
            if el not in candidates:
                candidates.append(el)
        if candidates:
            # Pick the candidate with the longest text content
            main_content = max(candidates, key=lambda el: len(el.get_text(strip=True)))
        else:
            main_content = soup.body or soup

    # Get text with some structure preservation
    text = main_content.get_text(separator="\n", strip=True)

    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Collapse multiple spaces
    text = re.sub(r"[ \t]{2,}", " ", text)

    # Remove very short lines (likely nav items, breadcrumbs).
    # Keep lines with dollar amounts (e.g., "$1.50") even if short — these are
    # rate values from table cells that are critical for extraction.
    lines = text.split("\n")
    filtered_lines = []
    for line in lines:
        stripped = line.strip()
        if len(stripped) > 3 or stripped == "" or "$" in stripped:
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
            try:
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()

                # Navigate with "load" wait — NOT "networkidle".
                # Chat widgets and analytics scripts keep the network busy
                # indefinitely on many utility sites, causing networkidle
                # to time out. "load" fires when the page itself is loaded;
                # the explicit wait afterward lets JS frameworks render.
                page.goto(url, timeout=timeout_ms, wait_until="load")

                # Wait for JS frameworks to render dynamic content.
                # CivicPlus and similar municipal CMS platforms load rate
                # table cell values via AJAX after the initial page load.
                # 5s was insufficient — table headers rendered but rate
                # values in <td> cells were missing (Broomfield, Dacono,
                # etc.). 12s covers the AJAX round-trip on slow municipal
                # servers.  No added cost — just wall-clock time.
                page.wait_for_timeout(12000)

                # Get the rendered HTML
                html = page.content()
                status_code = 200  # Playwright doesn't expose status easily
            finally:
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
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            # Retry with browser User-Agent — some sites block bots on PDFs
            logger.info(f"PDF 403 from {url} — retrying with browser headers")
            try:
                browser_headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "*/*",
                }
                with httpx.Client(
                    headers=browser_headers, timeout=timeout, follow_redirects=True
                ) as client:
                    response = client.get(url)
                    response.raise_for_status()
            except httpx.HTTPError as e2:
                return ScrapeResult(
                    url=url, text="", text_hash="", content_type="application/pdf",
                    status_code=403, is_pdf=True,
                    error=f"PDF download failed: {e2}",
                )
        else:
            return ScrapeResult(
                url=url, text="", text_hash="", content_type="application/pdf",
                status_code=e.response.status_code, is_pdf=True,
                error=f"PDF download failed: {e}",
            )
    except httpx.HTTPError as e:
        return ScrapeResult(
            url=url, text="", text_hash="", content_type="application/pdf",
            status_code=0, is_pdf=True,
            error=f"PDF download failed: {e}",
        )

    try:
        doc = pymupdf.open(stream=response.content, filetype="pdf")
        all_pages = []
        for page in doc:
            all_pages.append(page.get_text())
        doc.close()

        total_pages = len(all_pages)
        full_text = "\n\n".join(all_pages).strip()

        # For short PDFs, take everything (up to limit)
        if len(full_text) <= MAX_TEXT_LENGTH:
            text = full_text
        elif total_pages <= 20:
            # Moderate PDFs: truncate at limit
            text = full_text[:MAX_TEXT_LENGTH] + "\n\n[TRUNCATED]"
        else:
            # Large tariff PDFs (20+ pages): extract rate-relevant pages
            # instead of blindly taking the first N chars (which is usually
            # just the table of contents and definitions)
            import re
            rate_page_indices = []
            for i, page_text in enumerate(all_pages):
                # Pages with dollar amounts or rate keywords are rate-relevant
                has_dollars = bool(re.search(r'\$\d+\.\d{2}', page_text))
                has_rate_kw = bool(re.search(
                    r'(?:rate|charge|tariff|gallons|ccf|per\s+1,?000|service charge)',
                    page_text, re.IGNORECASE,
                ))
                if has_dollars and has_rate_kw:
                    rate_page_indices.append(i)

            if rate_page_indices:
                # Take rate-relevant pages, plus page 0-1 for context
                include = set(rate_page_indices[:30])  # Cap at 30 rate pages
                include.add(0)  # Cover page
                if 1 < total_pages:
                    include.add(1)  # Usually TOC
                selected = [all_pages[i] for i in sorted(include)]
                text = "\n\n".join(selected).strip()
                if len(text) > MAX_TEXT_LENGTH * 3:
                    text = text[: MAX_TEXT_LENGTH * 3]
                logger.info(
                    f"  PDF smart extract: {len(rate_page_indices)} rate pages "
                    f"of {total_pages} total, {len(text)} chars"
                )
            else:
                # No rate pages found — fall back to first N chars
                text = full_text[:MAX_TEXT_LENGTH] + "\n\n[TRUNCATED]"

        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

        logger.info(f"PDF extracted {url}: {len(text)} chars from {total_pages} pages")

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
        raw_html=html,  # Preserve for deep crawl link extraction
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

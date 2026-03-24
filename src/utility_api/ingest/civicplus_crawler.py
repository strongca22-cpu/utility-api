#!/usr/bin/env python3
"""
CivicPlus DocumentCenter Crawler

Purpose:
    Finds water rate PDFs on CivicPlus-powered municipal websites by
    searching their site and scoring results for rate-relevance.

    Approach:
    1. Playwright renders the CivicPlus site search (JS-rendered)
    2. Searches for "water rate schedule" + "utility billing fee"
    3. Extracts all links from results
    4. Scores each link by title for water-rate relevance
    5. Returns ranked candidates with DocumentCenter/View URLs

    This replaces the earlier folder-tree-navigation approach which
    was too slow (each folder expansion required a Playwright click).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - playwright (browser automation)
    - loguru (logging)

Usage:
    # Python
    from utility_api.ingest.civicplus_crawler import crawl_civicplus_search
    results = await crawl_civicplus_search("https://www.fredericksburgva.gov")

    # CLI
    ua-ingest civicplus-crawl --domain fredericksburgva.gov

Notes:
    - CivicPlus site search is JS-rendered — requires Playwright
    - ~14% of municipal utilities use CivicPlus (from Sprint 3 survey)
    - Multiple search queries are run to maximize coverage
    - Results are deduplicated by URL and scored by filename/title
    - Documents at /DocumentCenter/View/{id} are PDFs by default

Data Sources:
    - Input: CivicPlus municipal websites (site search)
    - Output: Ranked list of document URLs with relevance scores
"""

import asyncio
import re
from dataclasses import dataclass, field

from loguru import logger


# --- Search queries to run on each site ---
# Multiple queries to catch different naming conventions

SEARCH_QUERIES = [
    "water rate schedule",
    "utility billing fee schedule",
    "water sewer rates charges",
    "rate schedule residential water",
]


# --- Relevance scoring for link titles ---

# Strong positive: definitely a rate document (+3 each)
_STRONG_POS = [
    r"\bwater.*rate",
    r"\brate.*schedule",
    r"\bfee.*schedule",
    r"\butility.*rate",
    r"\bbilling.*calculat",
    r"\btariff",
    r"\bwater.*fee",
    r"\brate.*charge",
    r"\bsewer.*rate",
    r"\bwater.*sewer.*fee",
    r"\bamending.*water.*fee",
    r"\bamending.*rate",
]

# Moderate positive: could be a rate document (+1 each)
_MOD_POS = [
    r"\bwater\b",
    r"\brate\b",
    r"\butility.*billing",
    r"\bfee\b",
    r"\bresidential.*charge",
    r"\bservice.*charge",
    r"\bconsumption",
    r"\bccf\b",
    r"\bmetered",
    r"\bfy\s*20\d{2}",
]

# Negative: not a rate document (-2 each)
_NEG = [
    r"\bminutes\b",
    r"\bagenda\b",
    r"\baudit\b",
    r"\bwater\s*quality",
    r"\bccr\b",
    r"\bannual.*report\b",
    r"\bstormwater.*management",
    r"\bflood",
    r"\bordinance\b(?!.*rate)(?!.*fee)(?!.*charge)",  # ordinance without rate/fee
    r"\bjob.*posting",
    r"\bemployment",
    r"\bfire\b",
    r"\bpolice\b",
    r"\bpark\b",
    r"\brecreation\b",
    r"\belection\b",
    r"\bzoning\b",
    r"\bcomprehensive.*plan",
    r"\blihwap",
    r"\bspanish\b",
    r"\binfographic\b",
    r"\bsafety\b(?!.*rate)",
    r"\btax.*rate",
    r"\bproperty.*tax",
    r"\bvision\b",
    r"\baflac\b",
    r"\binsurance\b",
    r"\b401k\b",
    r"\bbudget\b(?!.*rate)",
    r"\bbuilding.*permit",
    r"\bconstruction.*permit",
    r"\bplanning.*service",
    r"\bcommercial.*permit",
    r"\belectric\b(?!.*water)",  # Electric-only rates (not combined)
    r"\bgas\b(?!.*water)",
    r"\bparking\b",
    r"\bgarage\b",
]


def score_link(title: str, url: str = "") -> float:
    """Score a search result link for water rate relevance.

    Parameters
    ----------
    title : str
        Link text / document title.
    url : str
        URL of the link.

    Returns
    -------
    float
        Relevance score. Higher = more likely rate document.
        Recommended threshold: 2.0 for high confidence.
    """
    text = title.lower()
    score = 0.0

    for pattern in _STRONG_POS:
        if re.search(pattern, text):
            score += 3.0

    for pattern in _MOD_POS:
        if re.search(pattern, text):
            score += 1.0

    for pattern in _NEG:
        if re.search(pattern, text):
            score -= 2.0

    # Bonus for DocumentCenter URLs (these are always downloadable PDFs)
    if "documentcenter" in url.lower():
        score += 1.0

    # Bonus for explicit PDF references
    if ".pdf" in url.lower() or "(pdf)" in text:
        score += 0.5

    return round(score, 1)


@dataclass
class SearchResult:
    """A link found via CivicPlus site search."""
    title: str
    url: str
    relevance_score: float = 0.0
    source_query: str = ""

    @property
    def is_document_center(self) -> bool:
        """Whether this is a DocumentCenter link (downloadable PDF)."""
        return "documentcenter" in self.url.lower()

    @property
    def doc_id(self) -> int | None:
        """Extract DocumentCenter document ID from URL."""
        match = re.search(r"documentcenter/view/(\d+)", self.url, re.IGNORECASE)
        return int(match.group(1)) if match else None


@dataclass
class CrawlResult:
    """Result of searching a CivicPlus site for rate documents."""
    domain: str
    base_url: str
    total_results: int = 0
    candidates: list[SearchResult] = field(default_factory=list)
    best_candidate: SearchResult | None = None
    errors: list[str] = field(default_factory=list)


async def crawl_civicplus_search(
    base_url: str,
    queries: list[str] | None = None,
    min_score: float = 2.0,
    timeout: int = 30000,
) -> CrawlResult:
    """Search a CivicPlus site for water rate documents.

    Parameters
    ----------
    base_url : str
        Base URL of the CivicPlus site (e.g., "https://www.fredericksburgva.gov").
    queries : list[str] | None
        Search queries to run. Defaults to SEARCH_QUERIES.
    min_score : float
        Minimum relevance score to include a result as a candidate.
    timeout : int
        Page load timeout in milliseconds.

    Returns
    -------
    CrawlResult
        Search results including ranked rate document candidates.
    """
    from playwright.async_api import async_playwright

    base_url = base_url.rstrip("/")
    domain = base_url.split("//")[-1]
    result = CrawlResult(domain=domain, base_url=base_url)

    if queries is None:
        queries = SEARCH_QUERIES

    all_links: dict[str, SearchResult] = {}  # url -> SearchResult (dedup by URL)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for query in queries:
            search_url = f"{base_url}/Search/Results?searchPhrase={query.replace(' ', '+')}&searchType=0"
            logger.debug(f"  Searching: {query}")

            try:
                await page.goto(search_url, timeout=timeout)
                await page.wait_for_timeout(4000)  # Wait for JS rendering
            except Exception as e:
                result.errors.append(f"Search failed for '{query}': {e}")
                continue

            # Extract all links
            links = await page.query_selector_all("a")
            for link in links:
                try:
                    href = await link.get_attribute("href") or ""
                    text = (await link.inner_text()).strip()
                except Exception:
                    continue

                if not text or len(text) < 3 or not href:
                    continue

                # Normalize URL
                if href.startswith("/"):
                    href = f"{base_url}{href}"

                # Skip navigation/footer links
                if any(skip in href.lower() for skip in [
                    "/search/", "javascript:", "#", "mailto:",
                    "/login", "/signin", "/account",
                ]):
                    continue

                # Score the link
                relevance = score_link(text, href)

                # Only keep if it has some relevance signal
                if relevance > 0:
                    if href not in all_links or relevance > all_links[href].relevance_score:
                        all_links[href] = SearchResult(
                            title=text,
                            url=href,
                            relevance_score=relevance,
                            source_query=query,
                        )

        await browser.close()

    # Sort by relevance
    sorted_results = sorted(all_links.values(), key=lambda r: r.relevance_score, reverse=True)

    result.total_results = len(sorted_results)
    result.candidates = [r for r in sorted_results if r.relevance_score >= min_score]
    result.best_candidate = result.candidates[0] if result.candidates else None

    logger.info(
        f"CivicPlus search {domain}: {result.total_results} results, "
        f"{len(result.candidates)} candidates (score >= {min_score})"
    )

    return result


async def crawl_multiple_sites(
    sites: list[dict],
    min_score: float = 2.0,
) -> list[dict]:
    """Search multiple CivicPlus sites for rate documents.

    Parameters
    ----------
    sites : list[dict]
        List of dicts with 'pwsid', 'name', 'domain' keys.
    min_score : float
        Minimum relevance score for candidates.

    Returns
    -------
    list[dict]
        Results per site with pwsid, domain, candidates, best_url.
    """
    results = []
    for i, site in enumerate(sites):
        domain = site["domain"]
        base_url = f"https://{domain}" if not domain.startswith("http") else domain
        logger.info(f"\n[{i + 1}/{len(sites)}] {site.get('name', domain)} ({site['pwsid']})")

        try:
            crawl = await crawl_civicplus_search(base_url, min_score=min_score)
            best_url = crawl.best_candidate.url if crawl.best_candidate else None
            results.append({
                "pwsid": site["pwsid"],
                "name": site.get("name", ""),
                "domain": domain,
                "total_results": crawl.total_results,
                "candidates": len(crawl.candidates),
                "best_url": best_url,
                "best_title": crawl.best_candidate.title if crawl.best_candidate else None,
                "best_score": crawl.best_candidate.relevance_score if crawl.best_candidate else 0,
                "all_candidates": [
                    {"title": c.title, "url": c.url, "score": c.relevance_score}
                    for c in crawl.candidates[:5]
                ],
                "errors": crawl.errors,
            })

            if best_url:
                logger.info(f"  Best: [{crawl.best_candidate.relevance_score:+.1f}] {crawl.best_candidate.title}")
                logger.info(f"        {best_url}")
            else:
                logger.warning(f"  No rate candidates found")

        except Exception as e:
            logger.error(f"  Failed: {e}")
            results.append({
                "pwsid": site["pwsid"],
                "domain": domain,
                "error": str(e),
            })

        # Rate limit between sites
        await asyncio.sleep(2)

    return results


def run_civicplus_crawl(
    sites: list[dict],
    min_score: float = 2.0,
) -> list[dict]:
    """Synchronous wrapper for crawl_multiple_sites.

    Parameters
    ----------
    sites : list[dict]
        List of dicts with 'pwsid', 'name', 'domain' keys.
    min_score : float
        Minimum relevance score for rate document candidates.

    Returns
    -------
    list[dict]
        Results per site.
    """
    return asyncio.run(crawl_multiple_sites(sites, min_score=min_score))

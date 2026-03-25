#!/usr/bin/env python3
"""
Domain Guesser

Purpose:
    Guess utility website domains from SDWIS metadata (utility name, county,
    state) without using any search engine. Uses DNS lookups and common
    domain patterns for municipal/county government sites.

    This bypasses SearXNG entirely — DNS lookups are free, instant, and
    unlimited. Expected hit rate: 20-30% of municipal utilities.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - socket (standard library, for DNS lookups)
    - sqlalchemy
    - loguru

Usage:
    from utility_api.ops.domain_guesser import DomainGuesser
    guesser = DomainGuesser()
    candidates = guesser.guess_urls('VA4760100', 'FAIRFAX WATER',
                                     'Fairfax', 'VA', 'L')

Notes:
    - No search engine, no LLM, no rate limiting
    - DNS lookups are fast (~100ms each) and free
    - Currently county-based only (no city column in SDWIS)
    - TODO: Add city-based patterns when city data is available from ECHO API
    - Only generates candidates for local (L) and mixed (M) owner types
    - Private (P) and federal (F) systems have unpredictable domains
"""

import re
import socket
from urllib.parse import urlparse

from loguru import logger


# County-based domain patterns (most common for municipal utilities)
_COUNTY_PATTERNS = [
    "{county_slug}county.gov",
    "{county_slug}countyva.gov",
    "{county_slug}.{state_lower}.us",
    "{county_slug}county.org",
    "{county_slug}county.com",
    "www.{county_slug}county.gov",
    "www.{county_slug}county.org",
    "{county_slug}co.gov",
    "co.{county_slug}.{state_lower}.us",
]

# Utility-name-based patterns
_NAME_PATTERNS = [
    "{name_slug}.org",
    "{name_slug}.com",
    "www.{name_slug}.org",
    "www.{name_slug}.com",
]

# Common rate page path suffixes
RATE_PATHS = [
    "/water/rates",
    "/utilities/rates",
    "/departments/public-works/water/rates",
    "/residents/water/rates",
    "/water-rates",
    "/rates-fees",
    "/customer-service/rates",
    "/billing/rates",
    "/public-works/water",
    "/utilities",
]


def _slugify(text: str) -> str:
    """Convert 'Prince William' to 'princewilliam'."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _slugify_hyphen(text: str) -> str:
    """Convert 'Prince William' to 'prince-william'."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _dns_resolves(domain: str) -> bool:
    """Check if a domain has DNS A records. Fast, free, unlimited."""
    try:
        socket.getaddrinfo(domain, None, socket.AF_INET, socket.SOCK_STREAM)
        return True
    except socket.gaierror:
        return False


class DomainGuesser:
    """Guess utility website domains from SDWIS metadata."""

    def guess_urls(
        self,
        pwsid: str,
        pws_name: str,
        county: str | None,
        state: str,
        owner_type: str | None = None,
    ) -> list[dict]:
        """Generate candidate URLs from metadata via DNS guessing.

        Parameters
        ----------
        pwsid : str
            EPA PWSID.
        pws_name : str
            SDWIS utility name.
        county : str, optional
            County name.
        state : str
            2-letter state code.
        owner_type : str, optional
            SDWIS owner_type_code: L/M/P/F/S.

        Returns
        -------
        list[dict]
            List of {url, confidence, method, domain} for live domains found.
        """
        # Only useful for local/mixed/state utilities
        if owner_type in ("F", "P"):
            return []

        state_lower = state.lower()
        domains = set()

        # Generate county-based domain candidates
        if county:
            county_slug = _slugify(county)
            for pattern in _COUNTY_PATTERNS:
                try:
                    domain = pattern.format(
                        county_slug=county_slug,
                        state_lower=state_lower,
                    )
                    domains.add(domain)
                except (KeyError, IndexError):
                    continue

        # Generate name-based domain candidates
        if pws_name:
            # Extract meaningful name parts (remove generic suffixes)
            clean_name = re.sub(
                r"(?i)\s*(water|utility|utilities|department|dept|authority|"
                r"service|district|system|commission|co\.?|inc\.?|llc)\.?\s*$",
                "", pws_name,
            ).strip()
            if clean_name and len(clean_name) > 3:
                name_slug = _slugify(clean_name)
                name_hyphen = _slugify_hyphen(clean_name)
                for pattern in _NAME_PATTERNS:
                    try:
                        domains.add(pattern.format(name_slug=name_slug))
                        domains.add(pattern.format(name_slug=name_hyphen))
                    except (KeyError, IndexError):
                        continue

        # DNS-check each candidate
        live_domains = []
        for domain in domains:
            if not domain or domain.startswith("."):
                continue
            if _dns_resolves(domain):
                live_domains.append(domain)

        if not live_domains:
            return []

        logger.debug(f"DomainGuesser: {pwsid} — {len(live_domains)}/{len(domains)} domains resolve")

        # Build candidate URLs
        candidates = []
        for domain in live_domains:
            base_url = f"https://{domain}" if not domain.startswith("www.") else f"https://{domain}"

            # Homepage — the deep crawl can find the rate page from here
            candidates.append({
                "url": base_url,
                "confidence": "medium",
                "method": "domain_guess_homepage",
                "domain": domain,
            })

            # Try common rate page paths
            for path in RATE_PATHS[:5]:  # Top 5 most common paths
                candidates.append({
                    "url": f"{base_url}{path}",
                    "confidence": "low",
                    "method": "domain_guess_path",
                    "domain": domain,
                })

        return candidates


def run_domain_guessing(
    state_filter: str | None = None,
    max_utilities: int = 50,
    dry_run: bool = False,
) -> dict:
    """Run domain guessing across uncovered PWSIDs.

    Queries pwsid_coverage for PWSIDs with no pending URLs, generates
    domain candidates, and writes live domains to scrape_registry.

    Parameters
    ----------
    state_filter : str, optional
        Limit to a single state code.
    max_utilities : int
        Max utilities to process.
    dry_run : bool
        If True, report candidates without writing.

    Returns
    -------
    dict
        Summary: utilities_checked, domains_found, urls_written.
    """
    from sqlalchemy import text

    from utility_api.config import settings
    from utility_api.db import engine
    from utility_api.ops.registry_writer import log_discovery

    schema = settings.utility_schema
    guesser = DomainGuesser()

    # Get uncovered PWSIDs with county data
    state_clause = ""
    params: dict = {"limit": max_utilities}
    if state_filter:
        state_clause = "AND pc.state_code = :state"
        params["state"] = state_filter.upper()

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pc.pwsid, pc.pws_name, pc.state_code, c.county_served,
                   s.owner_type_code, pc.population_served
            FROM {schema}.pwsid_coverage pc
            LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = pc.pwsid
            LEFT JOIN {schema}.sdwis_systems s ON s.pwsid = pc.pwsid
            WHERE pc.has_rate_data = FALSE
              AND pc.scrape_status = 'not_attempted'
              AND c.county_served IS NOT NULL
              {state_clause}
            ORDER BY pc.population_served DESC NULLS LAST
            LIMIT :limit
        """), params).fetchall()

    logger.info(f"DomainGuesser: checking {len(rows)} utilities")

    utilities_checked = 0
    domains_found = 0
    urls_written = 0
    all_candidates = []

    for row in rows:
        utilities_checked += 1
        candidates = guesser.guess_urls(
            pwsid=row.pwsid,
            pws_name=row.pws_name,
            county=row.county_served,
            state=row.state_code,
            owner_type=row.owner_type_code,
        )

        if candidates:
            domains_found += 1
            # Take only the homepage candidates (deep crawl handles the rest)
            homepage_candidates = [c for c in candidates if c["method"] == "domain_guess_homepage"]

            if dry_run:
                all_candidates.extend(homepage_candidates)
            else:
                for c in homepage_candidates:
                    log_discovery(
                        pwsid=row.pwsid,
                        url=c["url"],
                        url_source="domain_guess",
                        notes=f"Domain guess: {c['domain']}",
                    )
                    urls_written += 1

    logger.info(f"DomainGuesser: {utilities_checked} checked, "
                f"{domains_found} with live domains, {urls_written} written")

    result = {
        "utilities_checked": utilities_checked,
        "domains_found": domains_found,
        "urls_written": urls_written,
    }
    if dry_run:
        result["candidates"] = all_candidates

    return result

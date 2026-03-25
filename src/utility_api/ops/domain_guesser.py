#!/usr/bin/env python3
"""
Domain Guesser

Purpose:
    Guess utility website domains from SDWIS metadata (utility name, county,
    city, state) without using any search engine. Uses DNS lookups and common
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
                                     'Fairfax', 'VA', 'L', city='Herndon')

Notes:
    - No search engine, no LLM, no rate limiting
    - DNS lookups are fast (~100ms each) and free
    - Patterns derived from config/domain_patterns.yaml research —
      update both when changing patterns
    - Only generates candidates for local (L) and mixed (M) owner types
    - Private (P) and federal (F) systems have unpredictable domains

Data Sources:
    - Input: utility.sdwis_systems (pws_name, county, city, state_code)
    - Output: utility.scrape_registry via log_discovery()
"""

import re
import socket

from loguru import logger


# =============================================================================
# Domain Patterns — ordered by estimated hit rate (from domain_patterns.yaml)
# =============================================================================

# City-based .gov patterns (highest hit rate for municipal utilities)
_CITY_PATTERNS = [
    "{city_slug}{state_lower}.gov",          # fairfaxva.gov
    "cityof{city_slug}.gov",                 # cityofroanoke.gov
    "{city_slug}.gov",                        # seattle.gov
    "{city_hyphen}-{state_lower}.gov",       # martinsville-va.gov
    "cityof{city_slug}.org",                 # cityofroanoke.org
    "{city_slug}{state_lower}.org",          # roanokeva.org
    "cityof{city_slug}.net",                 # cityofpasadena.net
    "{city_slug}.{state_lower}.us",          # roanoke.va.us
    "ci.{city_slug}.{state_lower}.us",       # ci.roanoke.va.us
    "{city_slug}water.org",                  # roanokewater.org
    "{city_slug}water.com",                  # roanokewater.com
]

# County-based domain patterns
_COUNTY_PATTERNS = [
    "{county_slug}county{state_lower}.gov",  # staffordcountyva.gov
    "{county_slug}county.gov",               # fairfaxcounty.gov
    "{county_slug}.{state_lower}.us",        # stafford.va.us
    "co.{county_slug}.{state_lower}.us",     # co.stafford.va.us
    "{county_slug}county.org",               # staffordcounty.org
    "{county_slug}county.com",               # staffordcounty.com
    "{county_slug}co.gov",                   # staffordco.gov
]

# Utility-name-based patterns
_NAME_PATTERNS = [
    "{name_slug}.org",
    "{name_slug}.com",
]

# Subdomain prefixes to check on confirmed base domains
_SUBDOMAIN_PREFIXES = ["utilities", "water", "publicworks"]

# Common rate page path suffixes
RATE_PATHS = [
    "/water/rates",
    "/utilities/water-rates",
    "/utilities/rates",
    "/water/rates-fees",
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
        city: str | None = None,
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
        city : str, optional
            City name (mailing address city from SDWIS).

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

        # --- City-based patterns (highest priority) ---
        if city and len(city.strip()) > 2:
            city_slug = _slugify(city)
            city_hyphen = _slugify_hyphen(city)
            for pattern in _CITY_PATTERNS:
                try:
                    domain = pattern.format(
                        city_slug=city_slug,
                        city_hyphen=city_hyphen,
                        state_lower=state_lower,
                    )
                    domains.add(domain)
                except (KeyError, IndexError):
                    continue

        # --- County-based patterns ---
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

        # --- Name-based patterns ---
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

        # --- DNS-check each candidate ---
        live_domains = []
        for domain in domains:
            if not domain or domain.startswith("."):
                continue
            if _dns_resolves(domain):
                live_domains.append(domain)

        if not live_domains:
            return []

        logger.debug(f"DomainGuesser: {pwsid} — {len(live_domains)}/{len(domains)} domains resolve")

        # --- Build candidate URLs ---
        candidates = []
        for domain in live_domains:
            base_url = f"https://{domain}"

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

            # --- Subdomain checks on confirmed base domains ---
            # Strip leading www. or subdomain to get the base
            base_domain = domain
            if base_domain.startswith("www."):
                base_domain = base_domain[4:]
            # Only check subdomains on base domains (not already a subdomain)
            if base_domain.count(".") <= 2:
                for prefix in _SUBDOMAIN_PREFIXES:
                    subdomain = f"{prefix}.{base_domain}"
                    if _dns_resolves(subdomain):
                        candidates.append({
                            "url": f"https://{subdomain}",
                            "confidence": "high",
                            "method": "domain_guess_subdomain",
                            "domain": subdomain,
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

    # Get uncovered PWSIDs with county data, now including city
    state_clause = ""
    params: dict = {"limit": max_utilities}
    if state_filter:
        state_clause = "AND pc.state_code = :state"
        params["state"] = state_filter.upper()

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pc.pwsid, pc.pws_name, pc.state_code, c.county_served,
                   s.owner_type_code, pc.population_served, s.city
            FROM {schema}.pwsid_coverage pc
            LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = pc.pwsid
            LEFT JOIN {schema}.sdwis_systems s ON s.pwsid = pc.pwsid
            WHERE pc.has_rate_data = FALSE
              AND pc.scrape_status = 'not_attempted'
              AND (c.county_served IS NOT NULL OR s.city IS NOT NULL)
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
            city=row.city,
        )

        if candidates:
            domains_found += 1
            # Take homepage + subdomain candidates (deep crawl handles path guessing)
            write_candidates = [
                c for c in candidates
                if c["method"] in ("domain_guess_homepage", "domain_guess_subdomain")
            ]

            if dry_run:
                all_candidates.extend(write_candidates)
            else:
                for c in write_candidates:
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

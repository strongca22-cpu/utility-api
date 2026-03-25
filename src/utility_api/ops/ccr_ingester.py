#!/usr/bin/env python3
"""
CCR Link Ingester

Purpose:
    Reads a manually-created CSV of PWSID + CCR URL pairs (from the EPA
    CCR search at https://sdwis.epa.gov/fylccr), extracts the base domain
    from each CCR URL, and generates candidate rate page URLs.

    This is a manual-input pipeline: someone searches the EPA CCR database
    in a browser, exports results, and feeds them here. Automation of the
    Oracle APEX form is deferred.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - loguru

Usage:
    from utility_api.ops.ccr_ingester import ingest_ccr_csv
    result = ingest_ccr_csv("data/ccr_links_va.csv")

    CLI:
    ua-ops ingest-ccr-links data/ccr_links_va.csv
    ua-ops ingest-ccr-links data/ccr_links_va.csv --dry-run

Notes:
    - CSV must have columns: pwsid, ccr_url
    - Skips EPA-hosted CCRs (epa.gov domains)
    - Skips common document hosting services (Google Drive, Dropbox, etc.)
    - Generates 3 candidate URLs per domain: /water/rates, /utilities, /rates
    - Writes to scrape_registry with url_source='ccr_derived'
"""

import csv
from pathlib import Path
from urllib.parse import urlparse

from loguru import logger

from utility_api.ops.registry_writer import log_discovery


# Domains to skip — EPA-hosted or generic document services
_SKIP_DOMAINS = {
    "epa.gov", "ofmpub.epa.gov", "sdwis.epa.gov", "safewater.epa.gov",
    "s3.amazonaws.com", "docs.google.com", "drive.google.com",
    "dropbox.com", "issuu.com", "scribd.com", "yumpu.com",
}

# Candidate rate page path suffixes to try
_CANDIDATE_PATHS = [
    "/water/rates",
    "/utilities",
    "/rates",
    "/public-works/water",
    "/departments/water",
]


def _extract_base_domain(ccr_url: str) -> str | None:
    """Extract usable base URL from a CCR document URL.

    Returns scheme://hostname or None if the domain should be skipped.

    Examples:
        https://www.pulaskicounty.org/docs/ccr2025.pdf → https://www.pulaskicounty.org
        https://sdwis.epa.gov/fylccr/report.pdf → None (EPA-hosted)
    """
    try:
        parsed = urlparse(ccr_url)
        hostname = parsed.hostname
        if not hostname:
            return None

        # Check skip list
        for skip in _SKIP_DOMAINS:
            if hostname == skip or hostname.endswith(f".{skip}"):
                return None

        scheme = parsed.scheme or "https"
        return f"{scheme}://{hostname}"
    except Exception:
        return None


def ingest_ccr_csv(csv_path: str, dry_run: bool = False) -> dict:
    """Read a CCR link CSV and generate candidate rate page URLs.

    Parameters
    ----------
    csv_path : str
        Path to CSV with columns: pwsid, ccr_url
    dry_run : bool
        If True, report candidates but don't write to registry.

    Returns
    -------
    dict
        Summary: rows_read, domains_extracted, candidates_generated,
        urls_written, candidates (if dry_run).
    """
    path = Path(csv_path)
    if not path.exists():
        logger.error(f"CCR CSV not found: {csv_path}")
        return {
            "rows_read": 0,
            "domains_extracted": 0,
            "candidates_generated": 0,
            "urls_written": 0,
            "error": f"File not found: {csv_path}",
        }

    rows_read = 0
    domains_extracted = 0
    candidates = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            pwsid = row.get("pwsid", "").strip()
            ccr_url = row.get("ccr_url", "").strip()

            if not pwsid or not ccr_url:
                continue

            base_url = _extract_base_domain(ccr_url)
            if not base_url:
                continue

            domains_extracted += 1

            # Generate candidate rate page URLs
            for path_suffix in _CANDIDATE_PATHS:
                candidate_url = base_url + path_suffix
                candidates.append({
                    "pwsid": pwsid,
                    "url": candidate_url,
                    "ccr_url": ccr_url,
                    "base_domain": base_url,
                })

    logger.info(
        f"CCR ingester: {rows_read} rows, {domains_extracted} valid domains, "
        f"{len(candidates)} candidate URLs"
    )

    if dry_run:
        return {
            "rows_read": rows_read,
            "domains_extracted": domains_extracted,
            "candidates_generated": len(candidates),
            "urls_written": 0,
            "candidates": candidates,
        }

    # Write to scrape_registry
    urls_written = 0
    for c in candidates:
        try:
            log_discovery(
                pwsid=c["pwsid"],
                url=c["url"],
                url_source="ccr_derived",
                discovery_query=None,
                notes=f"Derived from EPA CCR link: {c['ccr_url'][:120]}",
            )
            urls_written += 1
        except Exception as e:
            logger.warning(f"CCR registry write failed for {c['pwsid']}: {e}")

    logger.info(f"CCR ingester: wrote {urls_written} entries to scrape_registry")

    return {
        "rows_read": rows_read,
        "domains_extracted": domains_extracted,
        "candidates_generated": len(candidates),
        "urls_written": urls_written,
    }

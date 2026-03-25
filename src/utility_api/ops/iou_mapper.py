#!/usr/bin/env python3
"""
Investor-Owned Utility Mapper

Purpose:
    Maps major investor-owned water utility parent companies to their
    subsidiary PWSIDs using SDWIS name patterns, and writes results
    to both YAML config files and scrape_registry.

    Key insight: ~1,000-1,500 PWSIDs can be mapped to known corporate
    rate page URLs with zero search queries. These companies have
    well-known websites with per-state rate pages.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Notes:
    - Sprint 17: replaced iou_subsidiaries.yaml with SEC-sourced research data
      (82 named subsidiaries with SDWIS name variants, confidence levels)
    - Subsidiary loader now reads 'named_subsidiaries' key + sdwis_name_variants

Dependencies:
    - sqlalchemy
    - loguru
    - pyyaml

Usage:
    from utility_api.ops.iou_mapper import run_iou_mapping
    result = run_iou_mapping()

    CLI:
    ua-ops iou-map
    ua-ops iou-map --state VA
    ua-ops iou-map --dry-run

Notes:
    - Reads from sdwis_systems table (already populated for all 50 states)
    - Does NOT download SDWIS CSV — queries existing database
    - Writes YAML to config/rate_urls_{state}_iou.yaml
    - Writes to scrape_registry via log_discovery() with notes annotation
    - Pattern matching uses regex against pws_name field
    - Only matches CWS (community water systems)
"""

import re
from collections import defaultdict
from datetime import date
from pathlib import Path

from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine
from utility_api.ops.registry_writer import log_discovery


# --- Investor-Owned Utility Definitions ---
# Each entry defines: parent company name, regex patterns to match in
# pws_name, and state-specific rate page URLs.

INVESTOR_OWNED = [
    {
        # Verified 2026-03-25: American Water migrated from {state}amwater.com
        # to amwater.com/{state_code}aw/ path-based structure.
        "parent": "American Water Works",
        "patterns": [
            r"(?i)american water",
            r"(?i)am\.\s*water",
            r"(?i)cal[\-/\s]am\s+water",
        ],
        "state_url_map": {
            "NJ": "https://www.amwater.com/njaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "PA": "https://www.amwater.com/paaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "IN": "https://www.amwater.com/inaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "WV": "https://www.amwater.com/wvaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "IL": "https://www.amwater.com/ilaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "CA": "https://www.amwater.com/caaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "MO": "https://www.amwater.com/moaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "VA": "https://www.amwater.com/vaaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "IA": "https://www.amwater.com/iaaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "TN": "https://www.amwater.com/tnaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "MD": "https://www.amwater.com/mdaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "GA": "https://www.amwater.com/gaaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "KY": "https://www.amwater.com/kyaw/Customer-Service-Billing/your-water-and-wastewater-rates",
            "HI": "https://www.amwater.com/hiaw/Customer-Service-Billing/your-water-and-wastewater-rates",
        },
    },
    {
        # Verified 2026-03-25: Aqua uses single URL for all states, not per-state paths.
        "parent": "Aqua / Essential Utilities",
        "patterns": [
            r"(?i)aqua\s+(america|pennsylvania|ohio|north\s+carolina|texas|"
            r"illinois|indiana|virginia|new\s+jersey)",
            r"(?i)aqua\s+water\s+supply",
            r"(?i)essential\s+utilities",
        ],
        "state_url_map": {
            "PA": "https://www.aquawater.com/customers/water-rates",
            "OH": "https://www.aquawater.com/customers/water-rates",
            "NC": "https://www.aquawater.com/customers/water-rates",
            "TX": "https://www.aquawater.com/customers/water-rates",
            "IL": "https://www.aquawater.com/customers/water-rates",
            "IN": "https://www.aquawater.com/customers/water-rates",
            "VA": "https://www.aquawater.com/customers/water-rates",
            "NJ": "https://www.aquawater.com/customers/water-rates",
        },
    },
    {
        # Verified 2026-03-25: CalWater URL works.
        "parent": "California Water Service",
        "patterns": [
            r"(?i)california\s+water\s+service",
            r"(?i)cal\s*water",
            r"(?i)hawaii\s+water\s+service",
            r"(?i)washington\s+water\s+service",
            r"(?i)new\s+mexico\s+water\s+service",
        ],
        "state_url_map": {
            "CA": "https://www.calwater.com/rates/",
            "HI": "https://www.hawaiiwaterservice.com/rates/",
            "WA": "https://www.washingtonwaterservice.com/rates/",
            "NM": "https://www.newmexicowaterservice.com/rates/",
        },
    },
    {
        # Verified 2026-03-25: SJW rate pages moved; CT/ME use SJW-style billing page.
        "parent": "SJW Group / San Jose Water",
        "patterns": [
            r"(?i)san\s+jose\s+water",
            r"(?i)\bsjw\b",
            r"(?i)\bsjwtx\b",
            r"(?i)connecticut\s+water\s+co",
            r"(?i)maine\s+water\s+company",
        ],
        "state_url_map": {
            "CA": "https://www.sjwater.com/customer-care/help-information/rates-regulations/",
            "TX": "https://www.sjwtx.com/customer-care/help-information/rates-regulations/",
            "CT": "https://www.ctwater.com/service-billing/your-bill/pay-your-bill/#rate-schedules",
            "ME": "https://www.mainewater.com/service-billing/your-bill/pay-your-bill/#rate-schedules",
        },
    },
    {
        # Verified 2026-03-25: Middlesex URL corrected.
        "parent": "Middlesex Water Company",
        "patterns": [
            r"(?i)middlesex\s+water",
            r"(?i)tidewater\s+utilities",
            r"(?i)tidewater\s+environmental",
        ],
        "state_url_map": {
            "NJ": "https://www.middlesexwater.com/customer-care/rate-information/",
            "DE": "https://www.tidewaterutilities.com/rates/",
        },
    },
    {
        # Verified 2026-03-25: Artesian homepage shows no clear rate link.
        # TODO: deeper research needed for Artesian rate page URL.
        "parent": "Artesian Resources",
        "patterns": [
            r"(?i)artesian\s+water",
            r"(?i)artesian\s+resources",
        ],
        "state_url_map": {
            # Artesian URLs were 404 as of 2026-03-25 — removed until verified
        },
    },
    {
        # Verified 2026-03-25: Aquarion blocks curl (403). May work with browser
        # user-agent via Playwright. Keeping URLs but expect scrape failures.
        "parent": "Aquarion Water / Eversource",
        "patterns": [
            r"(?i)aquarion\s+water",
        ],
        "state_url_map": {
            "CT": "https://www.aquarionwater.com/customer-service/rates-billing",
            "MA": "https://www.aquarionwater.com/customer-service/rates-billing",
            "NH": "https://www.aquarionwater.com/customer-service/rates-billing",
        },
    },
    {
        "parent": "Central States Water Resources",
        "patterns": [
            r"(?i)central\s+states\s+water",
            r"(?i)\bcswr\b",
        ],
        # CSWR has many small systems; no per-state rate pages known
        "state_url_map": {},
    },
    {
        "parent": "Nexus Water Group",
        "patterns": [
            r"(?i)nexus\s+water",
            r"(?i)corix\s+",
            r"(?i)global\s+water\s+resources",
        ],
        # Nexus has many small systems; no per-state rate pages known
        "state_url_map": {},
    },
]


def _match_system(pws_name: str, state_code: str) -> tuple[str, str] | None:
    """Try to match a PWS name against known IOU patterns.

    Returns (parent_name, url) or None. Only returns matches where a
    valid URL exists for that state.
    """
    for iou in INVESTOR_OWNED:
        for pattern in iou["patterns"]:
            if re.search(pattern, pws_name):
                url = iou["state_url_map"].get(state_code)
                if url:
                    return (iou["parent"], url)
                # Matched the name but no URL for this state — skip
                return None
    return None


# --- Subsidiary Name Matching (Sprint 16) ---

# Common SDWIS abbreviations for normalization
_NAME_NORMALIZE = [
    (r"\bCO\.?\b", "COMPANY"),
    (r"\bCORP\.?\b", "CORPORATION"),
    (r"\bWTR\b", "WATER"),
    (r"\bSVC\b", "SERVICE"),
    (r"\bUTIL(?:S)?\b", "UTILITIES"),
    (r"\bINC\.?\b", ""),
    (r"\bLLC\b", ""),
    (r"\bLTD\b", ""),
    (r"\s+", " "),
]


def _normalize_name(name: str) -> str:
    """Normalize a utility name for fuzzy matching.

    Expands abbreviations, strips punctuation and suffixes.
    """
    upper = name.upper().strip()
    for pattern, replacement in _NAME_NORMALIZE:
        upper = re.sub(pattern, replacement, upper)
    # Strip trailing/leading whitespace and punctuation
    return re.sub(r"[^A-Z0-9 ]", "", upper).strip()


def _load_subsidiary_database() -> list[dict]:
    """Load subsidiary name database from config/iou_subsidiaries.yaml.

    Returns a list of dicts ready for matching. Each entry has:
    - normalized_names: list of normalized name strings to match against
    - original_name: the primary subsidiary name
    - parent: parent company name
    - state: 2-letter state code
    - url: rate page URL
    - confidence: confirmed/likely/unverified
    """
    config_path = PROJECT_ROOT / "config" / "iou_subsidiaries.yaml"
    if not config_path.exists():
        return []

    import yaml
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}

    entries = []
    for company_key, company in data.items():
        if not isinstance(company, dict):
            continue
        parent = company.get("parent", company_key)

        for sub in (company.get("named_subsidiaries") or []):
            name = sub.get("name")
            if not name:
                continue

            state = sub.get("state")
            url = sub.get("url")
            confidence = sub.get("confidence", "unverified")

            if not url:
                continue  # No URL available — skip

            # Build list of names to match: primary name + SDWIS variants
            match_names = [name]
            for variant in (sub.get("sdwis_name_variants") or []):
                match_names.append(variant)

            entries.append({
                "normalized_names": [_normalize_name(n) for n in match_names],
                "original_name": name,
                "parent": parent,
                "state": state,
                "url": url,
                "confidence": confidence,
            })

    logger.debug(f"Loaded {len(entries)} subsidiary entries from YAML")
    return entries


# Cache the subsidiary database at module level
_SUBSIDIARY_DB: list[dict] | None = None


def _get_subsidiary_db() -> list[dict]:
    """Get the subsidiary database (cached)."""
    global _SUBSIDIARY_DB
    if _SUBSIDIARY_DB is None:
        _SUBSIDIARY_DB = _load_subsidiary_database()
    return _SUBSIDIARY_DB


def _match_subsidiary(pws_name: str, state_code: str) -> tuple[str, str, str] | None:
    """Try to match a PWS name against the subsidiary name database.

    Uses normalized name comparison (abbreviation expansion, case folding).
    Checks primary name and all SDWIS name variants.
    Returns (parent_name, url, subsidiary_name) or None.
    """
    db = _get_subsidiary_db()
    if not db:
        return None

    normalized = _normalize_name(pws_name)
    if len(normalized) < 4:
        return None  # Too short to match reliably

    for entry in db:
        if entry["state"] and entry["state"] != state_code:
            continue
        for candidate in entry["normalized_names"]:
            if len(candidate) < 4:
                continue
            if candidate in normalized or normalized in candidate:
                return (entry["parent"], entry["url"], entry["original_name"])

    return None


def run_iou_mapping(
    state_filter: str | None = None,
    dry_run: bool = False,
    write_yaml: bool = True,
) -> dict:
    """Run IOU pattern matching against sdwis_systems and write results.

    Parameters
    ----------
    state_filter : str, optional
        Limit to a single state code (e.g., 'VA').
    dry_run : bool
        If True, report matches but don't write to DB or YAML.
    write_yaml : bool
        If True, write per-state YAML files to config/.

    Returns
    -------
    dict
        Summary: total_matched, urls_written_registry, urls_written_yaml,
        by_parent, by_state.
    """
    schema = settings.utility_schema

    # Query all active CWS from sdwis_systems
    state_clause = ""
    params: dict = {}
    if state_filter:
        state_clause = "AND s.state_code = :state"
        params["state"] = state_filter.upper()

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT s.pwsid, s.pws_name, s.state_code, s.population_served_count
            FROM {schema}.sdwis_systems s
            WHERE s.pws_type_code = 'CWS'
            {state_clause}
            ORDER BY s.state_code, s.population_served_count DESC NULLS LAST
        """), params).fetchall()

    logger.info(f"IOU mapper: scanning {len(rows)} CWS records...")

    matches = []
    by_parent: dict[str, int] = defaultdict(int)
    by_state: dict[str, int] = defaultdict(int)

    matched_pwsids = set()
    pattern_matches = 0
    subsidiary_matches = 0

    for row in rows:
        # Step 1: Try regex pattern matching (existing logic)
        result = _match_system(row.pws_name, row.state_code)
        match_type = "pattern"
        subsidiary_name = None

        # Step 2: If regex didn't match, try subsidiary name database
        if not result:
            sub_result = _match_subsidiary(row.pws_name, row.state_code)
            if sub_result:
                parent, url, subsidiary_name = sub_result
                result = (parent, url)
                match_type = "subsidiary_name"

        if result and row.pwsid not in matched_pwsids:
            parent, url = result
            matches.append({
                "pwsid": row.pwsid,
                "pws_name": row.pws_name,
                "state_code": row.state_code,
                "population": row.population_served_count or 0,
                "parent": parent,
                "url": url,
                "match_type": match_type,
                "subsidiary_name": subsidiary_name,
            })
            matched_pwsids.add(row.pwsid)
            by_parent[parent] += 1
            by_state[row.state_code] += 1
            if match_type == "pattern":
                pattern_matches += 1
            else:
                subsidiary_matches += 1

    logger.info(
        f"IOU mapper: matched {len(matches)} systems to {len(by_parent)} parent companies "
        f"({pattern_matches} pattern, {subsidiary_matches} subsidiary name)"
    )

    if dry_run:
        return {
            "total_matched": len(matches),
            "urls_written_registry": 0,
            "urls_written_yaml": 0,
            "by_parent": dict(by_parent),
            "by_state": dict(by_state),
            "matches": matches,
        }

    # --- Write to scrape_registry ---
    urls_written_registry = 0
    for m in matches:
        if m["match_type"] == "subsidiary_name":
            notes = f"IOU subsidiary match: {m['parent']} ({m['subsidiary_name']})"
        else:
            notes = f"IOU pattern match: {m['parent']}"
        try:
            log_discovery(
                pwsid=m["pwsid"],
                url=m["url"],
                url_source="state_directory",
                discovery_query=None,
                notes=notes,
            )
            urls_written_registry += 1
        except Exception as e:
            logger.warning(f"IOU registry write failed for {m['pwsid']}: {e}")

    logger.info(f"IOU mapper: wrote {urls_written_registry} entries to scrape_registry")

    # --- Write per-state YAML files ---
    urls_written_yaml = 0
    if write_yaml:
        config_dir = PROJECT_ROOT / "config"
        config_dir.mkdir(parents=True, exist_ok=True)

        # Group matches by state
        state_groups: dict[str, list] = defaultdict(list)
        for m in matches:
            state_groups[m["state_code"]].append(m)

        today = date.today().isoformat()
        for state_code, state_matches in sorted(state_groups.items()):
            yaml_path = config_dir / f"rate_urls_{state_code.lower()}_iou.yaml"

            lines = [
                f"# Investor-owned water utility URL mappings — {state_code}",
                "# Source: iou_pattern_match against sdwis_systems",
                f"# Generated: {today}",
                f"# Total matches: {len(state_matches)}",
                "",
            ]

            # Group by parent within state
            parent_groups: dict[str, list] = defaultdict(list)
            for m in state_matches:
                parent_groups[m["parent"]].append(m)

            for parent in sorted(parent_groups.keys()):
                pmatches = sorted(parent_groups[parent], key=lambda x: -x["population"])
                lines.append(f"# {parent} ({len(pmatches)} systems)")
                for m in pmatches:
                    lines.append(f'# {m["pws_name"]} (pop {m["population"]:,})')
                    lines.append(f'{m["pwsid"]}: "{m["url"]}"')
                lines.append("")

            yaml_path.write_text("\n".join(lines), encoding="utf-8")
            urls_written_yaml += len(state_matches)
            logger.info(f"IOU mapper: wrote {yaml_path.name} ({len(state_matches)} entries)")

    return {
        "total_matched": len(matches),
        "urls_written_registry": urls_written_registry,
        "urls_written_yaml": urls_written_yaml,
        "by_parent": dict(by_parent),
        "by_state": dict(by_state),
    }

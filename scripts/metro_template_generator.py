#!/usr/bin/env python3
"""
Metro Template Generator

Purpose:
    Reads a metro config entry from metro_targets.yaml, queries SDWIS for all
    CWS in the target counties, filters out already-covered PWSIDs, and
    produces structured context for the Research Agent.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - sqlalchemy
    - utility_api (local package)

Usage:
    from scripts.metro_template_generator import generate_metro_context
    from utility_api.db import get_session
    context = generate_metro_context(metro_config, get_session())

Notes:
    - Queries cws_boundaries for county matching, sdwis_systems for city matching
    - Filters out PWSIDs with existing rate data or pending URLs in scrape_registry
    - Tiers utilities by population: large (>=50K), medium (10-50K), small (1-10K)
    - Default population threshold: 1,000 (skips very small systems)

Data Sources:
    - utility.sdwis_systems (system attributes)
    - utility.cws_boundaries (county_served)
    - utility.pwsid_coverage (rate data status)
    - utility.scrape_registry (pending URL status)
"""

import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema


def _get_pending_pwsids(conn) -> set[str]:
    """Get PWSIDs that already have pending or active URLs in scrape_registry."""
    result = conn.execute(
        text(f"""
            SELECT DISTINCT pwsid
            FROM {schema}.scrape_registry
            WHERE status IN ('pending', 'active', 'pending_retry')
            AND pwsid IS NOT NULL
        """)
    )
    return {row.pwsid for row in result}


def generate_metro_context(
    metro_config: dict,
    pop_threshold: int = 1000,
) -> dict:
    """Generate structured context for the Research Agent.

    Args:
        metro_config: A single metro entry from metro_targets.yaml.
        pop_threshold: Minimum population to include (default 1,000).

    Returns:
        Dict with metro_name, metro_id, states, utilities list, and stats.
    """
    # Flatten county pairs: [(state, county), ...]
    counties_flat = []
    for state, county_list in metro_config["counties"].items():
        for county in county_list:
            counties_flat.append((state, county))

    # Anchor cities — SDWIS stores city in UPPER CASE
    anchor_cities = [c.upper() for c in metro_config.get("anchor_cities", [])]

    with engine.connect() as conn:
        # Build dynamic IN clause for (state_code, county_served) pairs
        county_conditions = []
        params: dict = {"pop_threshold": pop_threshold}

        for i, (state, county) in enumerate(counties_flat):
            county_conditions.append(
                f"(cb.state_code = :st_{i} AND cb.county_served = :co_{i})"
            )
            params[f"st_{i}"] = state
            params[f"co_{i}"] = county

        county_clause = " OR ".join(county_conditions) if county_conditions else "FALSE"

        # Build anchor city IN clause
        city_conditions = []
        for i, city in enumerate(anchor_cities):
            city_conditions.append(f":city_{i}")
            params[f"city_{i}"] = city
        city_clause = ", ".join(city_conditions) if city_conditions else "'__NONE__'"

        # Also match by state codes for city matching
        state_codes = list(metro_config["counties"].keys())
        state_conditions = []
        for i, st in enumerate(state_codes):
            state_conditions.append(f":state_{i}")
            params[f"state_{i}"] = st
        state_clause = ", ".join(state_conditions) if state_conditions else "'__NONE__'"

        query = f"""
            SELECT DISTINCT ON (s.pwsid)
                s.pwsid, s.pws_name, s.state_code, s.city,
                s.population_served_count, s.owner_type_code,
                cb.county_served,
                pc.has_rate_data, pc.scrape_status
            FROM {schema}.sdwis_systems s
            LEFT JOIN {schema}.cws_boundaries cb ON cb.pwsid = s.pwsid
            LEFT JOIN {schema}.pwsid_coverage pc ON pc.pwsid = s.pwsid
            WHERE s.pws_type_code = 'CWS'
            AND (s.population_served_count >= :pop_threshold
                 OR s.population_served_count IS NULL)
            AND (
                ({county_clause})
                OR (s.city IN ({city_clause}) AND s.state_code IN ({state_clause}))
            )
            ORDER BY s.pwsid, s.population_served_count DESC
        """

        all_systems = conn.execute(text(query), params).fetchall()

        # Get PWSIDs with pending URLs
        pending_pwsids = _get_pending_pwsids(conn)

    # Split into buckets
    already_covered = []
    has_pending_url = []
    needs_url = []

    for s in all_systems:
        if s.has_rate_data:
            already_covered.append(s)
        elif s.pwsid in pending_pwsids:
            has_pending_url.append(s)
        else:
            needs_url.append(s)

    # Tier by population and serialize
    utilities = []
    for s in needs_url:
        pop = s.population_served_count or 0
        if pop >= 50000:
            tier = "large"
        elif pop >= 10000:
            tier = "medium"
        else:
            tier = "small"

        utilities.append({
            "pwsid": s.pwsid,
            "pws_name": s.pws_name or "",
            "city": s.city or "",
            "county": s.county_served or "",
            "state": s.state_code or "",
            "population": pop,
            "owner_type": s.owner_type_code or "",
            "tier": tier,
        })

    # Sort: large first (highest value per research API call)
    utilities.sort(key=lambda u: u["population"], reverse=True)

    stats = {
        "total_cws_in_area": len(all_systems),
        "already_covered": len(already_covered),
        "has_pending_url": len(has_pending_url),
        "needs_url": len(needs_url),
        "large": sum(1 for u in utilities if u["tier"] == "large"),
        "medium": sum(1 for u in utilities if u["tier"] == "medium"),
        "small": sum(1 for u in utilities if u["tier"] == "small"),
    }

    logger.info(
        f"Metro context for {metro_config['name']}: "
        f"{stats['total_cws_in_area']} CWS total, "
        f"{stats['already_covered']} covered, "
        f"{stats['has_pending_url']} pending, "
        f"{stats['needs_url']} need research "
        f"(L:{stats['large']} M:{stats['medium']} S:{stats['small']})"
    )

    return {
        "metro_name": metro_config["name"],
        "metro_id": metro_config["id"],
        "states": metro_config["states"],
        "utilities": utilities,
        "stats": stats,
    }


if __name__ == "__main__":
    import json

    import yaml

    # Quick test: load Portland and show context
    config_path = PROJECT_ROOT / "config" / "metro_targets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    metro_id = sys.argv[1] if len(sys.argv) > 1 else "portland"
    metro = next((m for m in config["metros"] if m["id"] == metro_id), None)

    if not metro:
        print(f"Metro '{metro_id}' not found in config")
        sys.exit(1)

    context = generate_metro_context(metro)
    print(json.dumps(context["stats"], indent=2))
    print(f"\nFirst 10 utilities needing research:")
    for u in context["utilities"][:10]:
        print(
            f"  {u['pwsid']} | {u['pws_name']:40s} | "
            f"pop {u['population']:>10,} | {u['tier']}"
        )

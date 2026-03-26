#!/usr/bin/env python3
"""
Export CWS Boundaries + Rate Data as GeoJSON for Dashboard

Purpose:
    Produces two static files for the UAPI Rate Explorer dashboard:
    1. cws_rates.geojson — all 44,643 CWS polygons with rate properties
    2. coverage_stats.json — pre-computed summary statistics

    Joins cws_boundaries (polygons) with sdwis_systems (metadata),
    rate_best_estimate (bill amounts), rate_schedules (tier detail),
    duke_reference_rates (coverage flag), and source_catalog (source names).

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - sqlalchemy
    - psycopg
    - loguru

Usage:
    python scripts/export_dashboard_data.py                    # Default: simplified
    python scripts/export_dashboard_data.py --full-resolution  # No simplification
    python scripts/export_dashboard_data.py --tolerance 0.002  # Custom simplification

Notes:
    - Geometry simplification with ST_Simplify(geom, 0.001) reduces file from
      ~846 MB to ~49 MB — acceptable for direct Maplibre loading.
    - If file exceeds 50 MB, consider PMTiles conversion via tippecanoe.
    - Properties are flat (no nested objects) for Maplibre style expression compat.
    - Duke reference rate VALUES are never exported — only the coverage flag.
    - rate_schedules tier detail (volumetric_tiers JSONB) is serialized as a
      JSON string in properties for the detail panel to parse.

Data Sources:
    - Input: utility.cws_boundaries, utility.sdwis_systems,
             utility.rate_best_estimate, utility.rate_schedules,
             utility.duke_reference_rates, utility.source_catalog
    - Output: dashboard/public/data/cws_rates.geojson
              dashboard/public/data/coverage_stats.json
"""

# Standard library imports
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

# Third-party imports
from loguru import logger
from sqlalchemy import text

# Local imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.db import engine  # noqa: E402

# Constants
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "dashboard" / "public" / "data"
DEFAULT_TOLERANCE = 0.002  # ST_Simplify tolerance in degrees (~222m at equator)


def build_source_lookup(conn) -> dict:
    """Build source_key → {display_name, tier} lookup from source_catalog."""
    result = conn.execute(text(
        "SELECT source_key, display_name, tier FROM source_catalog"
    ))
    lookup = {}
    for row in result:
        lookup[row[0]] = {
            "display_name": row[1],
            "tier": row[2],
        }
    return lookup


def export_geojson(conn, output_path: Path, tolerance: float | None) -> dict:
    """
    Export CWS polygons + rate data as GeoJSON FeatureCollection.

    Returns summary statistics dict for coverage_stats.json.

    Args:
        conn: SQLAlchemy connection
        output_path: Where to write the GeoJSON file
        tolerance: ST_Simplify tolerance (None for full resolution)
    """
    source_lookup = build_source_lookup(conn)

    # Build geometry expression — simplified or full
    if tolerance is not None:
        geom_expr = f"ST_AsGeoJSON(ST_Simplify(cb.geom, {tolerance}))"
        logger.info(f"Using simplified geometry (tolerance={tolerance})")
    else:
        geom_expr = "ST_AsGeoJSON(cb.geom)"
        logger.info("Using full-resolution geometry")

    # Main query: join all tables, one row per CWS boundary
    # Uses rate_best_estimate as the primary rate source (already source-prioritized).
    # Falls back to rate_schedules for tier detail (volumetric_tiers, fixed_charges).
    # Duke reference is only used for the has_reference_only flag.
    query = text(f"""
        SELECT
            {geom_expr}::json as geometry,
            cb.pwsid,
            COALESCE(s.pws_name, cb.pws_name) as pws_name,
            COALESCE(s.state_code, cb.state_code) as state,
            cb.county_served as county,
            s.city,
            COALESCE(s.population_served_count, cb.population_served) as population_served,
            s.owner_type_code as owner_type,

            -- Rate data from best_estimate
            rbe.bill_5ccf,
            rbe.bill_10ccf,
            rbe.bill_estimate_10ccf,
            rbe.fixed_charge_monthly as fixed_charge,
            rbe.rate_structure_type,
            rbe.confidence,
            rbe.selected_source as source_key,
            rbe.rate_effective_date,

            -- Tier detail from rate_schedules (best matching record)
            rs.volumetric_tiers,
            rs.fixed_charges as fixed_charges_detail,
            rs.tier_count,
            rs.bill_20ccf,

            -- Duke reference flag
            CASE
                WHEN dr.pwsid IS NOT NULL AND rbe.pwsid IS NULL THEN true
                ELSE false
            END as has_reference_only

        FROM cws_boundaries cb
        LEFT JOIN sdwis_systems s ON s.pwsid = cb.pwsid
        LEFT JOIN rate_best_estimate rbe ON rbe.pwsid = cb.pwsid
        LEFT JOIN LATERAL (
            SELECT rs2.volumetric_tiers, rs2.fixed_charges, rs2.tier_count, rs2.bill_20ccf
            FROM rate_schedules rs2
            WHERE rs2.pwsid = cb.pwsid
              AND rs2.customer_class = 'residential'
            ORDER BY rs2.vintage_date DESC NULLS LAST
            LIMIT 1
        ) rs ON true
        LEFT JOIN LATERAL (
            SELECT dr2.pwsid
            FROM duke_reference_rates dr2
            WHERE dr2.pwsid = cb.pwsid
            LIMIT 1
        ) dr ON true

        ORDER BY cb.pwsid
    """)

    logger.info("Executing main export query...")
    result = conn.execute(query)

    # Build GeoJSON feature collection
    features = []
    stats = {
        "total_cws": 0,
        "with_rate_data": 0,
        "with_reference_only": 0,
        "no_data": 0,
        "population_total": 0,
        "population_covered": 0,
        "population_reference": 0,
        "by_state": {},
        "by_source": {},
    }

    for row in result:
        stats["total_cws"] += 1
        pop = row.population_served or 0

        has_rate_data = row.bill_10ccf is not None or row.bill_estimate_10ccf is not None
        has_reference_only = bool(row.has_reference_only)

        # Accumulate stats
        stats["population_total"] += pop
        state = row.state or "XX"

        if state not in stats["by_state"]:
            stats["by_state"][state] = {
                "state": state,
                "total": 0,
                "covered": 0,
                "reference_only": 0,
                "pop_total": 0,
                "pop_covered": 0,
            }
        st = stats["by_state"][state]
        st["total"] += 1
        st["pop_total"] += pop

        if has_rate_data:
            stats["with_rate_data"] += 1
            stats["population_covered"] += pop
            st["covered"] += 1
            st["pop_covered"] += pop

            # Track sources
            src = row.source_key or "unknown"
            # Map source_key to display group
            src_display = source_lookup.get(src, {}).get("display_name", src)
            stats["by_source"][src_display] = stats["by_source"].get(src_display, 0) + 1
        elif has_reference_only:
            stats["with_reference_only"] += 1
            stats["population_reference"] += pop
            st["reference_only"] += 1
        else:
            stats["no_data"] += 1

        # Determine the best bill_10ccf value
        bill_10 = row.bill_10ccf
        if bill_10 is None:
            bill_10 = row.bill_estimate_10ccf

        # Source metadata from catalog
        source_meta = source_lookup.get(row.source_key or "", {})

        # Build flat properties
        properties = {
            "pwsid": row.pwsid,
            "pws_name": row.pws_name,
            "state": row.state,
            "county": row.county,
            "city": row.city,
            "population_served": row.population_served,
            "owner_type": row.owner_type,
            "has_rate_data": has_rate_data,
            "source_key": row.source_key,
            "source_name": source_meta.get("display_name"),
            "source_tier": source_meta.get("tier"),
            "data_vintage": str(row.rate_effective_date) if row.rate_effective_date else None,
            "bill_5ccf": _round(row.bill_5ccf),
            "bill_10ccf": _round(bill_10),
            "bill_20ccf": _round(row.bill_20ccf),
            "fixed_charge": _round(row.fixed_charge),
            "rate_structure_type": row.rate_structure_type,
            "tier_count": row.tier_count,
            "confidence": row.confidence,
            "has_reference_only": has_reference_only,
        }

        # Serialize tier detail as JSON string for detail panel
        if row.volumetric_tiers is not None:
            properties["volumetric_tiers_json"] = json.dumps(row.volumetric_tiers)
        if row.fixed_charges_detail is not None:
            properties["fixed_charges_json"] = json.dumps(row.fixed_charges_detail)

        feature = {
            "type": "Feature",
            "geometry": row.geometry,
            "properties": properties,
        }
        features.append(feature)

        if stats["total_cws"] % 10000 == 0:
            logger.info(f"  Processed {stats['total_cws']} features...")

    collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    # Write GeoJSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing {len(features)} features to {output_path}...")
    with open(output_path, "w") as f:
        json.dump(collection, f, separators=(",", ":"))

    file_size_mb = output_path.stat().st_size / 1024 / 1024
    logger.info(f"GeoJSON written: {file_size_mb:.1f} MB")

    if file_size_mb > 50:
        logger.warning(
            f"File size ({file_size_mb:.0f} MB) exceeds 50 MB threshold. "
            "Consider PMTiles conversion via tippecanoe."
        )

    return stats


def build_coverage_stats(stats: dict) -> dict:
    """
    Transform raw stats accumulator into the coverage_stats.json format.

    Args:
        stats: Raw accumulator from export_geojson()

    Returns:
        Structured dict for coverage_stats.json
    """
    total = stats["total_cws"]
    covered = stats["with_rate_data"]
    ref_only = stats["with_reference_only"]
    pop_total = stats["population_total"]
    pop_covered = stats["population_covered"]

    # State breakdown, sorted by coverage percentage descending
    by_state = []
    for st in stats["by_state"].values():
        pct = round(100 * st["covered"] / st["total"], 1) if st["total"] > 0 else 0
        pop_pct = round(100 * st["pop_covered"] / st["pop_total"], 1) if st["pop_total"] > 0 else 0
        by_state.append({
            "state": st["state"],
            "total": st["total"],
            "covered": st["covered"],
            "reference_only": st["reference_only"],
            "pct": pct,
            "pop_total": st["pop_total"],
            "pop_covered": st["pop_covered"],
            "pop_pct": pop_pct,
        })
    by_state.sort(key=lambda x: x["pop_pct"], reverse=True)

    # Source breakdown, sorted by count descending
    by_source = [
        {"source": name, "pwsids": count}
        for name, count in sorted(stats["by_source"].items(), key=lambda x: -x[1])
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_cws": total,
        "with_rate_data": covered,
        "with_reference_only": ref_only,
        "no_data": stats["no_data"],
        "pct_covered": round(100 * covered / total, 1) if total > 0 else 0,
        "population_total": pop_total,
        "population_covered": pop_covered,
        "pct_population": round(100 * pop_covered / pop_total, 1) if pop_total > 0 else 0,
        "by_state": by_state,
        "by_source": by_source,
    }


def _round(val, decimals=2):
    """Round a numeric value, returning None for None/NaN/Inf."""
    if val is None:
        return None
    fval = float(val)
    if math.isnan(fval) or math.isinf(fval):
        return None
    return round(fval, decimals)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Export CWS boundaries + rate data as GeoJSON for dashboard."
    )
    parser.add_argument(
        "--full-resolution",
        action="store_true",
        help="Export full-resolution geometry (warning: ~846 MB)",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help=f"ST_Simplify tolerance in degrees (default: {DEFAULT_TOLERANCE})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main():
    """Main entry point for dashboard data export."""
    args = parse_args()
    output_dir = Path(args.output)
    tolerance = None if args.full_resolution else args.tolerance

    geojson_path = output_dir / "cws_rates.geojson"
    stats_path = output_dir / "coverage_stats.json"

    logger.info("Starting dashboard data export...")
    logger.info(f"Output directory: {output_dir}")

    with engine.connect() as conn:
        # Export GeoJSON
        raw_stats = export_geojson(conn, geojson_path, tolerance)

        # Build and write coverage stats
        coverage = build_coverage_stats(raw_stats)
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        with open(stats_path, "w") as f:
            json.dump(coverage, f, indent=2)
        logger.info(f"Coverage stats written to {stats_path}")

        # Print summary
        logger.info("--- Export Summary ---")
        logger.info(f"Total CWS:           {coverage['total_cws']:,}")
        logger.info(f"With rate data:      {coverage['with_rate_data']:,} ({coverage['pct_covered']}%)")
        logger.info(f"Reference only:      {coverage['with_reference_only']:,}")
        logger.info(f"No data:             {coverage['no_data']:,}")
        logger.info(f"Population coverage: {coverage['pct_population']}%")


if __name__ == "__main__":
    main()

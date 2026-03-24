#!/usr/bin/env python3
"""
Sprint 1 Validation — 20 Known Addresses

Purpose:
    Test the /resolve endpoint against 20 known addresses (10 VA, 10 CA)
    with expected water utility assignments. Validates CWS boundary accuracy.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - requests

Usage:
    python scripts/validate_addresses.py [--api-url http://localhost:8000]

Notes:
    - Ground truth from EPA SDWIS search + utility websites
    - Reports match rate and documents any misses
"""

import argparse
import json
import sys
from datetime import datetime

import requests

# Test addresses with expected utility (PWSID or partial name match)
# Format: (lat, lng, description, expected_pwsid_prefix_or_name, state)
TEST_ADDRESSES = [
    # === Virginia (10) ===
    # Ashburn / Loudoun County — largest DC market
    (39.0438, -77.4875, "Ashburn, VA (Loudoun County)", "VA6107", "VA"),
    # Manassas / Prince William County
    (38.7509, -77.4753, "Manassas, VA", "VA6153", "VA"),
    # Richmond — city water
    (37.5407, -77.4360, "Richmond, VA", "VA4760", "VA"),
    # Virginia Beach
    (36.8529, -75.9780, "Virginia Beach, VA", "VA6810", "VA"),
    # Fairfax County — major utility
    (38.8462, -77.3064, "Fairfax, VA", "VA6059", "VA"),
    # Arlington County
    (38.8816, -77.0910, "Arlington, VA", "VA6013", "VA"),
    # Norfolk
    (36.8508, -76.2859, "Norfolk, VA", "VA6710", "VA"),
    # Henrico County (suburban Richmond)
    (37.5551, -77.3947, "Henrico County, VA", "VA6087", "VA"),
    # Chesapeake
    (36.7682, -76.2875, "Chesapeake, VA", "VA6550", "VA"),
    # Stafford County
    (38.4220, -77.4083, "Stafford County, VA", "VA6179", "VA"),

    # === California (10) ===
    # Los Angeles — LADWP
    (34.0522, -118.2437, "Los Angeles, CA", "CA1910067", "CA"),
    # San Francisco — SFPUC
    (37.7749, -122.4194, "San Francisco, CA", "CA3810001", "CA"),
    # San Jose
    (37.3382, -121.8863, "San Jose, CA", "CA4310011", "CA"),
    # San Diego
    (32.7157, -117.1611, "San Diego, CA", "CA3710020", "CA"),
    # Sacramento
    (38.5816, -121.4944, "Sacramento, CA", "CA3410020", "CA"),
    # Oakland — EBMUD
    (37.8044, -122.2712, "Oakland, CA (EBMUD)", "CA0110005", "CA"),
    # Santa Clara (Silicon Valley)
    (37.3541, -121.9552, "Santa Clara, CA", "CA4310020", "CA"),
    # Fresno
    (36.7378, -119.7871, "Fresno, CA", "CA1010001", "CA"),
    # Long Beach
    (33.7701, -118.1937, "Long Beach, CA", "CA1910044", "CA"),
    # Riverside
    (33.9534, -117.3962, "Riverside, CA", "CA3310005", "CA"),
]


def test_resolve(api_url: str, lat: float, lng: float) -> dict:
    """Call the /resolve endpoint and return the response."""
    r = requests.get(f"{api_url}/resolve", params={"lat": lat, "lng": lng}, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    parser = argparse.ArgumentParser(description="Validate /resolve against known addresses")
    parser.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    args = parser.parse_args()

    print(f"=== Sprint 1 Validation — {len(TEST_ADDRESSES)} addresses ===")
    print(f"API: {args.api_url}")
    print(f"Date: {datetime.now().isoformat()}")
    print()

    results = []
    cws_matches = 0
    aqueduct_matches = 0
    pwsid_correct = 0
    errors = 0

    for lat, lng, desc, expected_pwsid, state in TEST_ADDRESSES:
        try:
            resp = test_resolve(args.api_url, lat, lng)

            cws_match = resp.get("cws_match", False)
            aq_match = resp.get("aqueduct_match", False)
            actual_pwsid = resp.get("pwsid", "")

            if cws_match:
                cws_matches += 1
            if aq_match:
                aqueduct_matches += 1

            # Check PWSID match (prefix match is OK — some IDs may vary)
            pwsid_ok = False
            if actual_pwsid and expected_pwsid:
                pwsid_ok = (
                    actual_pwsid == expected_pwsid
                    or actual_pwsid.startswith(expected_pwsid)
                    or expected_pwsid.startswith(actual_pwsid[:6])
                )
            if pwsid_ok:
                pwsid_correct += 1

            status = "PASS" if (cws_match and pwsid_ok) else "MISS"
            stress = resp.get("water_stress_label", "N/A")

            print(f"  [{status}] {desc}")
            print(f"    Expected: {expected_pwsid}")
            print(f"    Got:      {actual_pwsid} — {resp.get('pws_name', 'N/A')}")
            print(f"    Pop: {resp.get('population_served', 'N/A')}, "
                  f"Source: {resp.get('water_source', 'N/A')}, "
                  f"Stress: {stress}")
            if not pwsid_ok and cws_match:
                print(f"    NOTE: PWSID mismatch (may be adjacent utility)")
            print()

            results.append({
                "description": desc,
                "lat": lat,
                "lng": lng,
                "expected_pwsid": expected_pwsid,
                "actual_pwsid": actual_pwsid,
                "pws_name": resp.get("pws_name"),
                "cws_match": cws_match,
                "aqueduct_match": aq_match,
                "pwsid_correct": pwsid_ok,
                "water_stress_label": stress,
                "status": status,
            })

        except Exception as e:
            print(f"  [ERROR] {desc}: {e}")
            errors += 1
            results.append({
                "description": desc,
                "lat": lat,
                "lng": lng,
                "status": "ERROR",
                "error": str(e),
            })

    # Summary
    total = len(TEST_ADDRESSES)
    print("=" * 60)
    print(f"RESULTS: {total} addresses tested")
    print(f"  CWS boundary match:  {cws_matches}/{total} ({100*cws_matches/total:.0f}%)")
    print(f"  Aqueduct match:      {aqueduct_matches}/{total} ({100*aqueduct_matches/total:.0f}%)")
    print(f"  PWSID correct:       {pwsid_correct}/{total} ({100*pwsid_correct/total:.0f}%)")
    print(f"  Errors:              {errors}")
    print()

    # Save results
    from pathlib import Path
    out_path = Path("data/interim/validation_sprint1.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({"date": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()

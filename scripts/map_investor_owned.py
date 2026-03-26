#!/usr/bin/env python3
"""
map_investor_owned.py
=====================
Maps major investor-owned water utility parent companies to their subsidiary
PWSIDs using SDWIS owner/system name patterns, and generates YAML URL mappings.

The major IOUs have well-known corporate websites with rate pages per service area.
This script searches the SDWIS master CSV for systems whose names match known
subsidiary naming patterns.

Usage:
    python download_sdwis_master.py          # First, get the data
    python map_investor_owned.py             # Then, map IOUs

Output:
    config/directory_urls_iou_matched.yaml   — All matched IOU systems with URLs
    data/iou_matches.csv                     — Detailed match data for review
"""

import csv
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

# --- Investor-Owned Utility Definitions ---
# Each entry: name patterns to match in PWS_NAME, the base URL, and rate page patterns
INVESTOR_OWNED = [
    {
        "parent": "American Water Works",
        "patterns": [
            r"(?i)american water",
            r"(?i)am\.\s*water",
        ],
        "base_url": "https://amwater.com",
        "rate_url_template": "https://www.{state_lower}amwater.com/customer-service/rates",
        "state_url_map": {
            "NJ": "https://www.newjerseyamwater.com/customer-service/rates",
            "PA": "https://www.pennsylvaniaamwater.com/customer-service/rates",
            "IN": "https://www.indianaamwater.com/customer-service/rates",
            "WV": "https://www.westvirginiaamwater.com/customer-service/rates",
            "IL": "https://www.illinoisamwater.com/customer-service/rates",
            "CA": "https://www.californiaamwater.com/customer-service/rates",
            "MO": "https://www.missouriamwater.com/customer-service/rates",
            "VA": "https://www.virginiaamwater.com/customer-service/rates",
            "IA": "https://www.iowaamericanwater.com/customer-service/rates",
            "TN": "https://www.tennesseeamwater.com/customer-service/rates",
            "MD": "https://www.marylandamwater.com/customer-service/rates",
            "GA": "https://www.georgiaamwater.com/customer-service/rates",
            "KY": "https://www.kentuckyamwater.com/customer-service/rates",
            "HI": "https://www.hawaiiamwater.com/customer-service/rates",
        }
    },
    {
        "parent": "Aqua / Essential Utilities",
        "patterns": [
            r"(?i)aqua\s+(america|pennsylvania|ohio|north\s+carolina|texas|illinois|indiana|virginia|new\s+jersey)",
            r"(?i)aqua\s+water\s+supply",
            r"(?i)essential\s+utilities",
            r"(?i)peoples\s+gas",  # Essential subsidiary
        ],
        "base_url": "https://www.essential.co",
        "state_url_map": {
            "PA": "https://www.aquawater.com/pa/rates",
            "OH": "https://www.aquawater.com/oh/rates",
            "NC": "https://www.aquawater.com/nc/rates",
            "TX": "https://www.aquawater.com/tx/rates",
            "IL": "https://www.aquawater.com/il/rates",
            "IN": "https://www.aquawater.com/in/rates",
            "VA": "https://www.aquawater.com/va/rates",
            "NJ": "https://www.aquawater.com/nj/rates",
        }
    },
    {
        "parent": "California Water Service",
        "patterns": [
            r"(?i)california\s+water\s+service",
            r"(?i)cal\s*water",
            r"(?i)hawaii\s+water\s+service",
            r"(?i)washington\s+water\s+service",
            r"(?i)new\s+mexico\s+water\s+service",
        ],
        "base_url": "https://www.calwater.com",
        "state_url_map": {
            "CA": "https://www.calwater.com/rates/",
            "HI": "https://www.hawaiiwaterservice.com/rates/",
            "WA": "https://www.washingtonwaterservice.com/rates/",
            "NM": "https://www.newmexicowaterservice.com/rates/",
        }
    },
    {
        "parent": "SJW Group / San Jose Water",
        "patterns": [
            r"(?i)san\s+jose\s+water",
            r"(?i)sjw\s+",
            r"(?i)sjwtx",
            r"(?i)connecticut\s+water\s+co",
            r"(?i)maine\s+water\s+company",
        ],
        "base_url": "https://www.sjwater.com",
        "state_url_map": {
            "CA": "https://www.sjwater.com/customer-care/rate-information",
            "TX": "https://www.sjwtx.com/customer-care/rate-information",
            "CT": "https://www.ctwater.com/rates",
            "ME": "https://www.mainewater.com/rates",
        }
    },
    {
        "parent": "Middlesex Water Company",
        "patterns": [
            r"(?i)middlesex\s+water",
            r"(?i)tidewater\s+utilities",
            r"(?i)tidewater\s+environmental\s+services",
        ],
        "base_url": "https://www.middlesexwater.com",
        "state_url_map": {
            "NJ": "https://www.middlesexwater.com/customer-service/rates-and-tariff",
            "DE": "https://www.tidewaterutilities.com/rates/",
        }
    },
    {
        "parent": "Artesian Resources",
        "patterns": [
            r"(?i)artesian\s+water",
            r"(?i)artesian\s+resources",
        ],
        "base_url": "https://www.artesianwater.com",
        "state_url_map": {
            "DE": "https://www.artesianwater.com/customer-service/rates",
            "MD": "https://www.artesianwater.com/customer-service/rates",
            "PA": "https://www.artesianwater.com/customer-service/rates",
        }
    },
    {
        "parent": "Aquarion Water / Eversource",
        "patterns": [
            r"(?i)aquarion\s+water",
        ],
        "base_url": "https://www.aquarionwater.com",
        "state_url_map": {
            "CT": "https://www.aquarionwater.com/customer-service/rates-billing",
            "MA": "https://www.aquarionwater.com/customer-service/rates-billing",
            "NH": "https://www.aquarionwater.com/customer-service/rates-billing",
        }
    },
    {
        "parent": "Central States Water Resources",
        "patterns": [
            r"(?i)central\s+states\s+water",
            r"(?i)cswr\b",
        ],
        "base_url": "https://centralstateswater.com",
        "state_url_map": {}  # CSWR has many small systems; base URL is best we can do
    },
    {
        "parent": "Nexus Water Group (formerly Corix/GWRI)",
        "patterns": [
            r"(?i)nexus\s+water",
            r"(?i)corix\s+",
            r"(?i)global\s+water\s+resources",
        ],
        "base_url": "https://nexuswatergroup.com",
        "state_url_map": {}
    },
]


def match_system(pws_name: str, state: str) -> tuple[str, str] | None:
    """
    Try to match a PWS name against known IOU patterns.
    Returns (parent_name, url) or None.
    """
    for iou in INVESTOR_OWNED:
        for pattern in iou["patterns"]:
            if re.search(pattern, pws_name):
                # Determine the best URL for this state
                url = iou["state_url_map"].get(state)
                if not url:
                    url = iou.get("base_url", "")
                return (iou["parent"], url)
    return None


def main():
    master_path = Path("data/sdwis_cws_master.csv")
    if not master_path.exists():
        # Try pop1000+ file
        master_path = Path("data/sdwis_cws_pop1000plus.csv")
    if not master_path.exists():
        print("ERROR: No SDWIS master CSV found. Run download_sdwis_master.py first.")
        sys.exit(1)

    with open(master_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)

    print(f"Scanning {len(records)} CWS records for investor-owned utility matches...")

    matches = []
    by_parent = defaultdict(int)
    by_state = defaultdict(int)

    for record in records:
        pwsid = record.get("pwsid", "").strip()
        name = record.get("pws_name", "").strip()
        state = record.get("state_code", pwsid[:2] if len(pwsid) >= 2 else "")
        pop = int(record.get("population_served", 0) or 0)

        result = match_system(name, state)
        if result:
            parent, url = result
            matches.append({
                "pwsid": pwsid,
                "pws_name": name,
                "state_code": state,
                "population": pop,
                "parent_company": parent,
                "url": url,
            })
            by_parent[parent] += 1
            by_state[state] += 1

    print(f"\nMatched {len(matches)} systems to investor-owned utilities")
    print("\nBy parent company:")
    for parent, count in sorted(by_parent.items(), key=lambda x: -x[1]):
        print(f"  {parent}: {count}")
    print("\nBy state:")
    for state, count in sorted(by_state.items()):
        print(f"  {state}: {count}")

    # Write detailed CSV
    csv_path = Path("data/iou_matches.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if matches:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=matches[0].keys())
            writer.writeheader()
            writer.writerows(matches)
        print(f"\nDetailed matches: {csv_path}")

    # Write YAML (only entries with valid URLs)
    valid_matches = [m for m in matches if m["url"].startswith("http")]
    if valid_matches:
        yaml_path = Path("config/directory_urls_iou_matched.yaml")
        yaml_path.parent.mkdir(parents=True, exist_ok=True)

        today = date.today().isoformat()
        lines = [
            "# Investor-owned water utility URL mappings",
            "# Source: iou_name_matching against SDWIS data",
            f"# Generated: {today}",
            f"# Total matches: {len(valid_matches)}",
            "",
        ]

        # Group by state
        state_groups = defaultdict(list)
        for m in valid_matches:
            state_groups[m["state_code"]].append(m)

        for state in sorted(state_groups.keys()):
            lines.append(f"# === {state} ===")
            state_matches = sorted(state_groups[state],
                                   key=lambda m: -m["population"])
            for m in state_matches:
                lines.append(f'# {m["pws_name"]} ({m["parent_company"]}, pop {m["population"]})')
                lines.append(f'{m["pwsid"]}: "{m["url"]}"')
                lines.append("")

        with open(yaml_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"YAML output: {yaml_path}")


if __name__ == "__main__":
    main()

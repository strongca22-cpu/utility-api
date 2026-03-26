#!/usr/bin/env python3
"""
mine_ccr_links.py
=================
Mines the EPA Consumer Confidence Report (CCR) search tool for utility-hosted
CCR URLs. Many utilities host their CCR reports on their own websites, so the
CCR URL's domain often reveals the utility's website.

The EPA CCR search is at: https://sdwis.epa.gov/fylccr
Utilities can add their CCR links via: https://sdwis.epa.gov/ccriwriter

This script:
1. Queries the CCR search by state
2. Extracts CCR URLs that point to utility websites (not EPA-hosted)
3. Strips the CCR path to get the base utility website URL
4. Outputs PWSID → URL mappings in YAML format

Usage:
    pip install requests beautifulsoup4
    python mine_ccr_links.py --state VA
    python mine_ccr_links.py --all-states

Note: The EPA CCR search uses Oracle APEX forms. This script may need
adjustment based on the actual response format. Test with a single state first.

Output:
    config/directory_urls_{state}_ccr.yaml   — CCR-derived utility URLs
    data/ccr_raw_{state}.json                — Raw CCR search results
"""

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Required libraries not installed.")
    print("Install with: pip install requests beautifulsoup4")
    sys.exit(1)

# --- Configuration ---
# The CCR search form posts to this endpoint
CCR_SEARCH_URL = "https://sdwis.epa.gov/fylccr"
# Alternative older URL
CCR_SEARCH_ALT = "http://ofmpub.epa.gov/apex/safewater/f?p=ccr_wyl"

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

PRIORITY_1 = ["VA", "TX", "CA", "OR", "GA", "NC", "OH", "IL", "AZ", "NV", "IA", "SC", "WA"]


def extract_base_url(ccr_url: str) -> str | None:
    """
    Extract the base utility website URL from a CCR document URL.
    
    Example:
        Input:  https://www.fairfaxwater.org/sites/default/files/ccr2023.pdf
        Output: https://www.fairfaxwater.org
        
        Input:  https://www.staffordcountyva.gov/utilities/water-quality-report
        Output: https://www.staffordcountyva.gov
    
    Returns None if the URL is EPA-hosted or otherwise not useful.
    """
    try:
        parsed = urlparse(ccr_url)
        domain = parsed.hostname
        if not domain:
            return None
        
        # Skip EPA-hosted CCRs
        skip_domains = [
            "epa.gov", "ofmpub.epa.gov", "sdwis.epa.gov",
            "safewater.epa.gov", "s3.amazonaws.com",
        ]
        for skip in skip_domains:
            if domain.endswith(skip):
                return None
        
        # Skip common document hosting services
        skip_services = [
            "docs.google.com", "drive.google.com", "dropbox.com",
            "issuu.com", "scribd.com", "yumpu.com",
        ]
        for skip in skip_services:
            if domain.endswith(skip):
                return None
        
        # Return the base URL (scheme + domain)
        return f"{parsed.scheme}://{domain}"
    except Exception:
        return None


def search_ccr_by_state(state_code: str, session: requests.Session) -> list[dict]:
    """
    Search the EPA CCR database for a specific state.
    Returns list of {pwsid, pws_name, ccr_url} dicts.
    
    NOTE: The EPA CCR search uses Oracle APEX which requires session management.
    This function attempts both the APEX form submission and direct URL patterns.
    You may need to adjust based on actual behavior.
    """
    results = []
    
    # Approach 1: Try the newer SDWIS CCR endpoint
    try:
        # First, get the search page to obtain session tokens
        resp = session.get(CCR_SEARCH_URL, timeout=30)
        if resp.status_code != 200:
            print(f"  Warning: CCR search page returned {resp.status_code}")
            return results
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Look for the APEX session info
        # Oracle APEX typically uses hidden form fields for state management
        # This is a simplified approach - may need refinement based on actual form structure
        
        # Try to find the search form and submit it
        forms = soup.find_all("form")
        # ... (APEX form handling would go here)
        
        # Approach 2: Look for CCR links in the page content
        # Many state pages list CCR links directly
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            # Look for links that contain PWSID patterns or CCR keywords
            pwsid_match = re.search(r'([A-Z]{2}\d{7})', text + href)
            if pwsid_match and href.startswith("http"):
                results.append({
                    "pwsid": pwsid_match.group(1),
                    "pws_name": text,
                    "ccr_url": href,
                })
    
    except requests.RequestException as e:
        print(f"  Error searching CCR for {state_code}: {e}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Mine CCR links from EPA search")
    parser.add_argument("--state", type=str, help="Single state code to search")
    parser.add_argument("--all-states", action="store_true", help="Search all states")
    parser.add_argument("--priority-only", action="store_true",
                        help="Search only Priority 1 states")
    parser.add_argument("--output-dir", type=str, default="config")
    parser.add_argument("--data-dir", type=str, default="data")
    args = parser.parse_args()

    if not args.state and not args.all_states and not args.priority_only:
        print("ERROR: Specify --state XX, --all-states, or --priority-only")
        sys.exit(1)

    states = []
    if args.state:
        states = [args.state.upper()]
    elif args.priority_only:
        states = PRIORITY_1
    elif args.all_states:
        states = STATES

    output_dir = Path(args.output_dir)
    data_dir = Path(args.data_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Water-Utility-Research/1.0)"
    })

    all_mappings = []

    for i, state in enumerate(states, 1):
        print(f"[{i}/{len(states)}] Searching CCR links for {state}...")
        results = search_ccr_by_state(state, session)

        if results:
            # Save raw results
            raw_path = data_dir / f"ccr_raw_{state.lower()}.json"
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

            # Extract base URLs
            mappings = []
            for r in results:
                base_url = extract_base_url(r["ccr_url"])
                if base_url:
                    mappings.append({
                        "pwsid": r["pwsid"],
                        "pws_name": r.get("pws_name", ""),
                        "url": base_url,
                        "ccr_url": r["ccr_url"],
                        "source": "epa_ccr_search",
                    })

            if mappings:
                # Write state YAML
                today = date.today().isoformat()
                lines = [
                    f"# {state} utility URLs derived from EPA CCR search",
                    f"# Source: epa_ccr_search",
                    f"# Generated: {today}",
                    f"# Method: Extract base domain from utility-hosted CCR report URLs",
                    "",
                ]
                for m in mappings:
                    lines.append(f'# {m["pws_name"]}')
                    lines.append(f'# CCR: {m["ccr_url"]}')
                    lines.append(f'{m["pwsid"]}: "{m["url"]}"')
                    lines.append("")

                yaml_path = output_dir / f"directory_urls_{state.lower()}_ccr.yaml"
                with open(yaml_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(lines))

                all_mappings.extend(mappings)
                print(f"  Found {len(results)} CCR entries, {len(mappings)} utility URLs")
            else:
                print(f"  Found {len(results)} CCR entries, 0 utility-hosted URLs")
        else:
            print(f"  No results")

        time.sleep(2)  # Be polite

    print(f"\nTotal: {len(all_mappings)} utility URLs from CCR links across {len(states)} states")
    print()
    print("IMPORTANT NOTES:")
    print("================")
    print("The EPA CCR search uses Oracle APEX forms which are difficult to")
    print("scrape programmatically. This script provides the framework but")
    print("may need manual intervention or Selenium/Playwright for full")
    print("automation. Consider these alternatives:")
    print()
    print("1. Manual state-by-state search at https://sdwis.epa.gov/fylccr")
    print("   - Select state, click Search, export results")
    print("   - Look for systems with CCR URLs that point to utility websites")
    print()
    print("2. Check if your state's drinking water program maintains its own")
    print("   CCR link database (many do, e.g., TCEQ CCR Generator for TX)")
    print()
    print("3. Use the 'Add Your CCR Link' tool URL pattern to check specific")
    print("   PWSIDs: https://sdwis.epa.gov/ccriwriter")


if __name__ == "__main__":
    main()

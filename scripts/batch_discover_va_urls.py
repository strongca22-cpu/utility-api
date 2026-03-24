#!/usr/bin/env python3
"""
Batch VA Rate URL Discovery + Verification

Purpose:
    Search SearXNG for rate page URLs for all 31 VA MDWD utilities,
    verify top candidates resolve (HTTP HEAD check), and output a
    curated table for human review before pipeline run.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - httpx
    - beautifulsoup4
    - sqlalchemy
    - pyyaml

Usage:
    python scripts/batch_discover_va_urls.py

Notes:
    - Skips utilities that already have high/medium confidence parses
    - Does NOT skip failed parses (re-discovers them)
    - Verifies URLs with HTTP HEAD before presenting
    - Outputs YAML-ready format for rate_urls_va.yaml
"""

import sys
import time
from pathlib import Path

import httpx
import yaml
from loguru import logger

# Add project to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_discovery import (
    _search_searxng,
    _score_candidate,
    _extract_domain,
    discover_rate_url,
    DiscoveryResult,
    RatePageCandidate,
)
from sqlalchemy import text


# ── Configuration ──────────────────────────────────────────────────────────

SEARCH_DELAY = 2.5  # seconds between SearXNG queries (polite)
HEAD_TIMEOUT = 10    # seconds for HTTP HEAD check
ALREADY_CURATED = {  # Skip these — already have good curated URLs
    "VA6510010",  # Alexandria (high)
    "VA1121052",  # Blacksburg (high)
    "VA6013010",  # Arlington (medium)
}


def get_target_utilities() -> list[dict]:
    """Get all 31 VA MDWD utilities, excluding already-successful ones."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT c.pwsid, c.pws_name, c.county_served, m.population
            FROM {schema}.cws_boundaries c
            JOIN {schema}.mdwd_financials m ON m.pwsid = c.pwsid
            WHERE c.state_code = 'VA'
            ORDER BY m.population DESC NULLS LAST
        """)).fetchall()

    targets = []
    for r in rows:
        pwsid = r[0]
        if pwsid in ALREADY_CURATED:
            continue
        targets.append({
            "pwsid": pwsid,
            "name": r[1],
            "county": r[2],
            "population": r[3],
        })
    return targets


def verify_url(url: str) -> dict:
    """HTTP HEAD check — verify URL actually resolves.

    Returns dict with status_code, content_type, is_pdf, ok.
    """
    result = {"url": url, "status_code": None, "content_type": None, "is_pdf": False, "ok": False, "error": None}

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    try:
        with httpx.Client(headers=headers, timeout=HEAD_TIMEOUT, follow_redirects=True) as client:
            # Try HEAD first (lighter)
            resp = client.head(url)
            # Some servers reject HEAD — fall back to GET with stream
            if resp.status_code == 405:
                resp = client.get(url, follow_redirects=True)

            result["status_code"] = resp.status_code
            ct = resp.headers.get("content-type", "")
            result["content_type"] = ct
            result["is_pdf"] = "pdf" in ct.lower() or url.lower().endswith(".pdf")
            result["ok"] = resp.status_code < 400
    except Exception as e:
        result["error"] = str(e)[:80]

    return result


def search_with_variants(name: str, county: str, state: str = "VA") -> list[RatePageCandidate]:
    """Search with multiple query variants to maximize coverage.

    Tries:
    1. "{name} {county} VA water rates"
    2. "{name} VA water rate schedule PDF"
    3. "{short_name} water rates {county} Virginia"
    """
    all_candidates = {}

    # Variant 1: standard
    q1 = f"{name} {county} {state} water rates"
    results1 = _search_searxng(q1, max_results=8)
    for c in results1:
        all_candidates[c.url] = c

    time.sleep(1.0)

    # Variant 2: PDF-focused
    # Clean name: strip "CITY OF" prefix/suffix, "TOWN OF" etc.
    short = name.replace("CITY OF ", "").replace(", CITY OF", "").replace("TOWN OF ", "").replace(", TOWN OF", "").strip()
    q2 = f"{short} VA water rate schedule PDF"
    results2 = _search_searxng(q2, max_results=8)
    for c in results2:
        if c.url not in all_candidates:
            all_candidates[c.url] = c

    # Deduplicate and re-sort
    combined = list(all_candidates.values())
    combined.sort(key=lambda c: c.score, reverse=True)
    return combined[:10]


def main():
    """Run batch discovery + verification for all VA utilities."""
    targets = get_target_utilities()
    logger.info(f"Discovering URLs for {len(targets)} VA utilities (3 already curated)")

    results = []

    for i, t in enumerate(targets):
        pwsid = t["pwsid"]
        name = t["name"]
        county = t["county"] or ""
        pop = t["population"] or 0

        logger.info(f"[{i+1}/{len(targets)}] {pwsid} — {name} ({county}, pop={pop:,})")

        candidates = search_with_variants(name, county)

        if not candidates or candidates[0].score <= 0:
            logger.warning(f"  → No relevant results")
            results.append({
                "pwsid": pwsid,
                "name": name,
                "county": county,
                "population": pop,
                "best_url": None,
                "best_title": None,
                "best_score": 0,
                "is_pdf": False,
                "http_ok": False,
                "alt_url": None,
                "error": "No relevant search results",
            })
            if i < len(targets) - 1:
                time.sleep(SEARCH_DELAY)
            continue

        # Verify top 3 candidates
        best_verified = None
        alt_verified = None

        for j, cand in enumerate(candidates[:3]):
            vr = verify_url(cand.url)
            logger.info(f"  #{j+1} score={cand.score:.1f} http={vr['status_code']} "
                       f"pdf={vr['is_pdf']} — {cand.url[:80]}")

            if vr["ok"] and best_verified is None:
                best_verified = {
                    "url": cand.url,
                    "title": cand.title,
                    "score": cand.score,
                    "is_pdf": vr["is_pdf"],
                    "status": vr["status_code"],
                }
            elif vr["ok"] and alt_verified is None:
                alt_verified = {
                    "url": cand.url,
                    "title": cand.title,
                    "score": cand.score,
                    "is_pdf": vr["is_pdf"],
                }

        if best_verified:
            results.append({
                "pwsid": pwsid,
                "name": name,
                "county": county,
                "population": pop,
                "best_url": best_verified["url"],
                "best_title": best_verified["title"],
                "best_score": best_verified["score"],
                "is_pdf": best_verified["is_pdf"],
                "http_ok": True,
                "alt_url": alt_verified["url"] if alt_verified else None,
                "error": None,
            })
        else:
            results.append({
                "pwsid": pwsid,
                "name": name,
                "county": county,
                "population": pop,
                "best_url": candidates[0].url if candidates else None,
                "best_title": candidates[0].title if candidates else None,
                "best_score": candidates[0].score if candidates else 0,
                "is_pdf": False,
                "http_ok": False,
                "alt_url": None,
                "error": f"Top candidates failed HTTP check",
            })

        if i < len(targets) - 1:
            time.sleep(SEARCH_DELAY)

    # ── Output Report ──────────────────────────────────────────────────────

    print("\n" + "=" * 100)
    print("VA RATE URL DISCOVERY RESULTS")
    print("=" * 100)

    verified_ok = [r for r in results if r["http_ok"]]
    failed = [r for r in results if not r["http_ok"]]

    print(f"\nVerified: {len(verified_ok)}/{len(results)} | Failed: {len(failed)}/{len(results)}")
    print(f"PDFs: {sum(1 for r in verified_ok if r['is_pdf'])}")

    print("\n── VERIFIED URLs (ready for YAML) ──\n")
    for r in sorted(verified_ok, key=lambda x: x["name"]):
        pdf_tag = " [PDF]" if r["is_pdf"] else " [HTML]"
        print(f"  {r['pwsid']}: {r['name']} ({r['county']})")
        print(f"    URL: {r['best_url']}")
        print(f"    Title: {r['best_title']}")
        print(f"    Score: {r['best_score']:.1f}{pdf_tag}")
        if r["alt_url"]:
            print(f"    Alt: {r['alt_url']}")
        print()

    if failed:
        print("── FAILED (need manual curation) ──\n")
        for r in sorted(failed, key=lambda x: x["name"]):
            print(f"  {r['pwsid']}: {r['name']} ({r['county']}) — {r['error']}")
            if r["best_url"]:
                print(f"    Best unverified: {r['best_url']}")
            print()

    # ── Write YAML candidate file ──────────────────────────────────────────

    yaml_out = Path(__file__).parents[1] / "config" / "rate_urls_va_candidates.yaml"
    yaml_data = {}

    # Include already-curated
    yaml_data["VA6510010"] = "https://www.amwater.com/vaaw/Resources/PDF/VA%20Water%20Tariff.pdf"
    yaml_data["VA1121052"] = "https://www.gbpw.com/sites/default/files/2024-03/BBResidentialRates_0.pdf"

    for r in results:
        if r["http_ok"] and r["best_url"]:
            yaml_data[r["pwsid"]] = r["best_url"]

    with open(yaml_out, "w") as f:
        f.write("# VA Rate URL Candidates — auto-discovered via SearXNG + HTTP verified\n")
        f.write("# Generated: 2026-03-23\n")
        f.write("# REVIEW BEFORE USE: verify URLs point to actual rate schedules\n\n")
        for pwsid, url in sorted(yaml_data.items()):
            # Find the name
            name_match = next((r["name"] for r in results if r["pwsid"] == pwsid), pwsid)
            if pwsid == "VA6510010":
                name_match = "ALEXANDRIA, CITY OF"
            elif pwsid == "VA1121052":
                name_match = "BLACKSBURG, TOWN OF"
            f.write(f"# {name_match}\n")
            f.write(f'{pwsid}: "{url}"\n\n')

    print(f"\n── Candidate YAML written to: {yaml_out}")
    print(f"── {len(yaml_data)} URLs total (3 curated + {len(yaml_data)-2} discovered)")

    return results


if __name__ == "__main__":
    main()

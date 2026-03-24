#!/usr/bin/env python3
"""
Standalone Rate URL Discovery — runs in tmux, no Claude Code needed

Purpose:
    Background discovery script that queries SearXNG for water rate URLs,
    verifies them, and writes results to a YAML file. Designed to run
    independently in a tmux session with throttling to avoid rate limits.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - httpx, pyyaml, sqlalchemy (from utility-api venv)

Usage:
    # In tmux session:
    tmux new -s url_discover
    source .venv/bin/activate
    python scripts/standalone_discover.py --state CA --delay 5.0 --output config/rate_urls_ca_discovered.yaml

    # Can be stopped and resumed — skips already-discovered PWSIDs

Notes:
    - Runs entirely locally (SearXNG + DB queries)
    - No Anthropic API calls — just URL discovery + HTTP verification
    - Supports VPS proxy routing via --proxy flag
    - Writes incrementally (safe to interrupt)
    - 5s default delay is sustainable for long runs (600+ queries/hour)
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from utility_api.config import settings
from utility_api.db import engine
from sqlalchemy import text


# ── Scoring ────────────────────────────────────────────────────────────────

RATE_KEYWORDS = [
    "water rate", "water rates", "rate schedule", "rate structure",
    "water charges", "water tariff", "fee schedule", "water billing",
    "monthly bill", "service charge", "volumetric rate", "ccf",
    "per gallon", "per 1,000", "per hcf",
]

DEPRIORITY_DOMAINS = [
    "wikipedia.org", "facebook.com", "twitter.com", "youtube.com",
    "yelp.com", "bbb.org", "linkedin.com", "indeed.com",
    "zillow.com", "realtor.com", "nextdoor.com",
]


def score_result(url: str, title: str, snippet: str) -> float:
    """Score a search result for rate-page relevance."""
    text_lower = f"{title} {snippet}".lower()
    score = 0.0

    for kw in RATE_KEYWORDS:
        if kw in text_lower:
            score += 2.0

    domain = urlparse(url).netloc.lower()
    if domain.endswith(".gov"):
        score += 3.0
    elif domain.endswith(".org"):
        score += 1.5

    for bad in DEPRIORITY_DOMAINS:
        if bad in domain:
            score -= 10.0

    url_lower = url.lower()
    for term in ["rate", "billing", "water-rate", "tariff", "fee-schedule", "schedule"]:
        if term in url_lower:
            score += 2.0

    if url_lower.endswith(".pdf"):
        score += 3.0  # Stronger PDF bonus — PDFs parse much better
    if "documentcenter" in url_lower or "viewfile" in url_lower:
        score += 2.0  # CMS document links are usually PDFs

    return score


# ── Search ─────────────────────────────────────────────────────────────────

def search_searxng(query: str, searxng_url: str, proxy: str | None = None) -> list[dict]:
    """Query SearXNG and return scored results."""
    params = {"q": query, "format": "json", "categories": "general", "language": "en"}
    client_kwargs = {"timeout": 15}
    if proxy:
        client_kwargs["proxy"] = proxy

    try:
        with httpx.Client(**client_kwargs) as client:
            resp = client.get(searxng_url, params=params)
            resp.raise_for_status()
    except Exception as e:
        return []

    results = []
    for r in resp.json().get("results", []):
        url = r.get("url", "")
        if not url:
            continue
        title = r.get("title", "")
        snippet = r.get("content", "")
        s = score_result(url, title, snippet)
        results.append({"url": url, "title": title, "snippet": snippet, "score": s})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:10]


def verify_url(url: str) -> dict:
    """HTTP HEAD check."""
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    try:
        with httpx.Client(headers=headers, timeout=10, follow_redirects=True) as c:
            r = c.head(url)
            if r.status_code == 405:
                r = c.get(url, follow_redirects=True)
            ct = r.headers.get("content-type", "")
            return {
                "ok": r.status_code < 400,
                "status": r.status_code,
                "is_pdf": "pdf" in ct.lower() or url.lower().endswith(".pdf"),
            }
    except Exception:
        return {"ok": False, "status": None, "is_pdf": False}


# ── Target Loading ─────────────────────────────────────────────────────────

def get_targets(state: str, min_population: int = 0) -> list[dict]:
    """Get MDWD utilities for a state, ordered by population."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT c.pwsid, c.pws_name, c.county_served, m.population
            FROM {schema}.cws_boundaries c
            JOIN {schema}.mdwd_financials m ON m.pwsid = c.pwsid
            WHERE c.state_code = :state
            AND (m.population >= :min_pop OR m.population IS NULL)
            ORDER BY m.population DESC NULLS LAST
        """), {"state": state, "min_pop": min_population}).fetchall()
    return [{"pwsid": r[0], "name": r[1], "county": r[2], "pop": r[3]} for r in rows]


def load_existing(output_path: Path) -> dict:
    """Load already-discovered URLs from output YAML."""
    if not output_path.exists():
        return {}
    with open(output_path) as f:
        data = yaml.safe_load(f) or {}
    return {k: v for k, v in data.items() if isinstance(v, str) and v.strip()}


def save_results(output_path: Path, results: dict, metadata: dict):
    """Write results to YAML incrementally."""
    with open(output_path, "w") as f:
        f.write(f"# Auto-discovered rate URLs via SearXNG\n")
        f.write(f"# State: {metadata.get('state', '?')}\n")
        f.write(f"# Discovered: {len(results)} | Last run: {datetime.now().isoformat()}\n")
        f.write(f"# Run: python scripts/standalone_discover.py --state {metadata.get('state', '?')}\n\n")
        for pwsid in sorted(results.keys()):
            url = results[pwsid]
            f.write(f'{pwsid}: "{url}"\n')


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Standalone rate URL discovery")
    parser.add_argument("--state", required=True, help="State code (VA, CA)")
    parser.add_argument("--delay", type=float, default=5.0, help="Seconds between queries")
    parser.add_argument("--output", default=None, help="Output YAML path")
    parser.add_argument("--searxng", default="http://localhost:8888/search", help="SearXNG URL")
    parser.add_argument("--proxy", default=None, help="HTTP proxy (e.g., socks5://vps:1080)")
    parser.add_argument("--min-pop", type=int, default=0, help="Minimum population filter")
    parser.add_argument("--limit", type=int, default=None, help="Max utilities to process")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parents[1] / "config" / f"rate_urls_{args.state.lower()}_discovered.yaml"
    )

    targets = get_targets(args.state, args.min_pop)
    existing = load_existing(output_path)
    results = dict(existing)  # Preserve previously found URLs

    # Skip already-discovered + already-parsed
    schema = settings.utility_schema
    with engine.connect() as conn:
        parsed = conn.execute(text(f"""
            SELECT DISTINCT pwsid FROM {schema}.water_rates
            WHERE parse_confidence IN ('high', 'medium')
        """)).fetchall()
        parsed_set = {r[0] for r in parsed}

    todo = [t for t in targets if t["pwsid"] not in existing and t["pwsid"] not in parsed_set]
    if args.limit:
        todo = todo[:args.limit]

    print(f"State: {args.state} | Targets: {len(targets)} | Already found: {len(existing)} "
          f"| Already parsed: {len(parsed_set)} | To discover: {len(todo)}")
    print(f"Output: {output_path}")
    print(f"Delay: {args.delay}s | Proxy: {args.proxy or 'none'}")
    print()

    consecutive_empty = 0

    for i, t in enumerate(todo):
        pwsid = t["pwsid"]
        name = t["name"]
        county = t["county"] or ""
        pop = t["pop"] or 0

        # Search with two query variants
        q1 = f"{name} {county} {args.state} water rates"
        hits1 = search_searxng(q1, args.searxng, args.proxy)
        time.sleep(1.0)

        short = name.replace("CITY OF ", "").replace(", CITY OF", "").replace("TOWN OF ", "").strip()
        q2 = f"{short} {args.state} water rate schedule PDF"
        hits2 = search_searxng(q2, args.searxng, args.proxy)

        # Merge and deduplicate
        seen = set()
        combined = []
        for h in hits1 + hits2:
            if h["url"] not in seen:
                seen.add(h["url"])
                combined.append(h)
        combined.sort(key=lambda x: x["score"], reverse=True)

        if not combined or combined[0]["score"] <= 0:
            consecutive_empty += 1
            print(f"  [{i+1}/{len(todo)}] ✗ {pwsid} {name[:35]} — no results")
            if consecutive_empty >= 8:
                print(f"\n  ⚠ {consecutive_empty} consecutive empties — rate limited. Saving and exiting.")
                break
            time.sleep(args.delay)
            continue

        consecutive_empty = 0

        # Verify top candidate
        best = combined[0]
        vr = verify_url(best["url"])
        if vr["ok"]:
            results[pwsid] = best["url"]
            tag = "[PDF]" if vr["is_pdf"] else "[HTML]"
            print(f"  [{i+1}/{len(todo)}] ✓ {pwsid} {name[:35]:<35} {tag} score={best['score']:.0f} pop={pop:,}")
        else:
            # Try second candidate
            if len(combined) > 1:
                vr2 = verify_url(combined[1]["url"])
                if vr2["ok"]:
                    results[pwsid] = combined[1]["url"]
                    tag = "[PDF]" if vr2["is_pdf"] else "[HTML]"
                    print(f"  [{i+1}/{len(todo)}] ✓ {pwsid} {name[:35]:<35} {tag} score={combined[1]['score']:.0f} (2nd)")
                else:
                    print(f"  [{i+1}/{len(todo)}] ✗ {pwsid} {name[:35]} — top 2 failed HTTP")
            else:
                print(f"  [{i+1}/{len(todo)}] ✗ {pwsid} {name[:35]} — HTTP {vr['status']}")

        # Save incrementally every 10 discoveries
        if len(results) % 10 == 0:
            save_results(output_path, results, {"state": args.state})

        time.sleep(args.delay)

    # Final save
    save_results(output_path, results, {"state": args.state})
    new_found = len(results) - len(existing)
    print(f"\n=== Done ===")
    print(f"New URLs found: {new_found} | Total in file: {len(results)}")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()

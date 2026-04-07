#!/usr/bin/env python3
"""
Recover Orphan source_urls (R0 Fingerprint Matcher)

Purpose:
    Re-link orphan rate_schedules rows to their source URLs by searching
    cached scrape_registry.scraped_text for distinctive fingerprint tokens
    extracted from each orphan row's bills, charges, tier rates, and
    parse_notes phrases. Zero LLM calls, zero web fetches, zero re-parsing.

    Built to recover the 2,643 scraped_llm rate_schedules rows from the
    2026-03-31 batch sweep that were inserted with NULL source_url despite
    the URL existing in scrape_registry. Generalizes to any provenance-loss
    recovery where rate_schedules has the rates but source_url is NULL and
    the corresponding scrape_registry rows still hold cached scraped_text.

    Used in the source_url integrity audit (2026-04-06) to recover 1,880
    of 2,643 orphans on first pass, taking rate_best_estimate.source_url
    coverage from 9.0% to 95.1% at $0 cost. See the session summary at
    docs/session_summaries/2026-04-06_source_url_audit_and_recovery.md
    for the full audit trail.

Author: AI-Generated (Claude Opus 4.6, 1M context)
Created: 2026-04-06
Modified: 2026-04-06

Dependencies:
    - sqlalchemy
    - utility_api.db.engine

Usage:
    python scripts/recover_orphan_urls.py             # dry-run report only
    python scripts/recover_orphan_urls.py --execute   # apply UPDATEs

    Default scope is the 2026-03-31 batch orphan set. Edit the SQL filter
    in main() if reusing for a different defect window.

Notes:
    - Confidence tiers (executed in --execute mode):
        match_phrase           — phrase hit + clear winner (highest)
        match_strong_numeric   — ≥3 numeric hits, clear winner
        match_medium_numeric   — 2 numeric hits, clear winner
        match_unique_candidate — only one candidate available
      Deferred (NOT executed, available in dry-run report):
        match_weak_unique      — 1 numeric hit, no competition
                                  (cross-state contamination risk)
    - Each UPDATE only writes when source_url IS NULL — idempotent and safe
      to re-run after a failed pass.
    - The full UPDATE batch runs in a single transaction so partial failure
      rolls back cleanly.
    - Phrase tokens (parse_notes proper nouns) are weighted 5x numeric tokens
      because they are dramatically more distinctive (e.g., "Pennsylvania-
      American Water Rate Zone 1" appears in exactly one URL).
    - COMMON_VALUES set excludes round amounts ($5.00, $10.00, etc.) that
      appear in many unrelated documents and would cause false positives.

Data Sources:
    - Input:  utility.rate_schedules (orphan rows with NULL source_url)
              utility.scrape_registry (candidate URLs + cached scraped_text)
    - Output: utility.rate_schedules.source_url (UPDATE only)
"""

# Standard library imports
import json
import re
import sys
from collections import Counter, defaultdict

# Third-party imports
from sqlalchemy import text

# Local imports
from utility_api.db import engine


# --- Constants ---

# Common bill amounts that appear in many unrelated documents — excluded as
# fingerprint tokens because they cause cross-document false positives.
COMMON_VALUES = {
    "5.00", "10.00", "15.00", "20.00", "25.00", "50.00", "100.00",
    "1.00", "2.00", "3.00", "5", "10", "15", "20", "25", "50", "100",
}

# Common phrases excluded from notes-based phrase extraction.
STOPWORDS = {
    "water", "rate", "rates", "service", "monthly", "residential", "customer",
    "customers", "tier", "tiers", "structure", "fee", "charge", "charges",
    "fixed", "volumetric", "billing", "meter", "5/8", "3/4", "inch",
    "gallons", "gallon", "ccf", "kgal", "per", "the", "for", "with",
    "and", "from", "first", "second", "third", "minimum", "basic",
}

# Confidence tiers eligible for execution (high + medium only).
TRUSTED_ACTIONS = {
    "match_phrase",
    "match_strong_numeric",
    "match_medium_numeric",
    "match_unique_candidate",
}


# --- Token extraction ---

def build_numeric_tokens(row) -> list[str]:
    """Distinctive numeric tokens from bill/charge/tier values, with format variants."""
    raw_values = []
    for col in ("bill_5ccf", "bill_6ccf", "bill_9ccf", "bill_10ccf",
                "bill_12ccf", "bill_20ccf", "bill_24ccf"):
        v = row.get(col)
        if v is not None and v > 0.5:
            raw_values.append(float(v))

    fc = row.get("fixed_charges")
    if fc:
        try:
            if isinstance(fc, str):
                fc = json.loads(fc)
            for f in fc:
                amt = f.get("amount")
                if amt is not None and amt > 0.5:
                    raw_values.append(float(amt))
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    vt = row.get("volumetric_tiers")
    if vt:
        try:
            if isinstance(vt, str):
                vt = json.loads(vt)
            for t in vt:
                for k in ("rate_per_1000_gal", "rate_per_ccf", "rate_per_unit", "rate"):
                    rate = t.get(k)
                    if rate is not None and rate > 0.05:
                        raw_values.append(float(rate))
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    raw_values = sorted(set(raw_values), reverse=True)

    tokens = set()
    for v in raw_values:
        s2 = f"{v:.2f}"
        tokens.add(s2)
        if v >= 1000:
            tokens.add(f"{v:,.2f}")
        if s2.endswith('0'):
            tokens.add(s2.rstrip('0').rstrip('.'))
        if v == int(v) and v >= 10:
            tokens.add(str(int(v)))

    return [t for t in tokens if t not in COMMON_VALUES]


def build_phrase_tokens(parse_notes: str) -> list[str]:
    """Extract distinctive phrases from parse_notes (proper nouns, multi-word locality phrases)."""
    if not parse_notes:
        return []
    notes = parse_notes[:500]  # bound work

    phrases = set()

    # Pattern 1: capitalized multi-word phrases (proper nouns), 2-5 words long
    multi_cap = re.findall(r"\b[A-Z][a-zA-Z]+(?:[\s\-][A-Z][a-zA-Z]+){1,4}\b", notes)
    for p in multi_cap:
        if len(p) >= 6 and not all(w.lower() in STOPWORDS for w in p.split()):
            phrases.add(p)

    # Pattern 2: hyphenated proper nouns ("Pennsylvania-American")
    hyph = re.findall(r"\b[A-Z][a-z]+-[A-Z][a-z]+(?:-[A-Z][a-z]+)*\b", notes)
    for p in hyph:
        phrases.add(p)

    # Pattern 3: city + state pairs ("La Porte, TX")
    city_state = re.findall(r"\b[A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?,\s[A-Z]{2}\b", notes)
    for p in city_state:
        phrases.add(p)
        city_only = p.split(",")[0].strip()
        if len(city_only) >= 4:
            phrases.add(city_only)

    return sorted(phrases, key=lambda p: -len(p))


# --- Scoring ---

def score_candidate(text_str: str, num_tokens: list[str],
                    phrase_tokens: list[str]) -> tuple[float, int, int, list[str]]:
    """Score a candidate scraped_text against fingerprint tokens.

    Returns
    -------
    tuple
        (composite_score, num_hits, phrase_hits, hit_tokens)
        Phrases are weighted 5x numeric tokens.
    """
    if not text_str:
        return 0.0, 0, 0, []
    num_hits = []
    for tok in num_tokens:
        if tok in text_str:
            num_hits.append(tok)
    phrase_hits = []
    text_lower = text_str.lower()
    for ph in phrase_tokens:
        if ph in text_str or ph.lower() in text_lower:
            phrase_hits.append(ph)
    composite = len(num_hits) + 5 * len(phrase_hits)
    return composite, len(num_hits), len(phrase_hits), num_hits + phrase_hits


# --- Main ---

def main():
    """Run the R0 fingerprint matcher in dry-run or execute mode."""
    execute = "--execute" in sys.argv
    print(f"Mode: {'EXECUTE' if execute else 'DRY-RUN'}\n")

    print("Loading orphan rs rows...")
    with engine.connect() as conn:
        orphans = conn.execute(text("""
            SELECT id, pwsid, bill_5ccf, bill_6ccf, bill_9ccf, bill_10ccf,
                   bill_12ccf, bill_20ccf, bill_24ccf,
                   fixed_charges, volumetric_tiers, parse_notes
            FROM utility.rate_schedules
            WHERE source_key='scraped_llm'
              AND source_url IS NULL
              AND date_trunc('day', created_at)='2026-03-31'
            ORDER BY pwsid, id
        """)).mappings().all()
    print(f"  {len(orphans)} orphan rows across {len(set(o['pwsid'] for o in orphans))} pwsids")

    print("Loading scrape_registry candidates...")
    pwsids = list(set(o["pwsid"] for o in orphans))
    cand_by_pwsid = defaultdict(list)
    with engine.connect() as conn:
        for batch_start in range(0, len(pwsids), 500):
            batch = pwsids[batch_start:batch_start+500]
            result = conn.execute(text("""
                SELECT id, pwsid, url, scraped_text, last_content_length
                FROM utility.scrape_registry
                WHERE pwsid = ANY(:p)
                  AND url IS NOT NULL
                  AND scraped_text IS NOT NULL
                  AND length(scraped_text) > 200
            """), {"p": batch}).mappings().all()
            for r in result:
                cand_by_pwsid[r["pwsid"]].append(dict(r))
    total_cands = sum(len(v) for v in cand_by_pwsid.values())
    print(f"  {total_cands} candidates with cached text\n")

    print("Scoring matches (numeric + phrase + format variants)...")
    decisions = []
    for o in orphans:
        num_tokens = build_numeric_tokens(o)
        phrase_tokens = build_phrase_tokens(o.get("parse_notes"))
        cands = cand_by_pwsid.get(o["pwsid"], [])

        if not num_tokens and not phrase_tokens:
            decisions.append({
                "id": o["id"], "pwsid": o["pwsid"], "action": "skip_no_tokens",
                "url": None, "score": 0, "runner_up": 0,
                "n_num": 0, "n_phr": 0, "n_cands": len(cands),
                "num_hits": 0, "phr_hits": 0, "reason": "no fingerprint tokens",
            })
            continue
        if not cands:
            decisions.append({
                "id": o["id"], "pwsid": o["pwsid"], "action": "skip_no_candidates",
                "url": None, "score": 0, "runner_up": 0,
                "n_num": len(num_tokens), "n_phr": len(phrase_tokens), "n_cands": 0,
                "num_hits": 0, "phr_hits": 0, "reason": "no cached candidates",
            })
            continue

        scored = []
        for c in cands:
            comp, nh, ph, hits = score_candidate(c["scraped_text"], num_tokens, phrase_tokens)
            scored.append((comp, nh, ph, c, hits))
        scored.sort(key=lambda x: (-x[0], -(x[3]["last_content_length"] or 0)))

        best = scored[0]
        runner = scored[1] if len(scored) > 1 else (0, 0, 0, None, [])

        best_score = best[0]
        runner_score = runner[0]

        # Decision rules
        if best_score == 0:
            action = "skip_zero_hits"
            url = None
        elif len(scored) == 1:
            action = "match_unique_candidate"
            url = best[3]["url"]
        elif best[2] >= 1 and best_score > runner_score:
            # Phrase match + clear winner = very strong
            action = "match_phrase"
            url = best[3]["url"]
        elif best[1] >= 3 and best_score > runner_score:
            action = "match_strong_numeric"
            url = best[3]["url"]
        elif best[1] >= 2 and best_score > runner_score:
            action = "match_medium_numeric"
            url = best[3]["url"]
        elif best[1] >= 1 and best_score > runner_score and runner_score == 0:
            action = "match_weak_unique"
            url = best[3]["url"]
        else:
            action = "skip_tie_or_weak"
            url = None

        decisions.append({
            "id": o["id"], "pwsid": o["pwsid"], "action": action, "url": url,
            "score": best_score, "runner_up": runner_score,
            "n_num": len(num_tokens), "n_phr": len(phrase_tokens), "n_cands": len(cands),
            "num_hits": best[1], "phr_hits": best[2], "reason": "",
        })

    # Report
    print("\n=== R0 MATCH REPORT ===")
    action_counts = Counter(d["action"] for d in decisions)
    for action, cnt in sorted(action_counts.items(), key=lambda x: -x[1]):
        print(f"  {action:30s}: {cnt:>6} ({100*cnt/len(decisions):.1f}%)")

    matched_actions = {a for a in action_counts if a.startswith("match")}
    matched = sum(action_counts[a] for a in matched_actions)
    print(f"\nTotal matched: {matched} / {len(decisions)} ({100*matched/len(decisions):.1f}%)")
    print(f"Trusted (would execute): {sum(action_counts[a] for a in TRUSTED_ACTIONS)}")

    # Show samples per category
    sample_order = ["match_phrase", "match_strong_numeric", "match_medium_numeric",
                    "match_unique_candidate", "match_weak_unique",
                    "skip_tie_or_weak", "skip_zero_hits"]
    for action in sample_order:
        rows = [d for d in decisions if d["action"] == action][:5]
        if rows:
            print(f"\n=== Sample {action} (5) ===")
            for d in rows:
                u = (d["url"] or "")[:75]
                print(f"  {d['pwsid']} score={d['score']} (num={d['num_hits']}/{d['n_num']}, "
                      f"phr={d['phr_hits']}/{d['n_phr']}) runner={d['runner_up']} "
                      f"cands={d['n_cands']} → {u}")

    if not execute:
        print("\n[DRY-RUN] No changes made. Re-run with --execute to apply.")
        return decisions

    # EXECUTE — only trusted-tier matches
    print("\n=== EXECUTING UPDATEs ===")
    confident = [d for d in decisions if d["action"] in TRUSTED_ACTIONS]
    weak_count = len([d for d in decisions if d["action"] == "match_weak_unique"])
    print(f"Will UPDATE {len(confident)} rows (excluding {weak_count} weak_unique deferred for review)")

    updated = 0
    failed = 0
    with engine.connect() as conn:
        with conn.begin():
            for d in confident:
                try:
                    conn.execute(text("""
                        UPDATE utility.rate_schedules
                        SET source_url = :url
                        WHERE id = :id AND source_url IS NULL
                    """), {"url": d["url"], "id": d["id"]})
                    updated += 1
                except Exception as e:
                    print(f"  failed id={d['id']}: {e}")
                    failed += 1

    print(f"\nUpdated: {updated}")
    print(f"Failed:  {failed}")
    print("\nRun BestEstimateAgent().run() to propagate URLs to rate_best_estimate.")
    return decisions


if __name__ == "__main__":
    main()

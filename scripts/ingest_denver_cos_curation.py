#!/usr/bin/env python3
"""
Ingest Denver/CO Springs Metro Water Rate URL Curation into scrape_registry

Purpose:
    Process denver_cos_metro_rate_curation.json and insert valid entries
    into utility.scrape_registry. Resolves PWSIDs from SDWIS, checks
    existing coverage, and inserts with ON CONFLICT DO NOTHING safety.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    python scripts/ingest_denver_cos_curation.py
    python scripts/ingest_denver_cos_curation.py --dry-run

Notes:
    - Idempotent: ON CONFLICT (pwsid, url) DO NOTHING
    - Follows same direct-insert pattern as Portland metro session
    - PWSID lookup uses OR logic (name_hint OR city) per CLAUDE.md spec
    - SEWER_ONLY entries skipped (no PWSID path)
    - NEEDS_VERIFICATION entries inserted with url_unconfirmed note flag

Data Sources:
    - Input: docs/denver_cos_metro_rate_curation.json
    - Output: utility.scrape_registry table
    - Lookup: utility.sdwis_systems table
    - Coverage: utility.pwsid_coverage view, utility.scrape_registry

Configuration:
    - DATABASE_URL from .env via utility_api.config
"""

import argparse
import json
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

# Constants
CURATION_FILE = PROJECT_ROOT / "docs" / "denver_cos_metro_rate_curation.json"
SCHEMA = settings.utility_schema


def _detect_content_type(url: str) -> str:
    """Guess content type from URL."""
    url_lower = url.lower()
    if url_lower.endswith(".pdf"):
        return "pdf"
    if url_lower.endswith((".xlsx", ".xls", ".csv")):
        return "xlsx"
    return "html"


def load_curation_file(path: Path) -> tuple[dict, list[dict]]:
    """Load and validate a curation JSON file.

    Returns
    -------
    tuple
        (_meta dict, utilities list)
    """
    with open(path) as f:
        data = json.load(f)

    meta = data["_meta"]
    utilities = data["utilities"]
    expected = meta.get("total_entries")

    if expected and len(utilities) != expected:
        logger.warning(
            f"Entry count mismatch: file says {expected}, found {len(utilities)}"
        )
    else:
        logger.info(f"Loaded {len(utilities)} entries (matches _meta.total_entries)")

    return meta, utilities


def filter_entries(utilities: list[dict]) -> tuple[list[dict], list[dict]]:
    """Filter out entries that should not be inserted.

    Returns
    -------
    tuple
        (insertable entries, filtered entries with reasons)
    """
    insertable = []
    filtered = []

    for entry in utilities:
        flag = entry.get("scraper_flag", "")

        if flag and flag.startswith("DUPLICATE_"):
            filtered.append({
                "id": entry["id"],
                "utility_name": entry["utility_name"],
                "reason": f"DUPLICATE: {flag}",
            })
            logger.info(
                f"  FILTERED id={entry['id']} {entry['utility_name']} → {flag}"
            )
        elif flag == "INFO_ONLY_DO_NOT_INSERT":
            filtered.append({
                "id": entry["id"],
                "utility_name": entry["utility_name"],
                "reason": "INFO_ONLY_DO_NOT_INSERT",
            })
            logger.info(
                f"  FILTERED id={entry['id']} {entry['utility_name']} → INFO_ONLY"
            )
        elif flag == "SEWER_ONLY_VERIFY_SCHEMA_FIT":
            filtered.append({
                "id": entry["id"],
                "utility_name": entry["utility_name"],
                "reason": "NO_PWSID_SEWER_ONLY — scrape_registry keyed on PWSID; "
                          "sewer-only systems have no PWSID",
            })
            logger.info(
                f"  FILTERED id={entry['id']} {entry['utility_name']} → "
                f"NO_PWSID_SEWER_ONLY"
            )
        else:
            insertable.append(entry)

    return insertable, filtered


# Manual PWSID overrides for entries that are unambiguous to a human
# but fail automated matching due to SDWIS name variations or city mismatches.
# Each was verified via targeted SDWIS query on 2026-03-26.
MANUAL_PWSID_OVERRIDES = {
    4: ("CO0103045", "ENGLEWOOD CITY OF — name+pop match"),
    5: ("CO0103476", "LITTLETON CITY OF — Denver Water wholesale, pop=0 expected"),
    6: ("CO0103185", "COLUMBINE WSD — small district, pop=0 in SDWIS"),
    12: ("CO0130800", "VALLEY WD — name match, pop=10000"),
    14: ("CO0130020", "CONSOLIDATED MUTUAL MAPLE GROVE — name match, pop=81400"),
    15: ("CO0130040", "GOLDEN CITY OF — name+pop match"),
    16: ("CO0103614", "PLATTE CANYON WSD — exact name match"),
    17: ("CO0103723", "SOUTHWEST METROPOLITAN WSD — exact name match"),
    20: ("CO0118040", "PARKER WSD — exact name match"),
    22: ("CO0118055", "ROXBOROUGH WSD — exact name match"),
    24: ("CO0121775", "SECURITY WATER DISTRICT — targeted SDWIS search"),
    25: ("CO0121900", "WIDEFIELD WSD — targeted SDWIS search"),
    31: ("CO0118021", "DOMINION WSD — new district, pop=0 expected"),
    34: ("CO0116175", "CHERRY CREEK VALLEY WSD — exact name match"),
}


def resolve_pwsids(entries: list[dict], state_codes: list[str]) -> list[dict]:
    """Resolve PWSIDs from SDWIS for entries missing them.

    Uses OR logic: matches on name_hint OR city independently.
    Falls back to MANUAL_PWSID_OVERRIDES for entries that fail automated matching.

    Parameters
    ----------
    entries : list[dict]
        Entries to resolve.
    state_codes : list[str]
        State codes to search in SDWIS.

    Returns
    -------
    list[dict]
        Entries with resolution status added.
    """
    resolved = []
    not_found = []
    ambiguous = []
    pre_set = []

    for entry in entries:
        if entry.get("pwsid"):
            # PWSID already provided
            entry["_pwsid_status"] = "PRE_SET"
            pre_set.append(entry)
            logger.info(
                f"  PWSID PRE_SET id={entry['id']} {entry['utility_name']} → "
                f"{entry['pwsid']}"
            )
            continue

        name_hint = entry.get("sdwis_name_hint")
        city = entry.get("sdwis_city")

        if not name_hint and not city:
            entry["_pwsid_status"] = "PWSID_NOT_FOUND"
            not_found.append(entry)
            logger.warning(
                f"  PWSID_NOT_FOUND id={entry['id']} {entry['utility_name']} → "
                f"no sdwis_name_hint or sdwis_city"
            )
            continue

        # Build query with OR logic
        # Build state_code filter as literal SQL since psycopg can't bind tuples
        state_list = ", ".join(f"'{s}'" for s in state_codes)

        with engine.connect() as conn:
            conditions = []
            params = {}

            if name_hint:
                conditions.append("UPPER(s.pws_name) LIKE :name_pattern")
                params["name_pattern"] = f"%{name_hint.upper()}%"

            if city:
                conditions.append("UPPER(s.city) = :city")
                params["city"] = city.upper()

            where_clause = " OR ".join(conditions)

            rows = conn.execute(text(f"""
                SELECT s.pwsid, s.pws_name, s.city, s.state_code,
                       s.population_served_count
                FROM {SCHEMA}.sdwis_systems s
                WHERE s.state_code IN ({state_list})
                  AND s.pws_type_code = 'CWS'
                  AND ({where_clause})
                ORDER BY s.population_served_count DESC NULLS LAST
                LIMIT 5
            """), params).fetchall()

        if len(rows) == 0:
            # Check manual overrides before giving up
            if entry["id"] in MANUAL_PWSID_OVERRIDES:
                pwsid, reason = MANUAL_PWSID_OVERRIDES[entry["id"]]
                entry["pwsid"] = pwsid
                entry["_pwsid_status"] = "RESOLVED"
                entry["_sdwis_name"] = reason
                entry["_sdwis_pop"] = None
                resolved.append(entry)
                logger.info(
                    f"  RESOLVED (manual) id={entry['id']} {entry['utility_name']} → "
                    f"{pwsid} ({reason})"
                )
            else:
                entry["_pwsid_status"] = "PWSID_NOT_FOUND"
                not_found.append(entry)
                logger.warning(
                    f"  PWSID_NOT_FOUND id={entry['id']} {entry['utility_name']} → "
                    f"0 SDWIS results for name='{name_hint}' city='{city}'"
                )
        elif len(rows) == 1:
            entry["pwsid"] = rows[0].pwsid
            entry["_pwsid_status"] = "RESOLVED"
            entry["_sdwis_name"] = rows[0].pws_name
            entry["_sdwis_pop"] = rows[0].population_served_count
            resolved.append(entry)
            logger.info(
                f"  RESOLVED id={entry['id']} {entry['utility_name']} → "
                f"{rows[0].pwsid} ({rows[0].pws_name}, pop={rows[0].population_served_count})"
            )
        else:
            # Multiple results — try to pick the best one
            pop_approx = entry.get("population_approx")
            best = _pick_best_match(rows, pop_approx, entry)

            if best:
                entry["pwsid"] = best.pwsid
                entry["_pwsid_status"] = "RESOLVED"
                entry["_sdwis_name"] = best.pws_name
                entry["_sdwis_pop"] = best.population_served_count
                resolved.append(entry)
                logger.info(
                    f"  RESOLVED id={entry['id']} {entry['utility_name']} → "
                    f"{best.pwsid} ({best.pws_name}, pop={best.population_served_count}) "
                    f"[picked from {len(rows)} candidates]"
                )
            else:
                # Check manual overrides before marking ambiguous
                if entry["id"] in MANUAL_PWSID_OVERRIDES:
                    pwsid, reason = MANUAL_PWSID_OVERRIDES[entry["id"]]
                    entry["pwsid"] = pwsid
                    entry["_pwsid_status"] = "RESOLVED"
                    entry["_sdwis_name"] = reason
                    entry["_sdwis_pop"] = None
                    resolved.append(entry)
                    logger.info(
                        f"  RESOLVED (manual) id={entry['id']} "
                        f"{entry['utility_name']} → {pwsid} ({reason}) "
                        f"[from {len(rows)} candidates]"
                    )
                else:
                    entry["_pwsid_status"] = "PWSID_AMBIGUOUS"
                    entry["_candidates"] = [
                        f"{r.pwsid} {r.pws_name} pop={r.population_served_count}"
                        for r in rows
                    ]
                    ambiguous.append(entry)
                    logger.warning(
                        f"  PWSID_AMBIGUOUS id={entry['id']} "
                        f"{entry['utility_name']} → {len(rows)} candidates:"
                    )
                    for r in rows:
                        logger.warning(
                            f"    {r.pwsid} | {r.pws_name} | {r.city} | "
                            f"pop={r.population_served_count}"
                        )

    logger.info(
        f"\nPWSID Resolution: {len(pre_set)} pre-set, {len(resolved)} resolved, "
        f"{len(not_found)} not found, {len(ambiguous)} ambiguous"
    )

    return entries


def _pick_best_match(rows, pop_approx, entry):
    """Pick the best SDWIS match from multiple candidates.

    Strategy:
    - If one candidate has population within 2x of pop_approx, prefer it
    - If the top candidate by population is clearly dominant (>5x next), use it
    - Otherwise return None (ambiguous)
    """
    if not rows:
        return None

    # If we have a population hint, look for closest match
    if pop_approx:
        plausible = [
            r for r in rows
            if r.population_served_count
            and 0.2 * pop_approx <= r.population_served_count <= 5 * pop_approx
        ]
        if len(plausible) == 1:
            return plausible[0]

    # If top candidate dominates by population
    if (
        len(rows) >= 2
        and rows[0].population_served_count
        and rows[1].population_served_count
        and rows[0].population_served_count > 5 * rows[1].population_served_count
    ):
        return rows[0]

    # Check if name_hint exactly matches one candidate
    name_hint = entry.get("sdwis_name_hint", "").upper()
    if name_hint:
        exact = [r for r in rows if name_hint in r.pws_name.upper()]
        if len(exact) == 1:
            return exact[0]

    return None


def check_coverage(entries: list[dict]) -> list[dict]:
    """Check existing coverage for entries with resolved PWSIDs.

    Returns entries with coverage status added.
    """
    already_covered = 0
    already_queued_same = 0
    proceed = 0

    for entry in entries:
        if entry.get("_pwsid_status") not in ("RESOLVED", "PRE_SET"):
            continue

        pwsid = entry["pwsid"]

        with engine.connect() as conn:
            # Check pwsid_coverage
            cov = conn.execute(text(f"""
                SELECT
                    pc.has_rate_data,
                    (SELECT COUNT(*) FROM {SCHEMA}.scrape_registry sr
                     WHERE sr.pwsid = pc.pwsid
                       AND sr.status IN ('pending', 'active')) AS pending_urls,
                    (SELECT COUNT(*) FROM {SCHEMA}.scrape_registry sr
                     WHERE sr.pwsid = pc.pwsid
                       AND sr.last_parse_result = 'success') AS successful_parses
                FROM {SCHEMA}.pwsid_coverage pc
                WHERE pc.pwsid = :pwsid
            """), {"pwsid": pwsid}).fetchone()

            if cov and cov.has_rate_data:
                entry["_coverage_status"] = "ALREADY_COVERED"
                already_covered += 1
                logger.info(
                    f"  ALREADY_COVERED id={entry['id']} {entry['utility_name']} "
                    f"({pwsid}) — has rate data"
                )
                continue

            if cov and cov.pending_urls > 0 and cov.successful_parses == 0:
                # Check if same URL already queued
                existing = conn.execute(text(f"""
                    SELECT url FROM {SCHEMA}.scrape_registry
                    WHERE pwsid = :pwsid
                      AND status IN ('pending', 'active')
                """), {"pwsid": pwsid}).fetchall()

                existing_urls = {r.url for r in existing}
                if entry["url"] in existing_urls:
                    entry["_coverage_status"] = "ALREADY_QUEUED"
                    already_queued_same += 1
                    logger.info(
                        f"  ALREADY_QUEUED id={entry['id']} {entry['utility_name']} "
                        f"({pwsid}) — same URL already pending"
                    )
                    continue
                else:
                    # Different URL — proceed to insert as additional
                    logger.info(
                        f"  ALREADY_QUEUED_DIFF_URL id={entry['id']} "
                        f"{entry['utility_name']} ({pwsid}) — inserting better URL"
                    )

            entry["_coverage_status"] = "PROCEED"
            proceed += 1

    logger.info(
        f"\nCoverage Check: {already_covered} covered, "
        f"{already_queued_same} queued (same URL), {proceed} to insert"
    )

    return entries


def insert_entries(
    entries: list[dict], meta: dict, dry_run: bool = False
) -> dict:
    """Insert filtered, resolved, coverage-cleared entries into scrape_registry.

    Returns summary dict.
    """
    url_source = meta["url_source"]
    default_notes = meta["default_notes"]

    primary_inserted = 0
    secondary_inserted = 0
    verification_flagged = 0
    skipped = 0

    for entry in entries:
        status = entry.get("_pwsid_status")
        coverage = entry.get("_coverage_status")

        if status not in ("RESOLVED", "PRE_SET"):
            skipped += 1
            continue

        if coverage not in ("PROCEED",):
            skipped += 1
            continue

        pwsid = entry["pwsid"]
        url = entry["url"]
        flag = entry.get("scraper_flag", "")

        # Build notes
        notes = default_notes
        if flag == "NEEDS_VERIFICATION":
            notes += " | url_unconfirmed"
            verification_flagged += 1

        if dry_run:
            logger.info(
                f"  [DRY RUN] INSERT id={entry['id']} {entry['utility_name']} "
                f"→ {pwsid} | {url[:60]}..."
            )
            primary_inserted += 1
            continue

        # Primary URL insert
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                INSERT INTO {SCHEMA}.scrape_registry
                    (pwsid, url, url_source, content_type, status, notes)
                VALUES
                    (:pwsid, :url, :url_source, :content_type, 'pending', :notes)
                ON CONFLICT (pwsid, url) DO NOTHING
            """), {
                "pwsid": pwsid,
                "url": url,
                "url_source": url_source,
                "content_type": _detect_content_type(url),
                "notes": notes,
            })
            if result.rowcount > 0:
                primary_inserted += 1
                logger.info(
                    f"  INSERTED id={entry['id']} {entry['utility_name']} "
                    f"→ {pwsid} | {url[:60]}"
                )
            else:
                logger.info(
                    f"  SKIPPED (conflict) id={entry['id']} {entry['utility_name']} "
                    f"→ {pwsid}"
                )

            # Secondary URL
            secondary_url = entry.get("url_secondary")
            if secondary_url and result.rowcount > 0:
                sec_result = conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.scrape_registry
                        (pwsid, url, url_source, content_type, status, notes)
                    VALUES
                        (:pwsid, :url, :url_source, :content_type, 'pending', :notes)
                    ON CONFLICT (pwsid, url) DO NOTHING
                """), {
                    "pwsid": pwsid,
                    "url": secondary_url,
                    "url_source": url_source,
                    "content_type": _detect_content_type(secondary_url),
                    "notes": notes + " [secondary]",
                })
                if sec_result.rowcount > 0:
                    secondary_inserted += 1
                    logger.info(f"    + secondary URL: {secondary_url[:60]}")

            # Rate PDF URL
            pdf_url = entry.get("url_rate_pdf")
            if pdf_url and result.rowcount > 0:
                pdf_result = conn.execute(text(f"""
                    INSERT INTO {SCHEMA}.scrape_registry
                        (pwsid, url, url_source, content_type, status, notes)
                    VALUES
                        (:pwsid, :url, :url_source, :content_type, 'pending', :notes)
                    ON CONFLICT (pwsid, url) DO NOTHING
                """), {
                    "pwsid": pwsid,
                    "url": pdf_url,
                    "url_source": url_source,
                    "content_type": _detect_content_type(pdf_url),
                    "notes": notes + " [pdf]",
                })
                if pdf_result.rowcount > 0:
                    secondary_inserted += 1
                    logger.info(f"    + PDF URL: {pdf_url[:60]}")

            conn.commit()

    return {
        "primary_inserted": primary_inserted,
        "secondary_inserted": secondary_inserted,
        "verification_flagged": verification_flagged,
        "skipped": skipped,
    }


def print_summary(
    meta: dict,
    all_entries: list[dict],
    filtered: list[dict],
    insert_summary: dict,
    dry_run: bool = False,
):
    """Print structured summary report."""
    resolved = [e for e in all_entries if e.get("_pwsid_status") == "RESOLVED"]
    pre_set = [e for e in all_entries if e.get("_pwsid_status") == "PRE_SET"]
    not_found = [e for e in all_entries if e.get("_pwsid_status") == "PWSID_NOT_FOUND"]
    ambiguous = [e for e in all_entries if e.get("_pwsid_status") == "PWSID_AMBIGUOUS"]
    covered = [e for e in all_entries if e.get("_coverage_status") == "ALREADY_COVERED"]
    queued = [e for e in all_entries if e.get("_coverage_status") == "ALREADY_QUEUED"]

    prefix = "[DRY RUN] " if dry_run else ""

    print(f"\n{'=' * 60}")
    print(f"{prefix}INGESTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"\nFILE: {CURATION_FILE.name}")
    print(f"  Total entries in file:       {len(all_entries) + len(filtered)}")
    print(f"  Filtered (dup/info/sewer):   {len(filtered)}")
    print(f"  PWSID pre-set:               {len(pre_set)}")
    print(f"  PWSID resolved:              {len(resolved)}")
    print(f"  PWSID not found:             {len(not_found)}")
    print(f"  PWSID ambiguous:             {len(ambiguous)}")
    print(f"  Already covered (skipped):   {len(covered)}")
    print(f"  Already queued (skipped):    {len(queued)}")
    print(f"  Inserted (primary URL):      {insert_summary['primary_inserted']}")
    print(f"  Inserted (secondary URLs):   {insert_summary['secondary_inserted']}")
    print(f"  Needs verification (flagged):{insert_summary['verification_flagged']}")

    total_urls = (
        insert_summary["primary_inserted"] + insert_summary["secondary_inserted"]
    )
    print(f"\n  New PWSIDs added to scrape_registry:  {insert_summary['primary_inserted']}")
    print(f"  Total URLs inserted:                  {total_urls}")

    # Manual follow-up
    followup = not_found + ambiguous
    needs_verif = [
        e for e in all_entries
        if e.get("scraper_flag") == "NEEDS_VERIFICATION"
        and e.get("_pwsid_status") in ("RESOLVED", "PRE_SET")
    ]

    if followup or needs_verif:
        print(f"\n  Entries requiring manual follow-up:   {len(followup) + len(needs_verif)}")
        print(f"\nMANUAL FOLLOW-UP REQUIRED:")
        for e in followup:
            print(
                f"  {CURATION_FILE.name} | id={e['id']} | {e['utility_name']} | "
                f"{e.get('_pwsid_status')}"
            )
            if e.get("_candidates"):
                for c in e["_candidates"]:
                    print(f"    candidate: {c}")
        for e in needs_verif:
            print(
                f"  {CURATION_FILE.name} | id={e['id']} | {e['utility_name']} | "
                f"NEEDS_VERIFICATION (inserted with url_unconfirmed)"
            )

    print(f"\n{'=' * 60}")


def main():
    """Main entry point for script execution."""
    parser = argparse.ArgumentParser(
        description="Ingest Denver/CO Springs metro curation into scrape_registry"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview without DB writes"
    )
    args = parser.parse_args()

    logger.info("=== Denver/CO Springs Metro Curation Ingestion ===")

    # Step 1: Load
    logger.info("\n--- Step 1: Load curation file ---")
    meta, utilities = load_curation_file(CURATION_FILE)

    # Read colorado_structural_notes
    structural_notes = meta.get("colorado_structural_notes") or \
        utilities[0] if False else None  # notes are in the parent JSON
    # Actually read from the raw JSON
    with open(CURATION_FILE) as f:
        raw = json.load(f)
    co_notes = raw.get("colorado_structural_notes", [])
    if co_notes:
        logger.info(f"Colorado structural notes ({len(co_notes)} items) — read and noted")

    # Step 2: Filter
    logger.info("\n--- Step 2: Filter entries ---")
    insertable, filtered = filter_entries(utilities)
    logger.info(f"After filtering: {len(insertable)} insertable, {len(filtered)} filtered")

    # Step 3: Resolve PWSIDs
    logger.info("\n--- Step 3: Resolve PWSIDs from SDWIS ---")
    insertable = resolve_pwsids(insertable, state_codes=["CO"])

    # Step 4: Check coverage
    logger.info("\n--- Step 4: Check existing coverage ---")
    insertable = check_coverage(insertable)

    # Step 5: Insert
    logger.info("\n--- Step 5: Insert into scrape_registry ---")
    insert_summary = insert_entries(insertable, meta, dry_run=args.dry_run)

    # Step 6: Summary
    print_summary(meta, insertable, filtered, insert_summary, dry_run=args.dry_run)

    if args.dry_run:
        logger.info("\n[DRY RUN] No DB writes performed")


if __name__ == "__main__":
    main()

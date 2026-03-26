# Session Summary — Sprint 18d: Source Provenance + Duke NIEPS Production Ingest

**Date:** 2026-03-26
**Duration:** ~1 session
**Commit:** f7ba543

## What Was Done

### 1. Source Provenance Schema (Migration 016)

Extended `source_catalog` with 16 licensing/distribution columns so every rate record
can be traced to its source, license, and distribution tier at query time. This is the
foundation for the free_attributed tier that separates CC BY-NC-ND academic data from
premium commercial data.

New columns: `license_spdx`, `license_url`, `license_summary`, `commercial_redistribution`,
`attribution_required`, `attribution_text`, `share_alike`, `modifications_allowed`, `tier`,
`tier_rationale`, `data_vintage`, `collection_date`, `upstream_sources`, `transformation`,
`citation_doi`, `source_url`.

Four tiers: free_open (EPA/Census), free_attributed (Duke NIEPS), premium (EFC/TML/scraped),
internal_only (reference data with unclear licenses).

### 2. Duke NIEPS Production Ingest

New module `duke_nieps_ingest.py` writes Duke 10-state data to the canonical `rate_schedules`
table with full JSONB rate structures. This supersedes the internal-only `duke_reference_ingest.py`
which wrote to a separate `duke_reference_rates` table.

**Key design decisions:**
- Duke data is `free_attributed`: served via API for free with attribution, never paywalled
- Original rate values preserved intact (satisfies ND clause)
- No one pays for this data (satisfies NC clause)
- Attribution text embedded in source_catalog entry

**Coverage:** 3,177 records across 10 states.

**Unit normalization:** Duke data has both gallons and cubic feet vol_units depending on state.
PA (18% CF), NJ (17% CF), CT (42% CF) use cubic feet for some utilities. The ingest converts
all tier boundaries and rates to gallons/$/1000gal before storage.

**Bill distributions (monthly @10CCF):**
- TX: avg $46.45 (min $5, max $159)
- CA: avg $60.79 (min $11, max $521)
- NC: avg $45.15 (min $9, max $114)
- PA: avg $59.29 (min $1, max $322)
- KS: avg $59.15 (min $12, max $137)
- WA: avg $46.28 (min $11, max $143)

## Files Changed

| File | Action | Purpose |
|------|--------|---------|
| `migrations/versions/016_add_provenance_columns_to_source_catalog.py` | Created | Add provenance columns |
| `src/utility_api/models/source_catalog.py` | Modified | ORM fields for provenance |
| `src/utility_api/ingest/duke_nieps_ingest.py` | Created | Production Duke ingest |
| `src/utility_api/cli/ingest.py` | Modified | `ua-ingest duke-nieps` command |
| `docs/next_steps.md` | Modified | Sprint 18d section |

## Key Context for Future Sessions

- **Migration 016 not yet applied** — needs `alembic -c migrations/alembic.ini upgrade head`
- **Ingest not yet run live** — only dry-run tested. Run `ua-ingest duke-nieps --all --seed-catalog`
- **duke_reference_rates** table is now a legacy artifact; retained but no longer the primary target
- **NM low yield** (50 of 697 PWSIDs): most NM utilities only have sewer data in Duke dataset
- **KS low yield** (411 of 952): similar — many KS utilities are sewer-only in Duke data
- The provenance schema is designed for all sources, not just Duke. Existing source_catalog
  entries should be backfilled with tier/license fields in a future session.

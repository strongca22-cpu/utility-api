# Session Summary — Sprint 19a: IN IURC + KY PSC State Ingest

**Date:** 2026-03-26
**Commits:** 00611f3, (final)

## What Was Done

### Source 1: Indiana IURC Annual Water Bill Analysis

- **Module:** `in_iurc_ingest.py`
- **Pattern:** Single PDF → text extraction → regex parsing → fuzzy PWSID match → `water_rates`
- **Results:** 80 utilities parsed, 58 matched (72.5%), 14 unmatched, 8 duplicates resolved
- **Bill distribution:** Avg $67.00 at 10 CCF (range $11–$128)
- **Key issues:**
  - Indiana American multi-row structure (parent + sub-areas) partially handled
  - Area Two/Three/Four sub-areas didn't match (would need manual PWSID mapping)
  - Some small NFPs have no SDWIS match (too small or named differently)

### Source 2: Kentucky PSC Water Tariff Directory

- **Module:** `ky_psc_ingest.py`
- **Pattern:** IIS directory crawl → per-utility PDF download → Claude Haiku parse → fuzzy match → `rate_schedules`
- **Results:** 136 directories, 134 PDFs, 98 parsed, 84 matched and inserted
- **Bill distribution:** Median $76.66 at 10 CCF (range $5–$333)
- **Key issues:**
  - 36 parse failures — mostly wholesale-only tariffs or unusual PDF layouts
  - Northern Kentucky Water District is an outlier ($333 at 10 CCF) — may need review
  - 12 unmatched utilities — name matching improvements could recover some

## Files Changed

| File | Action |
|------|--------|
| `src/utility_api/ingest/in_iurc_ingest.py` | Created |
| `src/utility_api/ingest/ky_psc_ingest.py` | Created |
| `src/utility_api/cli/ingest.py` | Modified (added in-iurc, ky-psc commands) |
| `docs/ingest_briefs/` | Created (specs from user) |
| `docs/next_steps.md` | Updated |

## Database State After Session

- `rate_schedules`: 5,679 rows, 3,687 unique PWSIDs
- `water_rates`: 6,804 rows, 6,178 unique PWSIDs
- New states with coverage: IN (58), KY (84)

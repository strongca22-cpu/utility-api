# Sprint 28: Duke NIEPS QA Cross-Reference Analysis

**Date:** 2026-04-02
**Commits:** `e4c644b`, `3e568b2`

## What Was Done

### 1. QA Cross-Reference Analysis (Task 1-5)
Compared 1,525 PWSIDs with both `duke_nieps_10state` and `scraped_llm` data.

**Key findings:**
- Median scraped bill is +36.5% higher than Duke (expected: 4-year vintage gap)
- 15.7% agree within 10%, 49.8% disagree by >50%
- 87% of large disagreements have scraped higher (consistent with real rate inflation)
- **108 PWSIDs** with 5x+ bill differences — probable unit/page errors in scraped data
- **60 PWSIDs** with NULL scraped bills where Duke has valid values
- **319 PWSIDs** where scraped says "uniform" but Duke shows tiered structure
- NJ (+87%) and OR (+336%) have worst agreement — likely scraping errors mixed with rate increases

**Deliverables:** `docs/duke_qa_cross_reference.md`, `scripts/duke_qa_analysis.py`

### 2. Duke Data Comparability Migration
Patched all 3,177 Duke records in `rate_schedules` for structural parity with scraped_llm:
- Stripped `frequency` key from fixed_charges JSONB (3,160 records)
- Made tier boundaries contiguous (2,300 records had rounding gaps)
- Removed duplicate tiers (222 records)
- Recalculated bills after tier fixes (1,031 shifted)
- Set nuanced confidence: high (2,340), medium (835), low (2) — was all "high"
- Flagged 262 records for review

**Migration script:** `scripts/migrate_duke_to_comparable.py`

### 3. Ingest Updates for Future Runs
Updated `duke_nieps_ingest.py`:
- Removed `frequency` from fixed_charges dict
- Added dedup + contiguity passes after tier sort
- Added `_assign_confidence()` for nuanced confidence

### 4. Export + Dashboard Updates
- Duke PWSIDs now included in `has_rate_data` (was excluded as "reference only")
- Export uses DB confidence instead of hardcoded "low"
- Source URLs shown for Duke (was suppressed)
- Dashboard: replaced incorrect "may be inaccurate" caveat with CC BY-NC-ND licensing notice
- Reference tier renders at full opacity (was 0.8x)
- Reference sublabel updated: "Duke NIEPS (CC BY-NC-ND)"

## What Did NOT Change
- Duke stays as `data_tier = "reference"` (amber) — CC BY-NC-ND license restriction
- Source priority stays at 8 (last resort fallback)
- No changes to scraped_llm data
- No writes to legacy `water_rates` or `duke_reference_rates` tables

## Coverage Impact
- "Reference only" dropped from ~1,125 to 1 (edge case with NULL bill)
- "With rate data" increased accordingly
- Lower 48 population coverage: 91.2%

## Actionable Follow-ups
1. **Re-parse 108 scraped PWSIDs** with 5x+ bill differences (American Water PA tariffs, Middlesex Water NJ/CT)
2. **Re-crawl 60 NULL extraction failures** where Duke has valid bills
3. **Spot-check 20-30 of 319 "uniform" scraped / "tiered" Duke** to determine if LLM is missing tiers
4. **Push dashboard changes to VPS** — manual push + restart needed

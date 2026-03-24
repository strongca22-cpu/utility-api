# Session Summary — Sprint 4: SWRCB eAR Bulk Rate Ingest

**Date:** 2026-03-24
**Duration:** ~30 min
**Scope:** CA SWRCB eAR bulk rate data ingest from HydroShare

## What Was Done

1. **Schema evolution** (migration 008):
   - Added `source` column to `water_rates` table
   - Added `bill_6ccf`, `bill_9ccf`, `bill_12ccf`, `bill_24ccf` columns
   - Updated unique constraint from `(pwsid, date)` to `(pwsid, date, source)` — allows duplicate records from different data sources
   - Backfilled existing scraped records with `source='scraped_llm'`

2. **HydroShare eAR data download and analysis:**
   - Downloaded `ear_annual_matrix_2022.xlsx` (17 MB, 7,228 CA systems, 2,978 columns)
   - Identified key rate columns: WRSFCostPerUOM (base charge), WRSFMetricUsage (tier limits), WRSFUsageCost (tier rates)
   - Verified bill calculation: bimonthly base + volumetric, then ÷ 2 = monthly equivalent. Cross-checked against EBMUD — perfect match.

3. **eAR ingest pipeline** (`ear_ingest.py`, 520 lines):
   - Reads formatted HydroShare Excel, maps SF residential tiers to water_rates schema
   - Normalizes billing period (monthly/bimonthly/quarterly) to monthly charges
   - Filters to PWSIDs in our DB (cws_boundaries ∩ mdwd_financials)
   - Idempotent: clears source-tagged records before reinserting
   - CLI: `ua-ingest ear --year 2022 [--dry-run]`

4. **Results:** 194/194 CA MDWD utilities ingested (100% match rate)
   - 188 with bill amounts, 187 with tier structure
   - 14 overlap with existing scraped records — several discrepancies suggest scraped rates include combined water+sewer

5. **Updated existing pipeline** (`rates.py`):
   - `_store_rate_record` now sets `source='scraped_llm'`
   - Delete-before-insert scoped to `source='scraped_llm'` (won't clobber eAR data)

## Key Findings

- **eAR is water-only by design** (state filing). Scraped rates for Vallejo (~3x), Redwood City (~2x), Livermore (~1.5x) are likely combined water+sewer. This validates the dual-source approach.
- **eAR Manteca reports ~$1/month** — obvious data quality issue in the state filing.
- **Sacramento shows close agreement** between scraped ($43/$50) and eAR ($44/$53) — good cross-validation.
- **194/194 match rate** means every CA utility in our MDWD list filed an eAR with rate data.

## Files Changed

- `migrations/versions/008_add_source_and_ear_bill_columns.py` (new)
- `src/utility_api/ingest/ear_ingest.py` (new)
- `src/utility_api/models/water_rate.py` (updated)
- `src/utility_api/cli/ingest.py` (updated)
- `src/utility_api/ingest/rates.py` (updated)

## Continued in Same Session

### eAR 2020 + 2021 Ingest
- Column indices differ across years (2020: 1314 cols at different positions)
- Fixed with dynamic name-based header lookup (replaces hardcoded indices)
- 2020: 194 records (no bill columns in this year)
- 2021: 193 records (1 system missing rate data)
- Total water_rates: 677 records (96 scraped + 581 eAR across 3 years)

### CivicPlus DocumentCenter Crawler
- Initial approach (folder tree navigation via Playwright) was too slow — each folder expansion requires a click + wait
- Pivoted to **search-based approach**: renders CivicPlus site search, extracts links, scores by relevance
- Relevance classifier: regex patterns for strong positive ("water rate", "fee schedule"), moderate positive ("water", "rate"), and negative ("building permit", "water quality", "parking")
- Tested on 3 sites:
  - Fredericksburg: top result "Ord25-06 Amending Water and Sewer Fees Rates and Charges" (score +23)
  - Martinsville: "Schedule of Water and Sewer Rates (PDF)" (+8.5) — matches our curated URL
  - Colonial Heights: "Tax & Utility Rates" (+1.0) — correct page but lower score due to generic title
- CLI: `ua-ingest civicplus-crawl --domain fredericksburgva.gov`

## What's Next

1. Run CivicPlus crawler on all known CivicPlus utilities missing rate URLs
2. Feed discovered URLs into rate parsing pipeline
3. OWRS ingest (machine-readable CA rate specs)
4. Reconciliation framework for duplicate source records
5. Cross-year rate change analysis (2020-2022)

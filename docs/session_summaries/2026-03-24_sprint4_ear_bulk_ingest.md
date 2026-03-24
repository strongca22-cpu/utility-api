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

## What's Next

1. CivicPlus DocumentCenter crawler (main Sprint 4 deliverable #2)
2. Ingest eAR 2020 + 2021 for time-series analysis
3. OWRS ingest (machine-readable CA rate specs)
4. Reconciliation framework for duplicate source records

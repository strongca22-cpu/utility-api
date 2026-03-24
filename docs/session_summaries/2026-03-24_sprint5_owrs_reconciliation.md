# Session Summary — Sprint 5: OWRS Ingest + Reconciliation Diagnostic

**Date:** 2026-03-24
**Duration:** ~45 min
**Scope:** OWRS data ingest + cross-source reconciliation diagnostic

## What Was Done

### 1. OWRS Research and Download
- Cloned CA Data Collaborative's Open-Water-Rate-Specification GitHub repo
- Discovered OWRS-Analysis repo has a pre-computed `summary_table.csv` with:
  - 419 rows, 386 unique PWSIDs, effective dates 2002-2021
  - PWSID crosswalk already resolved (no fuzzy matching needed)
  - Tier structures in newline-delimited format, bill amounts pre-computed
  - 61 utilities report in kgal instead of CCF

### 2. OWRS Ingest Pipeline (`owrs_ingest.py`)
- Parses CSV summary table, maps to water_rates schema
- Handles 3 rate types: tiered (291), uniform (109), budget-based (16)
- Unit conversion: kgal → CCF for limits (×1.337) and prices (×0.748)
- Billing frequency normalization: bimonthly ÷2, quarterly ÷3
- Budget-based tiers: stores prices but limits are NULL (allocation-relative)
- Bill recalculation at standard 5/10 CCF from tier structure
- Deduplicates 5 multi-district utilities sharing PWSIDs
- FK constraint: only inserts PWSIDs existing in cws_boundaries
- Result: **387 records inserted**, 443 total unique PWSIDs with rate data

### 3. Reconciliation Diagnostic (`scripts/reconcile_rates.py`)
- Per-record quality flagging: tier inflation, stale vintage, suspicious bills
- Cross-source variance via comparable bill (interpolated 10CCF for eAR)
- Divergence cause classification: inflation, combined charges, vintage gap, unexplained
- Combined water+sewer detection: scraped > 1.5× median of other sources
- CSV export for detailed review

## Key Findings

### eAR Tier Limit Inflation (Systematic)
- 88 records across 54 utilities have tier limits ~1000× too high
- Likely gallons reported as HCF in the state filing
- eAR 2022 partially corrected (Escondido: 7000→7 CCF)
- Causes bill computations of $5,000-$60,000/month — obviously wrong

### Suspected Combined Water+Sewer Scrapes
| Utility | Scraped @10CCF | Other Median | Ratio |
|---------|---------------|-------------|-------|
| Vallejo | $183 | $49 | 3.7x |
| Redwood City | $138 | $61 | 2.2x |
| San Diego | $121 | $56 | 2.1x |
| EBMUD | $110 | $56 | 2.0x |

### Variance Distribution
- 38% agree (<10% CV) — strong cross-validation
- 32% moderate (10-25%) — mostly vintage differences
- 15% divergent (25-50%) — mix of vintage + source differences
- 7% major (50-100%) — likely data quality issues
- 1% conflict (>100%) — eAR inflation artifacts

## Files Created/Modified

- `src/utility_api/ingest/owrs_ingest.py` (new, 530 lines)
- `src/utility_api/cli/ingest.py` (updated — added `owrs` command)
- `scripts/reconcile_rates.py` (new, 399 lines)
- `data/raw/owrs_summary_table.csv` (downloaded, gitignored)
- `data/raw/owrs_repo/` (shallow clone, gitignored)
- `data/interim/rate_reconciliation.csv` (generated report)

## Decisions Made

- **Summary table over YAML parsing**: The OWRS-Analysis summary table has PWSIDs already resolved and bills pre-computed. Parsing 492 individual YAML files would yield the same data with much more complexity.
- **Cross-year analysis tabled**: User direction — current-state accuracy matters more than historical trends. eAR tier inflation must be fixed first anyway.
- **Reconciliation = diagnostic only**: Flagging issues, not resolving them. Resolution methodology needs discussion (vintage priority, source trust hierarchy).
- **Budget-based tier limits stored as NULL**: Tier starts like "indoor", "100%" are allocation-relative, not absolute CCF values. Can't compute bills without household-specific water budgets.

## What's Next

1. **Fix eAR tier limit inflation** — 54 utilities need correction
2. **Re-parse 8 combined water+sewer scraped rates** — water-only prompt
3. **Design "best estimate" logic** — source priority × vintage × confidence
4. **Cross-year eAR analysis** — after tier data is clean

# Session Summary: EFC Bulk Source Audit — Pilot (4 States)

**Date:** 2026-04-02
**Sprint:** 28 (EFC audit)
**Duration:** Single session

## Objective
Audit the 4 largest EFC state survey sources using the Duke NIEPS audit-and-fix pattern. Assess JSONB format, cross-reference against scraped_llm, fix issues, simplify bill calculation.

## Key Outcomes

### 1. JSONB Format — Already Clean
The EFC generic ingest (Sprint 18b) wrote canonical JSONB from the start. Zero records needed structural fixes (no extra keys, no contiguity gaps, no duplicate tiers). This is a non-issue for EFC sources.

### 2. Bills vs. Tiers — Opposite Reliability
Critical discovery: EFC and scraped_llm have **opposite reliability hierarchies**:
- scraped_llm: tiers extracted from PDFs are primary, bills calculated from tiers
- EFC: bills from curve are primary, tiers reverse-engineered from curve

Implication: never overwrite EFC bills with tier-recalculated bills. The 6.2% median divergence between curve bills and tier-reconstructed bills confirms the tiers are a lossy approximation.

### 3. Confidence Recalibrated
1,220 records (55%) changed from "high" to "medium" — all 1-tier uniform structures. Applied live to pilot 4 states. Generalized script ready for all 18 EFC states.

### 4. Bill Calculation Simplified
Replaced `_interpolate_bill()` + `_compute_monthly_bill()` with `_bill_from_curve()`:
- Snap to nearest curve point when within 10% of curve increment
- Simple interpolation when splitting between points
- Handles both 500-gal and 1000-gal increment curves
- No extrapolation beyond curve maximum

### 5. Head-to-Head (N=36)
Median +19% scraped higher than EFC. Partially explained by vintage gap (EFC 3-8 years older). AR flagged as systematic bias. Sample too small for robust statistics — will improve as scraped coverage grows.

## Files Changed
- **New:** `scripts/efc_qa_analysis.py`, `scripts/migrate_efc_to_comparable.py`, `docs/efc_pilot_audit_report.md`
- **Modified:** `src/utility_api/ingest/efc_generic.py` (bill calc simplification)
- **Archived:** `src/utility_api/ingest/legacy/efc_fl_ingest__old_*.py`, `efc_nc_ingest__old_*.py`

## Database Changes
- 1,220 records: confidence "high" → "medium" (pilot 4 EFC sources)
- Logged to pipeline_runs as `efc_confidence_recalibration`

## Next Steps
- Run confidence recalibration on remaining 14 EFC states (`--all-efc`)
- Re-run QA analysis as scraped_llm overlap grows
- Flag GA vintage = 1500 anomalies
- Investigate AZ/NH bill outliers

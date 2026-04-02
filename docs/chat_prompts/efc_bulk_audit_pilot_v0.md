# EFC Bulk Source Audit — Pilot (4 Largest States) v0

## Context
Sprint 28 established an audit-and-fix pattern for bulk data sources using Duke NIEPS as the template. The approach: compare against scraped_llm, find structural issues, ensure comparable storage format in rate_schedules, and fix anything that prevents clean comparability.

**Config-driven tier management is now in place** — `config/source_priority.yaml` controls both selection priority and dashboard tier. All 31 source_keys are registered.

## Objective
Audit the 4 largest EFC state survey sources as a pilot. Apply the same pattern as Duke: ingest audit → head-to-head comparison → flag errors → fix storage format → document findings.

## Target Sources (pilot)

| Source Key | PWSIDs | Sole-Source | Overlap w/ Scraped | Vintage |
|-----------|--------|-------------|-------------------|---------|
| efc_ar_2020 | 599 | 587 | 12 | 2002-2021 |
| efc_ia_2023 | 570 | 561 | 9 | 2004-2023 |
| efc_wi_2016 | 569 | 567 | 2 | 1975-2024 |
| efc_ga_2019 | 488 | 482 | 6 | 1500-2021 |

**Total: 2,226 PWSIDs, 2,197 sole-source.** These 4 states represent ~40% of all EFC records.

## Tasks

### Task 1: EFC Ingest Audit
- Find the EFC ingest code (likely in `src/utility_api/ingest/` or migrated from `water_rates`)
- Trace: source file/API → field mapping → rate_schedules columns
- What format are EFC surveys in? CSV? Excel? Dashboard scrape?
- How are bills calculated? Are they pre-computed or calculated from tiers?
- What unit conversions happen?
- Are fixed_charges and volumetric_tiers JSONB populated, or are these bill-only records?
- What's the vintage story? Per-utility or uniform per state?

### Task 2: Head-to-Head Comparison
For the ~29 PWSIDs with both EFC and scraped_llm (across all 4 states):
- Compare bill_10ccf values
- Same analysis as Duke: % difference distribution, agreement buckets
- Note: small overlap (29 total) — may not be statistically meaningful, but shows if there's systematic bias

### Task 3: Storage Format Comparability
Compare EFC records' JSONB structure against scraped_llm:
- fixed_charges: same keys? (name, amount, meter_size)
- volumetric_tiers: same keys? (tier, min_gal, max_gal, rate_per_1000_gal)
- Are tier boundaries contiguous?
- Any duplicate tiers?
- rate_structure_type: canonical values or non-standard?

### Task 4: Fix Storage Issues
Apply the Duke template:
- Strip any extra JSONB keys not in the canonical format
- Fix tier contiguity gaps
- Remove duplicate tiers
- Recalculate bills if tier structure changed
- Set nuanced confidence levels

### Task 5: Document Findings
Create `docs/efc_pilot_audit_report.md` with:
- Ingest pipeline documentation
- Comparison statistics
- Issues found and fixed
- Recommendations for remaining 14 EFC states

## Key Files
- `src/utility_api/ingest/` — look for EFC ingest code
- `scripts/migrate_to_rate_schedules.py` — may have migrated EFC from water_rates
- `scripts/sync_water_rates_to_rate_schedules.py` — may have synced EFC
- `config/source_priority.yaml` — tier/priority config
- `scripts/duke_qa_analysis.py` — template for comparison queries (adapt for EFC)
- `scripts/migrate_duke_to_comparable.py` — template for fix script

## What NOT to Change
- Do not modify scraped_llm data
- Do not change source_priority.yaml priorities (may change confidence after audit)
- Do not write to water_rates table — all changes target rate_schedules
- Do not re-run EFC ingest — this is a patch to existing records

# Bulk Source Audit — Batch B (TX TML, OWRS, NM NMED, IN IURC) v0

## Context
Sprint 28 established the audit-and-fix pattern for bulk data sources. Five sources are now audited:

| Source | Records | Key Fixes | Status |
|--------|---------|-----------|--------|
| Duke NIEPS | 3,177 | Contiguity, dedup, confidence recalibration | Done |
| EFC (20 states) | 5,611 | 1,220 high→medium (1-tier cap) | Done |
| CA eAR (3 vintages) | 581 | Inflation verified, bill_10ccf backfilled, confidence recalibrated | Done |
| KY PSC | 84 | `frequency` key stripped, 1-gal gaps closed, bill bug fixed, ingest code patched | Done |
| WV PSC | 241 | 155 high→medium (all single-tier) | Done |

**Four bulk sources remain unaudited.** This session works through them one at a time.

## Audit Pattern (established Sprint 28)
For each source:
1. **Ingest audit** — trace how data flows from source to rate_schedules
2. **JSONB format check** — canonical keys? Extra keys? Contiguity? Duplicates?
3. **Confidence recalibration** — apply Duke criteria (high = bill_10ccf in [10,200] + tier_count >= 2, 1-tier capped at medium)
4. **Head-to-head** — compare bill_10ccf with scraped_llm where overlap exists
5. **Fix issues** — strip extra keys, close gaps, flag outliers
6. **Document** — audit report in docs/, update next_steps.md

## Target Sources (in priority order)

### 1. TX TML (tx_tml_2023) — 476 records, TX only
- **Structure:** All `bill_only` (no JSONB tiers or fixed_charges at all)
- **Overlap:** 52 pairs with scraped_llm (best overlap of remaining sources)
- **Expected work:** Confidence recalibration only (no JSONB to fix). Main value is H2H comparison.
- **Key question:** All 476 at "medium" confidence currently — correct for bill-only records?

### 2. OWRS (owrs) — 387 records, CA only
- **Structure:** Has tiers (387) and fixed_charges. Clean JSONB keys. Has `budget_based` rate_structure_type (unique).
- **Overlap:** 25 pairs with scraped_llm
- **Confidence:** 371 at "high" — many are 1-tier uniform, may need downgrade
- **Expected work:** Confidence recalibration. Check if `budget_based` records need special handling.

### 3. NM NMED (nm_nmed_rate_survey_2025) — 175 records, NM only
- **Structure:** All `uniform`, bill-only (no JSONB tiers or fixed_charges)
- **Overlap:** 17 pairs with scraped_llm
- **Expected work:** Minimal — confidence check, H2H, document.

### 4. IN IURC (in_iurc_water_billing_2024) — 58 records, IN only
- **Structure:** All `uniform`, bill-only (no JSONB tiers or fixed_charges)
- **Overlap:** 2 pairs with scraped_llm
- **Expected work:** Minimal — confidence check, document.

## Key Files
- `scripts/efc_qa_analysis.py` — generalizable QA analysis template
- `scripts/migrate_efc_to_comparable.py` — confidence recalibration template
- `config/source_priority.yaml` — source priority/confidence/display_tier config
- Existing audit reports: `docs/efc_pilot_audit_report.md`, `docs/ca_ear_audit_report.md`, `docs/ky_psc_audit_report.md`, `docs/wv_psc_audit_report.md`

## Key Ingest Files
- `src/utility_api/ingest/tx_tml_ingest.py` (if exists)
- `src/utility_api/ingest/owrs_ingest.py` (if exists)
- `src/utility_api/ingest/nm_nmed_ingest.py` (if exists)
- `src/utility_api/ingest/in_iurc_ingest.py` (if exists)
- Check `scripts/` for any related ingest scripts

## What NOT to Change
- Do not modify scraped_llm data
- Do not re-run any ingest — this is patch-to-existing only
- Do not overwrite source-authoritative bills
- Do not write to water_rates table — all changes target rate_schedules

## Deliverables
For each source:
- Migration script: `scripts/migrate_{source}_to_comparable.py`
- Audit report: `docs/{source}_audit_report.md`
- Updated `docs/next_steps.md`
- Session summary in `docs/session_summaries/`
- Git commit per source

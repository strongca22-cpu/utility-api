# Session Summary — Sprint 28 Batch B Bulk Source Audits

**Date:** 2026-04-02
**Sprint:** 28
**Chat prompt:** `docs/chat_prompts/bulk_source_audit_batch_b_v0.md`

---

## Objective

Audit the four remaining unaudited bulk data sources using the Sprint 28 audit-and-fix pattern established with Duke NIEPS, EFC, CA eAR, KY PSC, and WV PSC.

## Sources Audited

### 1. TX TML (tx_tml_2023) — 476 records
**Key action: Bill normalization (gallon → CCF)**
- TML reports at 5,000/10,000 gallons, not 5/10 CCF — required 2-point linear interpolation
- Model: F (implied fixed) + R (volumetric rate) from two data points
- 471 records: 2-point model. 5 records: 1-point proportional (bill_10ccf = 2 × bill_5ccf)
- 23 records with negative implied F (increasing block pattern)
- bill_20ccf populated for all 476 via extrapolation
- 1 outlier flagged (TX0700059, normalized bill_10ccf=$204.95)
- Original gallon values preserved in parse_notes
- H2H (N=160): TML systematically lower than scraped (79%), median diff 41%

### 2. OWRS (owrs) — 387 records
**Key action: Confidence recalibration**
- JSONB already clean (water_rate_to_schedule helper). Bills already at CCF benchmarks.
- 104 records downgraded: high → medium (1-tier cap per Duke criteria)
- 267 multi-tier records correctly remain high
- 16 budget_based records already at medium — no change
- 0 outliers
- H2H (N=183): OWRS systematically lower (72%), median diff 35%. Vintage gap (2002-2021 vs 2024-2025).

### 3. NM NMED (nm_nmed_rate_survey_2025) — 175 records
**Key action: bill_20ccf backfill**
- Bills already CCF-normalized in ingest (proportional from 6,000 gal)
- bill_20ccf backfilled: 2 × bill_10ccf for all 175 records
- 1 outlier flagged (NM3561101, $263.21)
- Confidence stays medium (all bill_only, 0 tiers)
- H2H (N=18): Best of Batch B at 23.7% median diff

### 4. IN IURC (in_iurc_water_billing_2024) — 58 records
**Key action: bill_20ccf backfill**
- Bills already CCF-normalized in ingest (proportional from 4,000 gal)
- bill_20ccf backfilled: 2 × bill_10ccf for all 58 records
- 0 outliers
- Confidence stays medium (all bill_only, 0 tiers)
- H2H (N=41 raw pairs): Mixed results, some excellent matches (<3%), some large divergences (>100%)

## Deliverables

| Source | Migration Script | Audit Report | Commit |
|--------|-----------------|--------------|--------|
| TX TML | `scripts/migrate_tx_tml_to_comparable.py` | `docs/tx_tml_audit_report.md` | 14ea914 |
| OWRS | `scripts/migrate_owrs_to_comparable.py` | `docs/owrs_audit_report.md` | 5b4f7db |
| NM NMED | `scripts/migrate_nm_nmed_to_comparable.py` | `docs/nm_nmed_audit_report.md` | 3a3bf0e |
| IN IURC | `scripts/migrate_in_iurc_to_comparable.py` | `docs/in_iurc_audit_report.md` | fb47b4d |

## Cross-Source Observations

1. **Bill-only sources (TML, NMED, IURC) share a fundamental limitation:** Without tier structure data, we can only model bills proportionally. The proportional model assumes no fixed charge, which systematically underestimates low-consumption bills and overestimates high-consumption bills.

2. **The $120.29 repeated value in scraped_llm** (642 PWSIDs) inflates H2H noise. Worth investigating separately — likely a template/default value from a batch scrape.

3. **Vintage gap is the dominant driver** of H2H divergence across all sources. OWRS (2002-2021), TML (2023), NMED (2024), and IURC (2024) all show scraped_llm bills higher, consistent with rate inflation over time.

4. **All 9 bulk sources are now audited.** Combined: 10,790 records across Duke NIEPS, EFC, CA eAR, KY PSC, WV PSC, TX TML, OWRS, NM NMED, and IN IURC.

## Files Changed
- `scripts/migrate_tx_tml_to_comparable.py` (new)
- `scripts/migrate_owrs_to_comparable.py` (new)
- `scripts/migrate_nm_nmed_to_comparable.py` (new)
- `scripts/migrate_in_iurc_to_comparable.py` (new)
- `docs/tx_tml_audit_report.md` (new)
- `docs/owrs_audit_report.md` (new)
- `docs/nm_nmed_audit_report.md` (new)
- `docs/in_iurc_audit_report.md` (new)
- `docs/next_steps.md` (updated × 4)

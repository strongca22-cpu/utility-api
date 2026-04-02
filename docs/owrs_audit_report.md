# OWRS Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (Batch B — bulk source audits)
**Scope:** owrs (California Data Collaborative Open Water Rate Specification)
**Total records:** 387 (381 unique PWSIDs, 6 multi-vintage PWSIDs)

---

## Executive Summary

- **JSONB is already clean.** Uses `water_rate_to_schedule()` helper — canonical keys (`tier`, `min_gal`, `max_gal`, `rate_per_1000_gal` for tiers; `name`, `amount`, `meter_size` for fixed charges). Zero contiguity issues. No structural fixes needed.
- **Bills are derived from tiers, not source-primary.** OWRS provides tier structures; bills are calculated in the ingest at true CCF benchmarks (5/10/20 CCF). No bill normalization needed (unlike TX TML).
- **Confidence recalibrated.** 104 records downgraded from "high" to "medium" — all 1-tier (uniform or collapsed budget-based) structures. 267 multi-tier records correctly remain "high."
- **No outliers.** All 387 records have bill_10ccf in [$6.88, $197.98] — within the [5, 200] range.
- **Budget-based rates (16 records)** are collapsed to single-tier uniform with a flat rate. Already at "medium" confidence — correct given the loss of budget allocation detail.
- **H2H with scraped_llm (N=183):** OWRS systematically lower (72% of pairs), median diff 35.1%. Driven by vintage gap (OWRS 2002-2021 vs scraped 2024-2025).

---

## Section 1: Ingest Pipeline Documentation

### Source
California Data Collaborative — Open Water Rate Specification (OWRS)
- **URL:** https://github.com/California-Data-Collaborative/Open-Water-Rate-Specification
- **Format:** Pre-computed summary table CSV from OWRS-Analysis repo
- **File:** `data/raw/owrs_summary_table.csv`
- **Coverage:** ~386 unique CA utilities, residential single-family only
- **Vintage range:** 2002–2021 (most records 2015-2018)

### Data Flow
```
OWRS summary CSV → owrs_ingest.py → water_rate_to_schedule() → rate_schedules
```

1. **Parse:** CSV rows → utility name, PWSID, bill type (Uniform/Tiered/Budget), tier starts/prices, service charge, billing frequency, effective date
2. **Unit conversion:** 61 of 419 records report in kgal → converted to CCF (×1.337 for limits, ×0.748 for prices)
3. **Billing frequency normalization:** Bimonthly charges ÷2, quarterly ÷3 → monthly equivalents. Tier limits also divided by billing period.
4. **Bill calculation:** `_calculate_bill()` applies tier structure at 5, 10, 20 CCF — these are derived values, not source-primary
5. **PWSID filter:** Only PWSIDs in `cws_boundaries` table (FK constraint)
6. **Dedup:** Same PWSID + effective_date → keep first. 6 PWSIDs have legitimate multi-vintage records.
7. **Write:** Via `water_rate_to_schedule()` + `write_rate_schedule()` → canonical JSONB format

### Key Design Decisions

**Bills vs. Tiers — reliability hierarchy:**

| | OWRS | scraped_llm |
|---|---|---|
| Tier data | From YAML rate specs — explicit, authoritative | LLM-extracted from PDFs |
| Bills | Calculated from tiers — derived | Calculated from tiers — derived |
| Primary truth | Tiers | Tiers |

Both sources derive bills from tier structures. OWRS tiers are machine-readable YAML specs (high fidelity). Scraped tiers are LLM-extracted (variable fidelity). OWRS tier data is authoritative for its vintage.

**Budget-based rates:**
- 16 records have `rate_structure_type='budget_based'` — unique to OWRS
- Budget-based rates use allocation-specific tier boundaries (e.g., "indoor", "100%+ of budget")
- The ingest collapses these to a single uniform rate (the base tier price)
- Bill calculations use only this flat rate — bills may underestimate for utilities with steep overage penalties
- All 16 correctly at "medium" confidence

---

## Section 2: JSONB Storage Format Audit

### Fixed Charges
- **Keys:** `name`, `amount`, `meter_size` — canonical ✓
- **Empty:** 4/387 records have no fixed charge (pure volumetric)
- **No extra keys.** No contaminants (e.g., `frequency`, `billing_period`)

### Volumetric Tiers
- **Keys:** `tier`, `min_gal`, `max_gal`, `rate_per_1000_gal` — canonical ✓
- **Contiguity:** 0 issues (every tier's `min_gal` == previous tier's `max_gal`)
- **Coverage:** 387/387 records have at least 1 tier

### Tier Count Distribution

| tier_count | Count | Rate structure |
|------------|-------|---------------|
| 1 | 118 | 102 uniform + 16 budget_based (collapsed) |
| 2 | 70 | increasing_block |
| 3 | 131 | increasing_block |
| 4 | 68 | increasing_block |

**No fixes needed.** JSONB is clean across all 387 records.

---

## Section 3: Confidence Recalibration

### Criteria (Duke-established, Sprint 28)
- **high:** bill_10ccf in [10, 200] AND tier_count >= 2
- **medium:** bill_10ccf in [5, 500] OR tier_count <= 1
- **low:** bill_10ccf NULL or outside [5, 500]

### Results

| Change | Count | Detail |
|--------|-------|--------|
| High → medium | 104 | All 1-tier records (1-tier cap) |
| High kept | 267 | Multi-tier, bill in range |
| Medium kept | 16 | Budget-based (already medium) |
| Low | 0 | All bills in [5, 200] |
| Flagged review | 0 | No outliers |

**Post-recalibration distribution:**
- high: 267 (69%)
- medium: 120 (31%)

---

## Section 4: Head-to-Head Comparison (N=183)

### Methodology
- 183 best-match pairs (best scraped per PWSID, tier_count > 0)
- Excluded scraped records with tier_count=0 and $120.29 template values

### Results

| Metric | Value |
|--------|-------|
| Median % diff | 35.1% |
| Mean % diff | 44.6% |
| < 10% agreement | 21 (11%) |
| 10–25% | 44 (24%) |
| 25–50% | 62 (34%) |
| > 50% divergence | 56 (31%) |

### Direction
OWRS lower in 131/183 pairs (72%).

### Interpretation
The systematic OWRS-lower pattern is expected:
1. **Vintage gap:** OWRS data is 2002-2021 (most 2015-2018). Scraped is 2024-2025. California water rates have increased significantly (drought surcharges, infrastructure investment).
2. **OWRS is authoritative for its vintage.** The data quality is high (machine-readable YAML), but the rates are stale. A 2024 OWRS refresh would likely close much of this gap.
3. **11% strong agreement (<10% diff)** — these are likely utilities whose rates haven't changed significantly.

---

## Section 5: Actions Taken

| Action | Records affected | Details |
|--------|-----------------|---------|
| Confidence downgraded | 104 | high → medium (1-tier cap) |
| JSONB unchanged | 387 | Already clean canonical format |
| Bills unchanged | 387 | Already at true CCF benchmarks |
| Outliers flagged | 0 | All bills in [5, 200] |

### Pipeline Run Logged
- Step: `owrs_audit_migration`
- Status: success
- Logged to `pipeline_runs` table

---

## Section 6: Recommendations

1. **OWRS data is high quality but stale.** Machine-readable YAML specs are the gold standard for rate structure data. Worth checking if the California Data Collaborative has published updates since 2021.

2. **Budget-based rates deserve richer modeling.** The current collapse to single uniform rate loses the budget allocation structure. If budget-based pricing becomes important for CA analysis, consider storing the original allocation tiers in parse_notes or a separate JSONB field.

3. **6 multi-vintage PWSIDs** are legitimate (different effective dates). No dedup action needed, but downstream queries should filter by most recent vintage per PWSID.

---

## Appendix: Scripts & Output Files

| File | Purpose |
|------|---------|
| `scripts/migrate_owrs_to_comparable.py` | Migration script (confidence recalibration) |
| `src/utility_api/ingest/owrs_ingest.py` | Original ingest pipeline |
| `data/raw/owrs_summary_table.csv` | Source data file |

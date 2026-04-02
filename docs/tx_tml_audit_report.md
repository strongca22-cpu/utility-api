# TX TML Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (Batch B — bulk source audits)
**Scope:** tx_tml_2023
**Total records:** 476 PWSIDs (all TX)

---

## Executive Summary

- **Bill normalization required.** TML reports bills at 5,000 and 10,000 gallons, not at standard 5/10 CCF benchmarks. Bills were normalized using a 2-point linear interpolation model (implied fixed charge + volumetric rate). **All bill values in the database are now interpolated, not source-primary.**
- **Original values preserved.** Source gallon-based bills are stored in `parse_notes` as `source_bill_5000gal=XX.XX` and `source_bill_10000gal=XX.XX`.
- **JSONB is empty (by design).** TML is a bill-only source with no tier structure or fixed charge data. `fixed_charges=[]`, `volumetric_tiers=[]`.
- **Confidence stays at "medium" for all 476 records.** Correct per Duke criteria (bill_only = 0 tiers, high requires tier_count >= 2).
- **bill_20ccf populated for all 476 records** via extrapolation beyond the data range.
- **1 outlier flagged** for review (TX0700059, normalized bill_10ccf=$204.95).
- **H2H with scraped_llm (N=160 best-match pairs):** TML is systematically lower than scraped (79% of pairs), reflecting vintage gap (TML 2023 vs scraped 2024-2025) and methodological differences.

---

## Section 1: Ingest Pipeline Documentation

### Source
Texas Municipal League Annual Water Rate Survey
- **Format:** XLSX (old Excel format read via xlrd)
- **File:** `data/bulk_sources/tx_tml/tml_water_2023.xlsx`
- **Coverage:** TX municipalities only — cities self-report bills
- **Vintage:** 2023 (stored as `vintage_date = 2023-01-01`)

### Data Flow
```
TML XLSX → tml_tx_ingest.py → rate_schedules
```

1. **Parse:** Excel rows → city name, population, bill at 5,000 gal, bill at 10,000 gal
2. **Filter:** $0.00 values = "no response" (skipped). Population group headers and averages skipped.
3. **Dedup:** Duplicate city names resolved by keeping row with more non-null bill values
4. **Outlier correction:** Gregory $4,141 → $41.41 (decimal misplacement, corrected in ingest)
5. **Name match:** City name → SDWIS PWSID via normalized name match → city field match → stripped suffix match
6. **Write:** UPSERT to `rate_schedules` with `source_key='tx_tml_2023'`, `rate_structure_type='bill_only'`

### Key Design Decisions

**Consumption units — the critical issue:**

| Benchmark | TML reports at | Our standard | Gap |
|-----------|---------------|-------------|-----|
| "5 CCF" | 5,000 gal (6.68 CCF) | 3,740 gal (5.00 CCF) | TML is +34% more consumption |
| "10 CCF" | 10,000 gal (13.37 CCF) | 7,480 gal (10.00 CCF) | TML is +34% more consumption |

The original ingest stored gallon-based bills directly into `bill_5ccf` / `bill_10ccf` columns with a parse_notes annotation. This made TML bills **not directly comparable** with any other source.

**Resolution (this migration):** Interpolate to true CCF benchmarks using a 2-point linear model. See Section 3.

---

## Section 2: JSONB Storage Format Audit

**No fixes needed.** TML is a bill-only source:
- `fixed_charges = []` (empty array) — 476/476 records
- `volumetric_tiers = []` (empty array) — 476/476 records
- `tier_count = NULL` — 476/476 records
- `rate_structure_type = 'bill_only'` — 476/476 records

There is no structural JSONB data to audit. This is correct — TML provides total monthly bills, not rate components.

---

## Section 3: Bill Normalization (Gallon → CCF)

### Methodology

**2-point linear model** — solves for two unknowns from two data points:

```
Given: bill at 5,000 gal, bill at 10,000 gal
Solve:
  F (implied fixed charge) = 2 × bill_5000 - bill_10000
  R (volumetric rate/gal)  = (bill_10000 - bill_5000) / 5000

Interpolate:
  bill_5ccf  = F + R × 3,740    (5 CCF)
  bill_10ccf = F + R × 7,480    (10 CCF)
  bill_20ccf = F + R × 14,960   (20 CCF — extrapolated)
```

**Assumptions:**
1. Constant volumetric rate between 0 and 10,000 gal (no tiering within range)
2. Fixed charge is the y-intercept of the linear model
3. bill_20ccf extrapolates beyond the data range — flagged in parse_notes

### Coverage

| Category | Count | Method |
|----------|-------|--------|
| Both bills available | 471 | 2-point linear model |
| Only bill_5000 | 5 | Proportional scaling: `bill_5ccf = bill_5000 × 0.748`, `bill_10ccf = 2 × bill_5ccf` |
| Neither | 0 | — |

### Implied Rate Structure

The 2-point model reveals the implied rate structure of each utility:

| Implied structure | Count | Interpretation |
|-------------------|-------|----------------|
| Positive F (fixed charge + volumetric) | 448 | Typical: base charge + per-unit rate |
| F ≈ 0 (pure volumetric) | ~10 | No fixed charge, bill scales linearly |
| Negative F (increasing block) | 23 | Marginal rate increases with consumption — model still valid for interpolation |

**Median bill_10k/bill_5k ratio: 1.59** — consistent with typical fixed charge + uniform volumetric structure (pure volumetric would be 2.0).

### Before/After

| Metric | Before (gallon-based) | After (CCF-normalized) |
|--------|----------------------|----------------------|
| bill_5ccf range | $10.00 – $137.00 | $6.61 – $107.51 |
| bill_5ccf mean | $44.39 | $37.72 |
| bill_10ccf range | $12.50 – $274.00 | $12.50 – $204.95 |
| bill_10ccf mean | $70.60 | $57.51 |
| bill_20ccf | all NULL | $12.50 – $409.90 (mean $97.09) |

### Data Provenance Labels

All records now carry in `parse_notes`:
- `source_bill_5000gal=XX.XX` — original TML value at 5,000 gal
- `source_bill_10000gal=XX.XX` — original TML value at 10,000 gal
- `ccf_bills=interpolated_2pt_linear` or `ccf_bills=interpolated_1pt_proportional`
- `bill_20ccf=extrapolated` — flags that 20 CCF is beyond data range
- `implied_fixed=XX.XX` — the model's implied fixed charge

---

## Section 4: Head-to-Head Comparison (N=160)

### Methodology
- Matched TML PWSIDs against scraped_llm records
- 177 total overlapping PWSIDs; filtered to 160 best-match pairs (best scraped per PWSID, tier_count > 0)
- Excluded scraped records with tier_count=0 and the suspicious $120.29 repeated value (appears in 642 scraped PWSIDs — likely a template/default)

### Results (post-normalization)

| Metric | bill_5ccf | bill_10ccf | bill_20ccf |
|--------|-----------|------------|------------|
| N pairs | 160 | 160 | 160 |
| Median % diff | 40.3% | 41.2% | 40.5% |
| Mean % diff | 42.2% | 40.0% | 39.0% |
| < 10% agreement | 27 (17%) | 29 (18%) | 30 (19%) |
| 10–25% | 30 (19%) | 32 (20%) | 32 (20%) |
| 25–50% | 39 (24%) | 39 (24%) | 40 (25%) |
| > 50% divergence | 64 (40%) | 60 (38%) | 58 (36%) |

### Direction

**TML is systematically lower:** 127/160 pairs (79%) have TML < scraped_llm.

### Interpretation

The ~40% median divergence and systematic TML-lower pattern have several plausible explanations:

1. **Vintage gap:** TML is 2023 rates; scraped_llm is predominantly 2024-2025. Water rates in TX have been increasing rapidly.
2. **Self-reporting bias:** TML is a municipal survey where cities self-report. Actual billed amounts may differ from posted rate schedules.
3. **Entity mismatch:** TML matches at city level; the scraped rate may be for a different water system serving the same city.
4. **Interpolation uncertainty:** Our 2-point model assumes uniform volumetric rate. Actual multi-tier structures would produce different bills at the interpolated consumption levels.

**Conclusion:** TML and scraped_llm are not strongly corroborative at the individual-PWSID level, but the dataset provides useful coverage for TX where scraped data is sparse. The ~18% of pairs with <10% agreement suggests the data is fundamentally sound for the subset where entity matching is correct and vintage difference is small.

---

## Section 5: Actions Taken

| Action | Records affected | Details |
|--------|-----------------|---------|
| Bill normalization (2-point) | 471 | Interpolated from gallon to CCF benchmarks |
| Bill normalization (1-point) | 5 | Proportional scaling, bill_10ccf = 2 × bill_5ccf |
| bill_20ccf populated | 476 | Extrapolated (flagged in parse_notes) |
| Outlier flagged | 1 | TX0700059: normalized bill_10ccf=$204.95 > $200 |
| Confidence unchanged | 476 | All remain "medium" (correct for bill_only) |
| JSONB unchanged | 476 | Already clean empty arrays |
| Original values preserved | 476 | In parse_notes: source_bill_5000gal, source_bill_10000gal |

### Pipeline Run Logged
- Step: `tx_tml_audit_migration`
- Status: success
- Logged to `pipeline_runs` table

---

## Section 6: Recommendations

1. **Do not treat TML bill_5ccf/bill_10ccf as source-primary.** These are interpolated values. For any analysis requiring source-authoritative bills, use the `source_bill_5000gal` and `source_bill_10000gal` values in parse_notes.

2. **bill_20ccf is extrapolated.** It extends 50% beyond the data range (14,960 gal vs 10,000 gal max). Use with appropriate caution, especially for the 23 increasing-block records where the extrapolation may underestimate.

3. **The 5 single-bill records** (1-point proportional) are lower quality. The `bill_10ccf = 2 × bill_5ccf` assumption is crude (no fixed charge modeled). These could be upgraded if TML publishes the 10,000 gal bill in a future survey.

4. **Vintage refresh.** TML publishes annually. A 2025 vintage ingest would eliminate the vintage gap driving most of the H2H divergence.

5. **The $120.29 repeated value in scraped_llm** (642 PWSIDs) warrants separate investigation — it appears to be a template or default value, not a real rate.

---

## Appendix: Scripts & Output Files

| File | Purpose |
|------|---------|
| `scripts/migrate_tx_tml_to_comparable.py` | Migration script (normalization + outlier flagging) |
| `src/utility_api/ingest/tml_tx_ingest.py` | Original ingest pipeline |
| `data/bulk_sources/tx_tml/tml_water_2023.xlsx` | Source data file |

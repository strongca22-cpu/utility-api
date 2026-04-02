# EFC Bulk Source Audit — Pilot (4 Largest States)

**Date:** 2026-04-02
**Sprint:** 28 (EFC audit, continuing Duke NIEPS audit pattern)
**Scope:** efc_ar_2020, efc_ia_2023, efc_wi_2016, efc_ga_2019
**Total records:** 2,226 PWSIDs (2,197 sole-source, 36 overlap with scraped_llm)

---

## Executive Summary

- **JSONB format is already clean.** No extra keys, no contiguity gaps, no duplicate tiers. No structural fixes needed — the EFC generic ingest (Sprint 18b) wrote canonical format from the start.
- **EFC bills are the ground truth.** Unlike scraped_llm (where tiers are primary and bills derived), EFC bills come directly from the bill curve API. The `volumetric_tiers` JSONB is reverse-engineered from the curve and intentionally lossy — it's a best-guess rate design, not authoritative structure.
- **Confidence recalibrated.** 1,220 records (55%) changed from "high" to "medium" — all 1-tier (uniform) structures, consistent with Duke-established criteria (high requires tier_count >= 2).
- **Overlap with scraped_llm is small (N=36)** but shows a consistent pattern: scraped is +19% median higher than EFC, with Arkansas showing systematic bias (+29%, p > 70% scraped higher).
- **Bill calculation simplified.** Replaced complex interpolation with snap-to-nearest-curve-point (when close) or simple interpolation (when splitting). Eliminates unnecessary precision for survey-vintage data.

---

## Section 1: Ingest Pipeline Documentation

### Source
UNC Environmental Finance Center Topsail Dashboard API
- **URL:** `https://dashboards.efc.sog.unc.edu/dashboards/{id}/chart_data.json`
- **Format:** JSON API responses; bill curve at 500-gal or 1000-gal increments (varies by state)
- **Coverage:** 24 state dashboards, ~7,096 utilities total

### Data Flow
```
EFC API → api_cache.json → efc_generic.py → rate_schedules
```

1. **Discovery:** Scrapes dashboard HTML for utility rate_structure_ids
2. **Fetch:** Calls JSON API per utility (0.5s pacing); caches to `data/raw/efc_{state}/api_cache.json`
3. **Parse:**
   - **Fixed charge:** Bill at 0 gallons, divided by billing period divisor
   - **Tier structure:** Reverse-engineered from bill curve marginal rates (±$0.05/kgal tolerance)
   - **Bill snapshots:** Snapped to nearest curve data point or interpolated between bracketing points
4. **Write:** UPSERT to `rate_schedules` (pwsid, source_key, vintage_date, customer_class)

### Key Design Decisions

**Bills vs. Tiers — Opposite Reliability Hierarchies:**

| | scraped_llm | EFC |
|---|---|---|
| Source | Utility's own rate schedule PDF | EFC bill curve API |
| Tier data | Extracted by LLM — explicit | Reverse-engineered from curve — inferred |
| Bills | Calculated from extracted tiers | Read from curve (ground truth) |
| Primary truth | Tiers | Bills |

**Bill Curve Simplification (as of 2026-04-02):**
- 10 CCF (7,481 gal) → snaps to 7,500 gal curve point on 500-gal curves (0.26% off)
- 20 CCF (14,961 gal) → snaps to 15,000 gal (0.26% off)
- 5 CCF (3,740 gal) → interpolates between 3,500 and 4,000 on 500-gal curves
- On 1000-gal increment curves: interpolates between bracketing points
- Snap threshold: within 10% of the curve increment from either bracket point
- No extrapolation beyond curve maximum (bill_24ccf stays NULL for EFC)

**No explicit tier data in the API.** The `rate_structure` portion of the API response contains metadata (PWSID, billing period, ownership, effective date) but NO rate tiers, block boundaries, or per-unit rates. The `bill.water` dictionary is the only rate information: pre-computed bills at gallon levels.

### Vintage

Per-utility, from API field `first_effective_date` or `bill_effective_date`:
- 28% of records have vintage < 2018 (predominantly WI and GA)
- Median vintage: 2019
- Range: 1500 (data quality issue in GA) to 2024

---

## Section 2: Head-to-Head Comparison (N=36)

**WARNING: N=36 is small. Results are indicative, not statistically conclusive.**

### Overall Bill Difference (scraped relative to EFC)

| Metric | Value |
|--------|-------|
| Median % diff | +19.0% |
| Mean % diff | +34.8% |
| P25 | +0.0% |
| P75 | +64.6% |
| Std Dev | 47.9% |

### Agreement Buckets

| Bucket | Count | % |
|--------|-------|---|
| <10% | 12 | 33.3% |
| 10-25% | 6 | 16.7% |
| 25-50% | 5 | 13.9% |
| 50-100% | 11 | 30.6% |
| >100% | 2 | 5.6% |

**Direction for >50% disagreements (N=13):** 92% scraped higher, 8% scraped lower.

### By EFC Source

| Source | N | Med %Diff | <10% | 10-25% | 25-50% | >50% | Avg EFC | Avg Scraped |
|--------|---|-----------|------|--------|--------|------|---------|-------------|
| efc_ar_2020 | 17 | +28.9% | 6 | 1 | 2 | 8 | $50.05 | $64.12 |
| efc_ga_2019 | 6 | +14.8% | 2 | 2 | 1 | 1 | $37.74 | $46.41 |
| efc_ia_2023 | 10 | +18.3% | 2 | 3 | 2 | 3 | $55.23 | $72.24 |
| efc_wi_2016 | 3 | +8.5% | 2 | 0 | 0 | 1 | $43.17 | $54.78 |

### Vintage Gap Analysis

- Records with both vintages: 20
- Median year gap (scraped - EFC): 5 years
- Explains part of the systematic positive bias — scraped rates are newer, and rates trend upward over time

### Interpretation

The +19% median scraped-higher bias is partially explained by vintage gap (EFC data is 3-8 years older than scraped). After accounting for ~3-5% annual rate inflation, residual disagreement likely reflects:
1. Different rate schedules for different customer classes
2. EFC surveys capturing a specific billing period vs. scraped capturing current posted rates
3. LLM extraction errors in a subset of scraped records

---

## Section 3: JSONB Storage Format Audit (N=2,226)

### fixed_charges

| Metric | Count | % |
|--------|-------|---|
| Has fixed_charges | 2,207 | 99.1% |
| NULL | 19 | 0.9% |
| Extra keys | 0 | 0.0% |
| Missing canonical keys | 0 | 0.0% |

**No fixes needed.** Canonical format {name, amount, meter_size} used throughout.

### volumetric_tiers

| Metric | Count | % |
|--------|-------|---|
| Has volumetric_tiers | 2,204 | 99.0% |
| NULL | 22 | 1.0% |
| Extra keys | 0 | 0.0% |
| Contiguity gaps | 0 | 0.0% |
| Duplicate tiers | 0 | 0.0% |

**No fixes needed.** Canonical format {tier, min_gal, max_gal, rate_per_1000_gal} used throughout.

### Tier Count Distribution

| Tiers | Count | % |
|-------|-------|---|
| 1 | 1,216 | 55.2% |
| 2 | 470 | 21.3% |
| 3 | 376 | 17.1% |
| 4 | 142 | 6.4% |

Note: 55% uniform (1-tier) is consistent with EFC's nationwide survey profile — many smaller rural utilities use flat or uniform rates.

### Rate Structure Distribution

| Type | Count | % |
|------|-------|---|
| uniform | 1,216 | 54.6% |
| decreasing_block | 551 | 24.8% |
| increasing_block | 437 | 19.6% |
| flat | 22 | 1.0% |

### Billing Frequency Distribution

| Frequency | Count | % |
|-----------|-------|---|
| monthly | 1,845 | 82.9% |
| quarterly | 342 | 15.4% |
| bimonthly | 23 | 1.0% |
| other | 16 | 0.7% |

---

## Section 4: EFC Data Issues

### Issue 1: NULL/Zero Bills
**Count: 0.** All 2,226 records have valid bill_10ccf > $0.

### Issue 2: Non-Flat with Identical Bills
**Count: 0.** No tiered structures with identical 5/10/20 CCF bills.

### Issue 3: Bill Outliers
- Bill < $5: 0
- Bill > $500: 1 (IA4283067, $533.65, increasing_block)

### Issue 4: Stale Vintage
- 622 records (28%) have vintage < 2018
- Most from WI (2016 survey vintage) and GA (older utilities)
- 2 GA records have vintage = 1500 (obvious data quality issue)

### Issue 5: Systematic Bias (Arkansas)
Arkansas shows systematic scraped-higher bias: median +28.9%, with >70% of pairs having scraped > EFC. This is flagged as "YES systematic" but is likely explained by the vintage gap (AR EFC is 2020 vintage, scraped is mostly 2025-2026).

---

## Section 5: Actions Taken

### Confidence Recalibration (applied 2026-04-02)
- **1,220 records** changed from "high" to "medium"
- All are 1-tier (uniform) structures
- Criteria: high confidence requires tier_count >= 2 (Duke-established standard)
- Logged to pipeline_runs as `efc_confidence_recalibration`

### Bill Calculation Simplification (code change, 2026-04-02)
- Replaced `_interpolate_bill()` and `_compute_monthly_bill()` with `_bill_from_curve()`
- Snap to nearest curve point when within 10% of the curve increment
- Simple linear interpolation when splitting between two points
- No extrapolation beyond curve maximum
- Archived deprecated state-specific modules (efc_fl_ingest.py, efc_nc_ingest.py) to `ingest/legacy/`

### NOT Done (and why)
- **Bill recalculation skipped.** Curve-interpolated bills are the ground truth. Recalculating from reverse-engineered tiers would introduce 6.2% median error, not reduce it.
- **JSONB structural fixes skipped.** Already clean — no extra keys, no contiguity gaps, no duplicates.
- **source_priority.yaml unchanged.** Confidence downgraded within records, but source priority remains at 5.

---

## Section 6: Recommendations for Remaining 14 EFC States

1. **Run confidence recalibration on all EFC sources:** `python scripts/migrate_efc_to_comparable.py --all-efc`. The same 1-tier → medium pattern will apply.

2. **No JSONB fixes expected.** All states used the same `efc_generic.py` ingest, which writes canonical format. Spot-check a few states to confirm.

3. **Grow overlap for better cross-reference.** Current 36-pair overlap is too small for robust statistics. As scraped_llm coverage expands into AR/IA/WI/GA, re-run `efc_qa_analysis.py` to get meaningful agreement distributions.

4. **Flag the GA vintage anomalies.** Two records with vintage = 1500 are obvious data quality issues in the EFC API. Consider excluding or flagging records with vintage < 1990.

5. **AZ and NH outlier bills** (noted in Sprint 18b ingest log: max $9,315 and $3,414) should be investigated in a future audit cycle when those states are included.

---

## Appendix: Scripts & Output

| File | Purpose |
|------|---------|
| `scripts/efc_qa_analysis.py` | QA analysis (Tasks 2-4). Generalized for any EFC source_key(s). |
| `scripts/migrate_efc_to_comparable.py` | Confidence recalibration. Generalized for any EFC source_key(s). |
| `data/interim/efc_qa_analysis_output.txt` | Full analysis output (pilot run). |
| `src/utility_api/ingest/efc_generic.py` | Simplified bill calculation (snap-or-interpolate). |
| `src/utility_api/ingest/legacy/` | Archived efc_fl_ingest.py, efc_nc_ingest.py. |

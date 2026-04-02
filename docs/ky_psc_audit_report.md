# KY PSC Water Tariff Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (KY PSC audit, fourth bulk source after Duke, EFC, eAR)
**Scope:** ky_psc_water_tariffs_2025
**Total records:** 84 PWSIDs (KY only)

---

## Executive Summary

- **JSONB had two structural issues, both fixed:**
  - `frequency` key in all 84 fixed_charges records (stripped)
  - 175 inter-tier 1-gallon contiguity gaps across all multi-tier records (closed)
- **One bill computation bug found and fixed.** KY0300387 (Daviess County) had bills = $4.52 at all consumption levels because the LLM over-reported `first_tier_gallons`, zeroing volumetric charges. Recomputed: $28.38/$52.24/$99.96 at 5/10/20 CCF.
- **Confidence upgraded.** 70 records medium → high (bill in [10,200] with 2+ tiers). 13 uniform stay medium. 1 outlier flagged for review.
- **H2H comparison unreliable** — 18 pairs but many duplicated PWSIDs in scraped, wildly inconsistent. Median +9.9% but mean +157% (skewed by outliers). Not useful for systematic QA.
- **KY PSC is a strong source.** LLM-parsed from official state regulatory tariff PDFs. 83% decreasing block structures (typical for KY utilities). Mostly 2025 vintage.

---

## Section 1: Ingest Pipeline Documentation

### Source
Kentucky Public Service Commission (PSC) tariff filings
- **URL:** `https://psc.ky.gov/tariffs/water/Districts,...`
- **Format:** Individual PDF tariff files per utility (~136 directories)
- **Coverage:** KY water districts, associations, and privately owned utilities (NOT city-owned)
- **Vintage:** 2025 (current filings)

### Data Flow
```
PSC IIS directory listing → PDF download → PyMuPDF text extraction
    → Claude Haiku LLM parsing → name matching (SDWIS) → rate_schedules
```

1. **Discovery:** Parse IIS directory listing for utility subdirectories
2. **Download:** Fetch `Tariff.pdf` from each directory; cache to `data/raw/ky_psc/`
3. **Extract:** PyMuPDF extracts rate-relevant pages (pages with "$" + rate keywords)
4. **Parse:** Claude Haiku extracts JSON rate structure (fixed charge, tiers, effective date)
5. **Match:** Fuzzy name matching to SDWIS PWSIDs (exact → substring → word overlap)
6. **Convert:** `_convert_to_rate_schedule()` converts to canonical format, computes bills
7. **Write:** INSERT to `rate_schedules` (idempotent delete-then-insert)

### Key Design: Minimum Bill Pattern

KY PSC tariffs use a distinctive rate structure:
- **Minimum Bill** (e.g., $24.87) covers the first N gallons (e.g., 2,000 gal)
- Volumetric tiers start ABOVE the included gallons (e.g., tier 1 at min_gal=2001)
- This is architecturally correct — the minimum bill IS the fixed charge for included volume

The ingest's `calc_bill()` function correctly handles this via `first_tier_gallons`:
```python
remaining = max(0, gallons - float(first_tier_gallons))
```

**Caveat:** The generic `compute_bill_at_gallons()` helper does NOT account for this pattern. It processes tiers sequentially from remaining=gallons, which overcounts volumetric charges when tier 1 starts above 0. KY PSC bills should NOT be recomputed using the generic helper (except for records where tier 1 starts at min_gal=0, like the KY0300387 fix).

---

## Section 2: JSONB Storage Format Audit

### fixed_charges

| Metric | Before | After |
|--------|--------|-------|
| Keys | `{name, amount, frequency, meter_size}` | `{name, amount, meter_size}` (canonical) |
| Extra `frequency` key | **84 (all records)** | **0** |
| Sample | `{"name": "Minimum Bill", "amount": 24.87, "frequency": "monthly", "meter_size": "5/8\""}` | `{"name": "Minimum Bill", "amount": 24.87, "meter_size": "5/8\""}` |

The `frequency` key was written by the ingest (line 358 of `ky_psc_ingest.py`). All values were "monthly" (redundant with the `billing_frequency` column). Stripped for canonical consistency.

### volumetric_tiers

| Metric | Before | After |
|--------|--------|-------|
| Keys | `{tier, min_gal, max_gal, rate_per_1000_gal}` (canonical) | Same |
| Contiguity gaps | **175** (all 1-gallon: N+1 instead of N) | **0** |
| Duplicate tiers | 0 | 0 |
| Extra keys | 0 | 0 |

The 1-gallon gaps came from the LLM interpreting "first 5,000 gallons" as 0-5000, "next 5,000" as 5001-10000. Canonical format: consecutive tiers share boundaries (prev max = next min).

### Tier Count Distribution

| Tiers | Count | % |
|-------|-------|---|
| 1 | 13 | 15.5% |
| 2 | 7 | 8.3% |
| 3 | 31 | 36.9% |
| 4 | 27 | 32.1% |
| 5 | 5 | 6.0% |
| 6 | 1 | 1.2% |

### Rate Structure Distribution

| Type | Count | % |
|------|-------|---|
| decreasing_block | 70 | 83.3% |
| uniform | 13 | 15.5% |
| increasing_block | 1 | 1.2% |

83% decreasing block is distinctive for KY — most KY utilities offer volume discounts (higher usage = lower per-unit rate). This is the opposite of the conservation pricing pattern seen in CA and western states.

### Tier 1 Start Points

| min_gal | Count | Interpretation |
|---------|-------|----------------|
| 2001 | 54 | Minimum bill includes first 2,000 gal |
| 1001 | 19 | Minimum bill includes first 1,000 gal |
| 0 | 6 | No included gallons (meter charge only) |
| 1501 | 4 | Minimum bill includes first 1,500 gal |
| 2501 | 1 | Minimum bill includes first 2,500 gal |

---

## Section 3: Bill Bug — KY0300387 (Daviess County)

### Problem
Bills stored as $4.52 at 5/10/20 CCF — identical to the fixed charge. Volumetric charges were zero despite having 2 tiers.

### Root Cause
The LLM reported `first_tier_gallons: 20000` (matching tier 1's max_gal of 20,000). The `calc_bill()` function computed:
```python
remaining = max(0, 7480 - 20000) = 0  # at 10 CCF
```
All consumption was treated as "included in minimum bill."

### Why the LLM Was Wrong
A $4.52 minimum bill covering 20,000 gallons (26.7 CCF) is economically nonsensical. The $4.52 is a meter/service charge, and the $6.38/1000gal rate applies to all consumption above the boundary. The LLM confused the tier boundary with the included volume.

### Fix Applied
Recomputed using `compute_bill_at_gallons()` (valid here because tier 1 starts at min_gal=0):

| Benchmark | Old | New |
|-----------|-----|-----|
| bill_5ccf | $4.52 | $28.38 |
| bill_10ccf | $4.52 | $52.24 |
| bill_20ccf | $4.52 | $99.96 |

Parse notes updated with provenance tag documenting the fix and old values.

### Implications for Ingest
The `first_tier_gallons` field in the LLM prompt is ambiguous — it can mean "gallons included in minimum" OR "upper bound of first tier." Consider tightening the prompt to:
```
"first_tier_gallons": <number of gallons where no ADDITIONAL volumetric charge applies beyond the minimum bill>
```
Or: validate that `first_tier_gallons < 5 * CCF` (i.e., less than 5 CCF = 3,740 gal) as a sanity check.

---

## Section 4: Head-to-Head Comparison

### Overlap
18 pairs (11 unique PWSIDs — some PWSIDs have multiple scraped records).

### Results (bill_10ccf vs bill_10ccf)

| Metric | Value |
|--------|-------|
| Median % diff | +9.9% |
| Mean % diff | +156.6% (skewed by outliers) |
| <10% | 2 (11%) |
| 10-25% | 1 (6%) |
| 25-50% | 4 (22%) |
| >50% | 11 (61%) |

### Why This Comparison Is Unreliable

1. **Duplicate PWSIDs in scraped:** KY0300387, KY0340250, KY0400151, KY0430616, KY0540406, KY0630477, KY0740276 each have 2 scraped records with different bills — one often reasonable, one wildly wrong.
2. **Extreme outliers:** KY0300387 was +1885% pre-fix (ingest bug, not a source quality issue).
3. **Structural mismatches:** KY PSC extracts from official tariff PDFs; scraped extracts from utility websites. Different documents may present different rates.

### Notable Pairs

| PWSID | KY PSC | Scraped | Diff | Notes |
|-------|--------|---------|------|-------|
| KY0540406 | $69.80 | $67.51 | -3.3% | Excellent agreement |
| KY0340250 | $78.73 | $125.09 | +59% | Kentucky American Water (IOU) — scraped may include sewer |
| KY0300387 | $52.24 | $89.72 | +72% | After bill fix; reasonable disagreement |
| KY0590220 | $333.10 | $552.00 | +66% | Both very high — Northern KY Water District |

---

## Section 5: Actions Taken

### JSONB Cleanup (applied 2026-04-02)
- **84 records:** `frequency` key stripped from fixed_charges
- **175 tier gaps:** 1-gallon contiguity gaps closed (min_gal N+1 → N)

### Bill Recomputation (applied 2026-04-02)
- **KY0300387:** Bills recomputed from $4.52 to $28.38/$52.24/$99.96
- Provenance documented in parse_notes

### Confidence Recalibration (applied 2026-04-02)

**Before:** All 84 records at "medium"

**After:**

| Confidence | Count | Criteria |
|------------|-------|----------|
| high | 70 | bill_10ccf in [10,200], tier_count >= 2 |
| medium | 13 | 1-tier uniform (capped at medium) |
| medium | 1 | bill_10ccf > $200 (KY0590220, $333) |

1 record flagged for review: KY0590220 ($333 at 10 CCF).

Logged to `pipeline_runs` as `ky_psc_audit_migration`.

### Ingest Code Note (NOT changed)
The `frequency` key in fixed_charges originates from line 358 of `ky_psc_ingest.py`. Should be removed in a future ingest code cleanup to prevent reintroduction on re-ingest. Same applies to WV PSC (`wv_psc_ingest.py`) which likely has the same pattern.

---

## Section 6: Recommendations

### 1. Fix Ingest: Remove `frequency` from fixed_charges
Line 358 of `ky_psc_ingest.py` writes `"frequency": "monthly"` into fixed_charges JSONB. Remove to prevent reintroduction. Also check `wv_psc_ingest.py` for the same issue.

### 2. Tighten LLM Prompt for `first_tier_gallons`
The current prompt is ambiguous about what `first_tier_gallons` means. Add a sanity check: if `first_tier_gallons > 5000` (5 CCF), flag for review.

### 3. Investigate KY0590220 (Northern KY Water District)
$333 at 10 CCF is very high. Rates of $51-55/1000gal may be correct (Northern KY is a large district) but warrant verification against the actual tariff PDF.

### 4. Check WV PSC for Same Issues
WV PSC uses the same ingest pattern (`wv_psc_ingest.py`). Likely has:
- `frequency` key in fixed_charges
- 1-gallon tier contiguity gaps
- Potential `first_tier_gallons` bugs

### 5. Document `compute_bill_at_gallons` Limitation
The generic bill helper doesn't handle the "minimum bill includes first N gallons" pattern. When tier 1 starts above min_gal=0, the helper overcounts volumetric charges. This affects any future use of the helper on KY/WV PSC data.

---

## Appendix: Scripts & Output

| File | Purpose |
|------|---------|
| `scripts/migrate_ky_psc_to_comparable.py` | JSONB fix + confidence recalibration (this audit) |
| `src/utility_api/ingest/ky_psc_ingest.py` | KY PSC ingest pipeline |
| `config/rate_urls_ky_iou.yaml` | IOU utility URL config (3 Kentucky American Water systems) |
| `data/raw/ky_psc/` | Cached tariff PDFs and match log |

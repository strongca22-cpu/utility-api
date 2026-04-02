# NM NMED Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (Batch B — bulk source audits)
**Scope:** nm_nmed_rate_survey_2025
**Total records:** 175 PWSIDs (all NM)

---

## Executive Summary

- **Minimal intervention required.** NMED is a clean bill-only source. No JSONB to fix, no confidence changes needed, bills already normalized to CCF by the ingest.
- **bill_20ccf backfilled** for all 175 records via proportional model (2 × bill_10ccf).
- **1 outlier flagged** (NM3561101, bill_10ccf=$263.21).
- **Confidence stays at "medium"** for all 175 records (correct: bill_only, 0 tiers).
- **H2H with scraped_llm (N=18):** Median diff 23.7% — better agreement than TX TML or OWRS. Small sample limits conclusions.

---

## Section 1: Ingest Pipeline Documentation

### Source
New Mexico Environment Department (NMED) Drinking Water Bureau Annual Rate Survey
- **URL:** https://www.env.nm.gov/drinking_water/rates/
- **Format:** PDF (15 pages, text-selectable table)
- **File:** `data/raw/nm_nmed/2025-nmed-rate-survey.pdf`
- **Coverage:** ~270 publicly owned NM community water systems
- **Vintage:** December 2024 rates (stored as `vintage_date = 2024-12-01`)
- **Consumption standard:** 6,000 gallons/month (8.02 CCF)

### Data Flow
```
NMED PDF → nm_nmed_ingest.py → water_rate_to_schedule() → rate_schedules
```

1. **Download:** PDF cached from NMED website
2. **Parse:** PyMuPDF text extraction → state machine identifies utility name, county, dollar amount
3. **Name match:** NMED utility name → SDWIS PWSID via exact/substring/word-overlap matching
4. **Bill normalization (in ingest):** Single-point proportional scaling from 6,000 gal:
   - `rate_per_gal = bill_6000 / 6000`
   - `bill_5ccf = rate_per_gal × 5 × 748 = bill_6000 × 0.6233`
   - `bill_10ccf = rate_per_gal × 10 × 748 = bill_6000 × 1.2467`
5. **Write:** Via `water_rate_to_schedule()` → canonical empty JSONB + bills

### Key Design Decisions

**Bill normalization model: proportional (no fixed charge)**

The ingest uses a purely proportional model — the entire bill is assumed volumetric. This means:
- `bill_10ccf = 2 × bill_5ccf` exactly
- No fixed charge component modeled
- This underestimates low-consumption bills and overestimates high-consumption bills

This is the best available model given a single data point (bill at 6,000 gal). The same model was applied to TX TML's 5 single-bill records.

**Original values preserved:** `parse_notes` contains `Bill @6000gal=$XX.XX` for every record.

---

## Section 2: JSONB Storage Format Audit

**No fixes needed.** NMED is a bill-only source:
- `fixed_charges = []` (empty array) — 175/175 records
- `volumetric_tiers = []` (empty array) — 175/175 records
- `tier_count = 0` — 175/175 records
- `rate_structure_type = 'uniform'` — 175/175 records

---

## Section 3: Confidence Check

All 175 records at "medium" — correct per Duke criteria:
- tier_count = 0 < 2 → cannot qualify for "high"
- bill_10ccf range [$8.49, $263.21] — 174 within [5, 500], 0 would be "low"
- No changes needed

---

## Section 4: Head-to-Head Comparison (N=18)

### Methodology
- 18 best-match pairs (best scraped per PWSID, tier_count > 0)
- Small sample — results are indicative only

### Results

| Metric | Value |
|--------|-------|
| N pairs | 18 |
| Median % diff | 23.7% |
| < 10% agreement | 3 (17%) |
| 10–25% | 8 (44%) |
| 25–50% | 2 (11%) |
| > 50% divergence | 5 (28%) |

### Interpretation
- **23.7% median diff is the best of the Batch B sources** (vs 41% TX TML, 35% OWRS)
- The proportional model (no fixed charge) is a known source of systematic error, but the 6,000 gal benchmark (8.02 CCF) is close to our 10 CCF standard, limiting the extrapolation distance
- 5 pairs with >50% divergence likely reflect entity mismatches or fundamentally different rate structures
- N=18 is too small for statistical conclusions

---

## Section 5: Actions Taken

| Action | Records affected | Details |
|--------|-----------------|---------|
| bill_20ccf backfilled | 175 | Proportional: 2 × bill_10ccf |
| Outlier flagged | 1 | NM3561101: bill_10ccf=$263.21 > $200 |
| Confidence unchanged | 175 | All remain "medium" (correct for bill_only) |
| JSONB unchanged | 175 | Already clean empty arrays |
| Bills unchanged | 175 | Already normalized to CCF in ingest |

### Pipeline Run Logged
- Step: `nm_nmed_audit_migration`
- Status: success
- Logged to `pipeline_runs` table

---

## Section 6: Recommendations

1. **NMED is the cleanest bill-only source.** The PDF parse is reliable, name matching is high quality, and the 6,000 gal benchmark is close to our CCF standards.

2. **Investigate NM3561101** ($263.21 at 10 CCF normalized) — verify against NMED PDF. At 6,000 gal this would be ~$211, which is high but not impossible for a small NM system.

3. **The proportional model limitation is inherent to single-benchmark sources.** Without a second data point, we cannot separate fixed from volumetric charges. This affects all bill_only sources (TML, NMED, IURC).

---

## Appendix: Scripts & Output Files

| File | Purpose |
|------|---------|
| `scripts/migrate_nm_nmed_to_comparable.py` | Migration script (bill_20ccf backfill + outlier flagging) |
| `src/utility_api/ingest/nm_nmed_ingest.py` | Original ingest pipeline |
| `data/raw/nm_nmed/2025-nmed-rate-survey.pdf` | Source PDF |
| `data/raw/nm_nmed/name_match_log.json` | Name matching audit trail |

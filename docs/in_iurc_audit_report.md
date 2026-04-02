# IN IURC Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (Batch B — bulk source audits)
**Scope:** in_iurc_water_billing_2024
**Total records:** 58 PWSIDs (all IN)

---

## Executive Summary

- **Minimal intervention required.** IURC is a clean bill-only source. No JSONB to fix, no confidence changes needed, bills already normalized to CCF by the ingest.
- **bill_20ccf backfilled** for all 58 records via proportional model (2 × bill_10ccf).
- **No outliers.** All 58 records have bill_10ccf in [$11.22, $128.37] — well within the [5, 200] range.
- **Confidence stays at "medium"** for all 58 records (correct: bill_only, 0 tiers).
- **H2H with scraped_llm (N=41 raw pairs, ~25 unique PWSIDs):** Mixed results, median ~25% diff. Better overlap than initially estimated (prompt said 2 pairs). Some excellent matches (<3%) alongside large divergences (>100%).

---

## Section 1: Ingest Pipeline Documentation

### Source
Indiana Utility Regulatory Commission (IURC) Annual Water Bill Analysis
- **URL:** https://www.in.gov/iurc/files/2024-Water-Billing-Survey-Final.pdf
- **Format:** PDF (4 pages, text-selectable table)
- **File:** `data/raw/in_iurc/2024-Water-Billing-Survey-Final.pdf`
- **Coverage:** IURC-regulated IN water utilities (IOU, Municipal, NFP, Conservancy Districts)
- **Vintage:** January 2024 (stored as `vintage_date = 2024-01-01`)
- **Consumption standard:** 4,000 gallons/month (5.35 CCF)

### Data Flow
```
IURC PDF → in_iurc_ingest.py → water_rate_to_schedule() → rate_schedules
```

1. **Download:** PDF cached from IURC website
2. **Parse:** PyMuPDF text extraction → regex state machine handles parent/sub-area structure (Indiana American, Aqua Indiana)
3. **Name match:** IURC utility name → SDWIS PWSID via exact/substring/word-overlap
4. **Bill normalization (in ingest):** Single-point proportional scaling from 4,000 gal:
   - `rate_per_gal = bill_4000 / 4000`
   - `bill_5ccf = rate_per_gal × 5 × 748 = bill_4000 × 0.935`
   - `bill_10ccf = rate_per_gal × 10 × 748 = bill_4000 × 1.870`
5. **Write:** Via `water_rate_to_schedule()` → canonical empty JSONB + bills

### Key Design Decisions

**Parent/sub-area handling:** Indiana American and Aqua Indiana have multiple service areas with different rates listed as sub-rows. The parser handles this with a parent-tracking state machine.

**Fire protection surcharge:** Some entries include fire protection surcharge (marked with *). This is noted in `parse_notes` but the surcharge is included in the bill amount — no separation attempted.

**IURC-regulated only:** This source covers only IURC-regulated utilities, not all Indiana water systems. Municipal systems outside IURC jurisdiction are not included.

---

## Section 2: JSONB Storage Format Audit

**No fixes needed.** IURC is a bill-only source:
- `fixed_charges = []` (empty array) — 58/58 records
- `volumetric_tiers = []` (empty array) — 58/58 records
- `tier_count = 0` — 58/58 records
- `rate_structure_type = 'uniform'` — 58/58 records

---

## Section 3: Confidence Check

All 58 records at "medium" — correct per Duke criteria:
- tier_count = 0 < 2 → cannot qualify for "high"
- bill_10ccf range [$11.22, $128.37] — all within [5, 500]
- No changes needed

---

## Section 4: Head-to-Head Comparison

### Raw Overlap
41 raw pairs across ~25 unique PWSIDs (some PWSIDs have multiple scraped records).

### Notable Matches
- **Strong agreement (<5%):** IN5245012 (0.7%), IN5236005 (1.5%), IN5253009 (2.5%), IN5222001 (3.0%)
- **Large divergences (>100%):** IN5222002 (197%), IN5282002 (129-219%), IN5282003 (145%), IN5279013 (102%)

### Interpretation
The IURC proportional model (no fixed charge from a single 4,000 gal data point) introduces systematic error:
- At 4,000 gal (5.35 CCF), the original bill is close to our 5 CCF benchmark — bill_5ccf is reliable
- At 10 CCF (7,480 gal), we're extrapolating 87% beyond the data point — more error expected
- Large divergences likely reflect: (a) multi-tier structures where the proportional model breaks down, (b) entity mismatches in name matching

---

## Section 5: Actions Taken

| Action | Records affected | Details |
|--------|-----------------|---------|
| bill_20ccf backfilled | 58 | Proportional: 2 × bill_10ccf |
| Outliers flagged | 0 | All bills in [5, 200] |
| Confidence unchanged | 58 | All remain "medium" (correct for bill_only) |
| JSONB unchanged | 58 | Already clean empty arrays |
| Bills unchanged | 58 | Already normalized to CCF in ingest |

### Pipeline Run Logged
- Step: `in_iurc_audit_migration`
- Status: success
- Logged to `pipeline_runs` table

---

## Section 6: Recommendations

1. **IURC is the smallest Batch B source** (58 records) and covers only IURC-regulated utilities. Its primary value is coverage for Indiana where scraped data provides the dominant source.

2. **The 4,000 gal benchmark is the lowest of any bulk source** (vs 5,000 TML, 6,000 NMED). This means bill_10ccf extrapolation stretches further — +87% beyond the data point. Use bill_5ccf (which is very close to the original 4,000 gal measurement) for highest-confidence comparisons.

3. **Fire protection surcharge contamination:** Some IURC bills include fire protection surcharges that are not separated. This inflates bills relative to scraped rates that exclude surcharges. Consider noting affected PWSIDs if fire surcharge analysis becomes important.

---

## Appendix: Scripts & Output Files

| File | Purpose |
|------|---------|
| `scripts/migrate_in_iurc_to_comparable.py` | Migration script (bill_20ccf backfill + outlier flagging) |
| `src/utility_api/ingest/in_iurc_ingest.py` | Original ingest pipeline |
| `data/raw/in_iurc/2024-Water-Billing-Survey-Final.pdf` | Source PDF |
| `data/raw/in_iurc/name_match_log.json` | Name matching audit trail |

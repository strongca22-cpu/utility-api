# Session Summary — KY PSC Bulk Source Audit (Sprint 28)

**Date:** 2026-04-02
**Commit:** 963f38d
**Duration:** Single session (continuation of eAR audit session)

## What Was Done

Audited all 84 KY PSC water tariff records (ky_psc_water_tariffs_2025). This is the fourth bulk source audit in Sprint 28 (after Duke, EFC, eAR).

### Fixes Applied

1. **JSONB cleanup:** Stripped `frequency` key from all 84 fixed_charges records. Closed 175 inter-tier 1-gallon contiguity gaps (min_gal N+1 → N).

2. **Bill bug fix (KY0300387):** Daviess County had bills = $4.52 at all consumption levels. Root cause: LLM reported `first_tier_gallons: 20000`, causing `calc_bill()` to subtract 20,000 from consumption, leaving 0 remaining gallons for volumetric charges. Fixed bills: $28.38/$52.24/$99.96.

3. **Confidence recalibration:** 70 records upgraded medium → high. 13 uniform stay medium. 1 outlier (KY0590220, $333) flagged for review.

### Key Findings

- **83% decreasing block** — KY utilities overwhelmingly use volume discount pricing (opposite of CA/western conservation pricing)
- **"Minimum bill includes first N gallons" pattern** — 78/84 records have tier 1 starting above min_gal=0. The generic `compute_bill_at_gallons()` helper doesn't handle this. Bills should NOT be recomputed with the generic helper for KY/WV data.
- **`frequency` key in ingest** — ky_psc_ingest.py line 358 writes this. Needs code fix to prevent reintroduction. WV PSC likely has same issue.
- **H2H unreliable** — 18 pairs with many duplicates in scraped, extreme outliers. Not useful for systematic QA.

## Files Created/Modified

| File | Action |
|------|--------|
| `scripts/migrate_ky_psc_to_comparable.py` | Created — JSONB fix + confidence recalibration |
| `docs/ky_psc_audit_report.md` | Created — full audit report |
| `docs/next_steps.md` | Updated — KY PSC section added |

## Post-Migration State

| Confidence | Count |
|------------|-------|
| high | 70 |
| medium | 14 |
| needs_review | 1 |

## Remaining Work
- Fix `ky_psc_ingest.py` line 358 (remove frequency key)
- Tighten LLM prompt for first_tier_gallons
- Investigate KY0590220 ($333 at 10 CCF)
- Audit WV PSC (same ingest pattern, likely same issues)

# Session Summary — CA eAR Bulk Source Audit (Sprint 28)

**Date:** 2026-04-02
**Commit:** bc764dc
**Duration:** Single session

## What Was Done

Completed full audit of all 3 CA SWRCB eAR vintages (2020/2021/2022, 581 records total) following the Duke → EFC audit pattern established earlier in Sprint 28.

### Key Findings

1. **JSONB already clean** — canonical keys only, no contiguity gaps, no duplicate tiers, no `frequency` key. Zero structural fixes needed.

2. **Tier inflation fix verified** — `fix_ear_tier_inflation.py` (applied 2026-03-24) successfully NULLed all inflated tiers. 100 records documented in parse_notes. Zero inflated tiers remain.

3. **Confidence recalibrated** — 202 records changed via `scripts/migrate_ear_to_comparable.py`:
   - 52 (2020): medium→low (tiers NULLed by inflation fix, no pre-computed bills in 2020)
   - 146 (2021+2022): high→medium (1-tier uniforms + NULL billing_frequency)
   - 3 low→medium upgrades
   - 58 records flagged for review (57 NULL billing_frequency, 1 identical bills)

4. **Head-to-head limited** — Only 20 bill-comparable pairs (eAR 2022 vs scraped_llm). Comparison is apples-to-oranges: eAR has bill_12ccf, scraped has bill_10ccf. Median +33.8% scraped higher — partly benchmark mismatch, vintage gap, and sewer contamination (23 scraped CA records mention sewer).

5. **One scraped error surfaced** — CA4810007 has tier limit 2,600 CCF in scraped_llm (obviously inflated). eAR ($55) and Duke ($65) agree on reasonable bill.

### Unique eAR Characteristic
eAR is the only bulk source with **both** explicit tiers AND pre-computed bills independently filed. Unlike EFC (bills → tiers) or Duke (tiers → bills), eAR has both as primary data. This makes eAR particularly valuable for QA cross-referencing.

### Bill Benchmark Gap
eAR provides bill_6/9/12/24ccf. Scraped/EFC/Duke use bill_5/10/20ccf. **Zero overlapping benchmarks.** Recommendation: compute bill_10ccf from clean eAR tiers during future ingest to enable direct comparison.

## Files Created/Modified

| File | Action |
|------|--------|
| `scripts/migrate_ear_to_comparable.py` | Created — confidence recalibration script |
| `docs/ca_ear_audit_report.md` | Created — full audit report |
| `docs/next_steps.md` | Updated — Sprint 28 eAR section added |

## Post-Migration State

| Source | High | Medium | Low | Review |
|--------|------|--------|-----|--------|
| swrcb_ear_2020 | 0 | 135 | 59 | 20 |
| swrcb_ear_2021 | 85 | 96 | 12 | 20 |
| swrcb_ear_2022 | 91 | 94 | 9 | 18 |

## Remaining Work
- Add bill_10ccf to eAR ingest (cross-source comparability)
- Fix scraped CA4810007 (inflated tier)
- Consider `budget_based` rate_structure_type for CA "Allocation" records
- Resolve 57 NULL billing_frequency records
- Re-run H2H as scraped CA coverage grows

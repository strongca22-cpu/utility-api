# WV PSC Water Tariff Bulk Source Audit

**Date:** 2026-04-02
**Sprint:** 28 (WV PSC audit, fifth bulk source after Duke, EFC, eAR, KY PSC)
**Scope:** wv_psc_2026
**Total records:** 241 PWSIDs (WV only)

---

## Executive Summary

- **JSONB is already clean.** Canonical keys only, no contiguity gaps, no duplicates, no extra keys. WV PSC uses the `water_rate_to_schedule()` helper (unlike KY PSC which built JSONB manually), so structural issues don't apply.
- **Confidence recalibrated downward.** 155 records high → medium (all 1-tier; high requires tier_count >= 2). 1 record high → low ($3 flat rate). 3 records flagged for review (bill outliers).
- **WV is entirely single-tier or flat.** 208 uniform (1-tier), 15 decreasing_block (1-tier), 18 flat (0-tier). Zero multi-tier records → zero records qualify for "high" confidence under Duke criteria.
- **H2H comparison is chaotic** (34 pairs). No systematic bias — disagreements go both directions with extreme outliers. Not useful for systematic QA.
- **Data source is strong.** PSC-regulated tariff filings parsed via 2-point cost method. 2026 vintage (freshest bulk source in the database).

---

## Section 1: Ingest Pipeline Documentation

### Source
West Virginia Public Service Commission tariff filings
- **Method:** Automated web scraping of PSC tariff database
- **Format:** HTML tariff pages with rate tables
- **Coverage:** WV regulated water utilities (districts, associations, IOUs)
- **Vintage:** 2026 (current filings)

### Data Flow
```
PSC web tariff pages → cost at 3,400 + 4,000 gal → 2-point slope method
    → fixed_charge + volumetric_rate → water_rate_to_schedule() → rate_schedules
```

**Key difference from KY PSC:** WV ingest uses a 2-point cost method (bill at 3,400 gal and 4,000 gal) to derive a single volumetric rate via slope calculation. This produces uniform (1-tier) structures only. The data goes through `water_rate_to_schedule()` helper, which outputs canonical JSONB.

### Rate Computation
- Volumetric rate: `(cost_4000 - cost_3400) / 600 * 1000` → $/1000 gal
- Fixed charge: `cost_3400 - (volumetric_rate * 3.4)` (y-intercept)
- Special case: If slope ≈ 0 → flat rate (no volumetric component)
- Special case: If slope < 0 → decreasing block (uses average rate instead)

### Why All Single-Tier
The 2-point method can only resolve a single slope. Multi-tier structures would require 3+ data points. This is a limitation of the PSC data format, not the ingest code.

---

## Section 2: JSONB Storage Format Audit

### fixed_charges
| Metric | Result |
|--------|--------|
| Keys | `{name, amount, meter_size}` — canonical |
| Extra keys | **0** |
| Sample | `{"name": "Service Charge", "amount": 3.61, "meter_size": null}` |

### volumetric_tiers
| Metric | Result |
|--------|--------|
| Keys | `{tier, min_gal, max_gal, rate_per_1000_gal}` — canonical |
| Extra keys | **0** |
| Contiguity gaps | **0** (all single-tier) |
| Duplicate tiers | **0** |
| Tier 1 min_gal | All 223 at 0 (no included-gallons pattern) |

### Rate Structure Distribution
| Type | Count | % |
|------|-------|---|
| uniform | 208 | 86.3% |
| flat | 18 | 7.5% |
| decreasing_block | 15 | 6.2% |

### Billing Frequency
All 241 records: monthly.

---

## Section 3: Head-to-Head Comparison (N=34)

### Results (bill_10ccf vs bill_10ccf)

| Metric | Value |
|--------|-------|
| Pairs | 34 |
| Median % diff | -14.3% (scraped lower) |
| Direction | Mixed — no systematic bias |

### Agreement Buckets
| Bucket | Count | % |
|--------|-------|---|
| <10% | 6 | 18% |
| 10-25% | 4 | 12% |
| 25-50% | 10 | 29% |
| >50% | 14 | 41% |

### Interpretation
The H2H is unreliable. Disagreements go both directions with extreme outliers (WV3305204: +282%, WV3302710: +172%, WV3301042: -71%, WV3303107: -76%). Likely causes:
1. **Different rate interpretations:** PSC tariffs vs utility website rates may differ (purchased water adjustments, surcharges)
2. **Scraped extraction errors:** Several scraped WV records have suspiciously low bills ($23-51 range for utilities where PSC says $80-158)
3. **No scraped-side sewer contamination** — WV PSC is water-only, and the bias isn't consistently scraped-higher

Not a useful QA signal. Best used as sole source for WV utilities not in scraped_llm.

---

## Section 4: Actions Taken

### Confidence Recalibration (applied 2026-04-02)

**Before:**
| Confidence | Count |
|------------|-------|
| high | 155 |
| medium | 86 |

**After:**
| Confidence | Count |
|------------|-------|
| high | 0 |
| medium | 239 |
| low | 2 |
| needs_review | 3 |

**Key transitions:**
- **155 high → medium:** All 1-tier records. Duke criteria: high requires tier_count >= 2.
- **1 high → low:** WV3302814 ($3.00 flat rate, bill_10ccf < $5)
- **3 flagged for review:** WV3300806 ($251), WV3301912 ($237), WV3302814 ($3)

Logged to `pipeline_runs` as `wv_psc_audit_migration`.

### NOT Done (and why)
1. **JSONB fixes skipped.** Already canonical — WV uses `water_rate_to_schedule()` helper.
2. **Bill recomputation skipped.** WV PSC bills are computed from 2-point slope (authoritative for this data format). No bugs found.
3. **No ingest code changes.** WV ingest is structurally sound — it uses the helper functions correctly.

---

## Section 5: Recommendations

### 1. Investigate $3 Flat Rate (WV3302814)
A $3/month flat rate with zero volumetric charge is extremely unusual. May be a partial rate (missing purchased water adjustment) or a very small system with subsidized rates. Worth verifying against actual PSC filing.

### 2. Investigate High-Bill Outliers
WV3300806 ($251) and WV3301912 ($237) at 10 CCF are high but not implausible for small WV utilities with expensive infrastructure. Flagged for review — no automatic action.

### 3. Multi-Tier Extraction
WV PSC data only supports single-tier extraction (2-point method). If PSC tariff pages ever expose full tier tables, the ingest could be upgraded to extract multi-tier structures. Low priority — 86% uniform is likely accurate for WV.

---

## Appendix: Scripts & Output

| File | Purpose |
|------|---------|
| `scripts/migrate_wv_psc_to_comparable.py` | Confidence recalibration (this audit) |
| `src/utility_api/ingest/wv_psc_ingest.py` | WV PSC ingest pipeline |

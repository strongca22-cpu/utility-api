# Bulk Source Audit — Comprehensive Summary

**Sprint:** 28 (completed 2026-04-02)
**Scope:** All 9 bulk data sources in `rate_schedules`
**Total records audited:** 10,790

---

## Purpose

Sprint 28 established a standardized audit-and-fix pattern for all bulk data sources feeding the `rate_schedules` table. The objective was to ensure cross-source comparability by:
1. Validating JSONB storage format (canonical keys, contiguity, no extra fields)
2. Normalizing bill benchmarks to standard CCF consumption levels (5/10/20 CCF)
3. Applying consistent confidence criteria across all sources
4. Running head-to-head comparisons against `scraped_llm` where overlap exists
5. Documenting data provenance, limitations, and transformation methodology

---

## Audit Pattern

For each source:
1. **Ingest audit** — trace data flow from source to `rate_schedules`
2. **JSONB format check** — canonical keys? Extra keys? Contiguity? Duplicates?
3. **Bill normalization** — are bills at true CCF benchmarks or do they need conversion?
4. **Confidence recalibration** — apply Duke criteria (high = bill_10ccf in [10,200] + tier_count >= 2; 1-tier capped at medium)
5. **Head-to-head** — compare bill_10ccf with `scraped_llm` where overlap exists
6. **Fix issues** — strip extra keys, close gaps, normalize bills, flag outliers
7. **Document** — audit report, migration script, updated next_steps, session summary

### Confidence Criteria (Duke-established)

| Level | Requirements |
|-------|-------------|
| **high** | bill_10ccf in [$10, $200] AND tier_count >= 2 |
| **medium** | bill_10ccf in [$5, $500] OR tier_count <= 1 |
| **low** | bill_10ccf NULL or outside [$5, $500] |
| **needs_review** | Flagged outlier (bill > $200 or < $5) |

---

## Source Inventory

### Batch A (Audited First)

| Source | Records | States | Vintage | Data Type | Display Tier |
|--------|---------|--------|---------|-----------|-------------|
| Duke NIEPS | 3,177 | 10 states | 2019–2021 | Academic rate schedules (manually digitized) | reference |
| EFC (20 states) | 5,611 | 20 states | 2014–2024 (median 2019) | UNC bill curve API | free |
| CA eAR (3 vintages) | 581 | CA | 2020–2022 | State-mandated utility filings | free |
| KY PSC | 84 | KY | 2025 | PSC tariff PDFs, LLM-parsed | free |
| WV PSC | 241 | WV | 2026 | PSC tariff filings, 2-point slope method | free |

### Batch B (Audited 2026-04-02)

| Source | Records | States | Vintage | Data Type | Display Tier |
|--------|---------|--------|---------|-----------|-------------|
| TX TML | 476 | TX | 2023 | Municipal League survey (XLSX, gallon-based bills) | free |
| OWRS | 387 | CA | 2002–2021 | California Data Collaborative YAML rate specs | free |
| NM NMED | 175 | NM | Dec 2024 | NMED rate survey PDF (bill at 6,000 gal) | free |
| IN IURC | 58 | IN | Jan 2024 | IURC billing survey PDF (bill at 4,000 gal) | free |

---

## Bill Data Hierarchy

Understanding what each source actually provides is critical for interpretation.

### Source-Primary Bills (Ground Truth)

These sources report bills directly — the bill values are authoritative for their vintage:

| Source | Bill origin | CCF benchmarks? | Notes |
|--------|-----------|-----------------|-------|
| **EFC** | Bill curve API (pre-computed by UNC) | Snapped/interpolated to 5/10/20 CCF | Bills are ground truth; tiers are reverse-engineered and intentionally lossy |
| **CA eAR** | State-filed bills at 6/9/12/24 CCF | Backfilled to 5/10/20 CCF from tiers | Both tiers AND pre-computed bills are authoritative (unique) |
| **TX TML** | Survey-reported at 5,000/10,000 gal | **Interpolated** to 5/10 CCF via 2-point linear model | Original gallon values preserved in parse_notes |
| **NM NMED** | Survey-reported at 6,000 gal | Proportional scaling (no fixed charge modeled) | bill_10ccf = 2 × bill_5ccf exactly |
| **IN IURC** | Survey-reported at 4,000 gal | Proportional scaling (no fixed charge modeled) | bill_10ccf = 2 × bill_5ccf exactly |

### Tier-Derived Bills (Calculated)

These sources provide rate structures; bills are computed from tiers + fixed charges:

| Source | Tier origin | Bill calculation |
|--------|-----------|-----------------|
| **Duke NIEPS** | Manually digitized from rate schedules | Standard tier-walk at 5/10/20 CCF |
| **OWRS** | Machine-readable YAML rate specifications | Standard tier-walk at 5/10/20 CCF |
| **KY PSC** | LLM-extracted from tariff PDFs | Tier-walk with minimum-bill pattern |
| **WV PSC** | 2-point slope method (single linear rate) | Fixed + volumetric × CCF |

### Bill-Only Sources (No Tier Data)

| Source | Benchmark | Model | Limitation |
|--------|-----------|-------|-----------|
| **TX TML** | 5,000 + 10,000 gal | 2-point linear (F + R) | Assumes uniform rate; can't capture multi-tier |
| **NM NMED** | 6,000 gal | 1-point proportional | No fixed charge separation; bill_10ccf = 2 × bill_5ccf |
| **IN IURC** | 4,000 gal | 1-point proportional | No fixed charge separation; furthest extrapolation distance |

---

## Fixes Applied

### JSONB Structural Fixes

| Source | Issue | Fix | Records |
|--------|-------|-----|---------|
| KY PSC | `frequency` key in fixed_charges | Stripped | 84 |
| KY PSC | 1-gallon inter-tier contiguity gaps | Closed (N+1 → N) | 175 gaps across 84 records |
| CA eAR | Tier limits in gallons instead of HCF (748x inflation) | NULLed tiers, preserved bills | ~80 records |

**No JSONB fixes needed:** Duke NIEPS, EFC, WV PSC, TX TML, OWRS, NM NMED, IN IURC

### Bill Normalization

| Source | Action | Method | Records |
|--------|--------|--------|---------|
| TX TML | Gallon → CCF normalization | 2-point linear model (F + R × gal) | 471 (2-point) + 5 (1-point) |
| CA eAR | bill_5ccf/bill_10ccf backfill from tiers | Standard tier-walk computation | 460 |
| KY PSC | Bill recomputation (KY0300387) | Corrected first_tier_gallons, recomputed | 1 |

**Bills already at CCF benchmarks (no normalization):** Duke NIEPS, EFC, OWRS, WV PSC, NM NMED, IN IURC

### bill_20ccf Backfill

| Source | Method | Records |
|--------|--------|---------|
| TX TML | Extrapolated from 2-point linear model (F + R × 14,960) | 476 |
| NM NMED | Proportional (2 × bill_10ccf) | 175 |
| IN IURC | Proportional (2 × bill_10ccf) | 58 |

---

## Confidence Recalibration

### Summary

| Source | Before | After | Net Change |
|--------|--------|-------|------------|
| **Duke NIEPS** | (not formally recalibrated) | — | — |
| **EFC** | 2,226 (mix) | 1,006 high, 1,220 medium | **1,220 high → medium** (1-tier cap) |
| **CA eAR** | 581 (mix) | 176 high, 325 medium, 80 low | ~146 high → medium, ~53 medium → low |
| **KY PSC** | 84 medium | 70 high, 13 medium, 1 flagged | **70 medium → high** (multi-tier with good bills) |
| **WV PSC** | 155 high, 86 medium | 0 high, 239 medium, 2 low | **155 high → medium** (all 1-tier) |
| **TX TML** | 476 medium | 476 medium | No change (correct for bill_only) |
| **OWRS** | 371 high, 16 medium | 267 high, 120 medium | **104 high → medium** (1-tier cap) |
| **NM NMED** | 175 medium | 175 medium | No change (correct for bill_only) |
| **IN IURC** | 58 medium | 58 medium | No change (correct for bill_only) |

### Post-Audit Confidence Distribution (All Bulk Sources)

| Confidence | Records | Sources contributing "high" |
|------------|---------|---------------------------|
| **high** | ~1,513 | OWRS (267), EFC (~1,006), CA eAR (176), KY PSC (70) |
| **medium** | ~9,185 | All sources |
| **low** | ~82 | CA eAR (80), WV PSC (2) |
| **needs_review** | ~10 | Outliers across sources |

---

## Head-to-Head Comparisons

### All Sources vs. scraped_llm

| Source | N Pairs | Median % Diff | <10% | 10–25% | 25–50% | >50% | Scraped Higher? |
|--------|---------|--------------|------|--------|--------|------|-----------------|
| Duke NIEPS | 1,525 | +36.5% | 16% | 14% | 21% | 50% | 87% of large diffs |
| EFC | 36 | +19.0% | 33% | 17% | 14% | 36% | 92% of large diffs |
| CA eAR (2022) | 16 | +11.6% | 19% | — | — | 31% | 75% |
| CA eAR (2021) | 19 | +12.4% | 16% | — | — | 42% | 74% |
| CA eAR (2020) | 16 | +45.7% | 19% | — | — | 56% | 73% |
| KY PSC | 18 | +9.9% | 11% | 6% | 22% | 61% | Mixed (unreliable) |
| WV PSC | 34 | -14.3% | 18% | 12% | 29% | 41% | Mixed (no bias) |
| TX TML | 160 | +41.2% | 18% | 20% | 24% | 38% | 79% lower (not higher) |
| OWRS | 183 | +35.1% | 11% | 24% | 34% | 31% | 72% lower |
| NM NMED | 18 | +23.7% | 17% | 44% | 11% | 28% | — |
| IN IURC | ~25 | ~25% | — | — | — | — | Mixed |

### Key H2H Findings

1. **Scraped is systematically higher** than bulk sources in most comparisons. Primary driver: vintage gap (bulk sources 2-5 years older than scraped 2024-2025 data) combined with real rate inflation (5-8% annually in many states).

2. **Best agreement:** CA eAR 2021-2022 (12%), EFC (19%), NM NMED (24%). These have the smallest vintage gaps and/or state-filed authoritative data.

3. **Worst agreement:** Duke NIEPS (37%), TX TML (41%). Duke has a ~4-year vintage gap; TML has both vintage gap and gallon/CCF unit mismatch effects.

4. **Small sample warning:** Only Duke NIEPS (1,525) and TX TML (160) / OWRS (183) have statistically meaningful sample sizes. EFC (36), KY PSC (18), NM NMED (18), and others are indicative only.

5. **$120.29 template value in scraped_llm:** 642 PWSIDs have identical bill_10ccf = $120.29, adding noise to H2H comparisons. Warrants separate investigation.

---

## Source Quality Assessment

### Tier 1 — Highest Confidence

| Source | Why | Caveat |
|--------|-----|--------|
| **KY PSC** | Official tariff PDFs, LLM-parsed with validation, 2025 vintage | Small (84 records), KY only, minimum-bill pattern complexity |
| **WV PSC** | Official tariff filings, fresh 2026 vintage | 2-point slope method = single-tier only, WV only |
| **CA eAR** | State-mandated filings, both tiers and bills authoritative | 2020-2022 vintage, tier inflation issue (patched), CA only |

### Tier 2 — Strong but Older

| Source | Why | Caveat |
|--------|-----|--------|
| **OWRS** | Machine-readable YAML specs, multi-tier, calculated bills | Vintage 2002-2021 (most 2015-2018), CA only |
| **Duke NIEPS** | Manually digitized by researchers, multi-state | Academic/licensed (reference tier), 2019-2021 vintage |
| **EFC** | 20-state coverage, bill curve API is ground truth | Tiers reverse-engineered (lossy), median 2019 vintage |

### Tier 3 — Bill-Only (No Tier Data)

| Source | Why | Caveat |
|--------|-----|--------|
| **NM NMED** | Government survey, 2024 vintage, good name matching | Single data point (6,000 gal), proportional model only |
| **TX TML** | Large dataset (476), 2-point model allows F+R separation | 2023 vintage, gallon benchmarks required interpolation |
| **IN IURC** | Government survey, 2024 vintage | Smallest source (58), 4,000 gal benchmark = furthest extrapolation |

---

## Bill Provenance Quick Reference

When consuming bill data from `rate_schedules`, check `parse_notes` for provenance markers:

| Marker | Meaning |
|--------|---------|
| `ccf_bills=interpolated_2pt_linear` | Bill normalized from gallon benchmarks via 2-point model (TX TML) |
| `ccf_bills=interpolated_1pt_proportional` | Bill scaled from single gallon benchmark (TX TML 5 records, NM NMED, IN IURC) |
| `bill_20ccf=extrapolated` | bill_20ccf extends beyond source data range |
| `bill_20ccf=extrapolated_proportional` | bill_20ccf = 2 × bill_10ccf (NM NMED, IN IURC) |
| `source_bill_5000gal=XX.XX` | Original TX TML gallon-based bill preserved |
| `source_bill_10000gal=XX.XX` | Original TX TML gallon-based bill preserved |
| `Bill @6000gal=$XX.XX` | Original NM NMED gallon-based bill |
| `Bill @4000gal=$XX.XX` | Original IN IURC gallon-based bill |
| `implied_fixed=XX.XX` | TX TML 2-point model implied fixed charge |

---

## Outliers Flagged for Review

| PWSID | Source | Issue | bill_10ccf |
|-------|--------|-------|-----------|
| TX0700059 | TX TML | Normalized bill > $200 | $204.95 |
| NM3561101 | NM NMED | bill_10ccf > $200 | $263.21 |
| KY0590220 | KY PSC | bill_10ccf > $200 | $333.00 |
| WV3300806 | WV PSC | bill_10ccf > $200 | $251.00 |
| WV3301912 | WV PSC | bill_10ccf > $200 | $237.00 |
| WV3302814 | WV PSC | bill_10ccf < $5 (flat rate) | $3.00 |
| IA4283067 | EFC | bill_10ccf > $500 | $533.65 |

---

## Scripts & Reports Index

### Migration Scripts

| Script | Source | Purpose |
|--------|--------|---------|
| `scripts/migrate_duke_to_comparable.py` | Duke NIEPS | Contiguity, dedup, confidence |
| `scripts/migrate_efc_to_comparable.py` | EFC | Confidence recalibration (1-tier cap) |
| `scripts/migrate_ear_to_comparable.py` | CA eAR | Confidence recalibration |
| `scripts/backfill_ear_bills.py` | CA eAR | bill_5ccf/bill_10ccf from tiers |
| `scripts/migrate_ky_psc_to_comparable.py` | KY PSC | JSONB fix + bill recompute + confidence |
| `scripts/migrate_wv_psc_to_comparable.py` | WV PSC | Confidence recalibration |
| `scripts/migrate_tx_tml_to_comparable.py` | TX TML | Bill normalization (gal→CCF) |
| `scripts/migrate_owrs_to_comparable.py` | OWRS | Confidence recalibration |
| `scripts/migrate_nm_nmed_to_comparable.py` | NM NMED | bill_20ccf backfill |
| `scripts/migrate_in_iurc_to_comparable.py` | IN IURC | bill_20ccf backfill |

### Audit Reports

| Report | Source |
|--------|--------|
| `docs/duke_qa_cross_reference.md` | Duke NIEPS |
| `docs/efc_pilot_audit_report.md` | EFC (4-state pilot) |
| `docs/ca_ear_audit_report.md` | CA eAR |
| `docs/ky_psc_audit_report.md` | KY PSC |
| `docs/wv_psc_audit_report.md` | WV PSC |
| `docs/tx_tml_audit_report.md` | TX TML |
| `docs/owrs_audit_report.md` | OWRS |
| `docs/nm_nmed_audit_report.md` | NM NMED |
| `docs/in_iurc_audit_report.md` | IN IURC |

---

## Open Items

### Source-Specific
- [ ] TX TML: Investigate TX0700059 ($205 normalized), vintage refresh with 2025 data
- [ ] OWRS: Check for post-2021 updates from California Data Collaborative
- [ ] NM NMED: Verify NM3561101 ($263) against PDF
- [ ] KY PSC: Verify KY0590220 ($333) against tariff PDF
- [ ] WV PSC: Verify WV3302814 ($3 flat), WV3300806 ($251), WV3301912 ($237)
- [ ] CA eAR: Fix scraped CA4810007 (inflated tier limit), resolve 57 NULL billing_frequency records

### Cross-Source
- [ ] Investigate scraped_llm $120.29 repeated value (642 PWSIDs) — likely template/default
- [ ] Develop bill-only source improvement path: collect second data point for TML/NMED/IURC to enable F+R model
- [ ] Consider `budget_based` as canonical rate_structure_type (currently OWRS-only)
- [ ] Build vintage-aware comparison logic (adjust for inflation when comparing across vintages)

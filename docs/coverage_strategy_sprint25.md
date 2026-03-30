# Coverage Strategy Report — Sprint 25

**Date:** 2026-03-30
**Purpose:** Inform strategic decisions about comprehensive rate coverage expansion

---

## Current State

| Metric | Value |
|---|---|
| Total US community water systems (CWS) | 44,643 |
| Systems with rate data | 9,734 (21.8%) |
| **Population covered** | **233.9M (73.1%)** |
| Sources contributing | 30 (EFC, SWRCB, Duke, LLM-scraped, state PSCs, OWRS) |
| States with >50% system coverage | 11 |
| States with <10% system coverage | 15 |

The system/population gap is stark: 78% of systems lack rate data, but those systems serve only 27% of the US population. The missing systems are overwhelmingly small.

---

## What We Learned Today (3 Sweep Runs, 700 PWSIDs)

### The pipeline works at scale

| Run | PWSIDs | Parse Success | Cost |
|---|---|---|---|
| 440 gap states (>=3k pop) | 440 | 261 (64%) | $12.10 |
| ND (>=500 pop) | 119 | 56 (62%) | $2.72 |
| SD (>=500 pop) | 141 | 86 (67%) | $2.90 |
| **Total** | **700** | **403 (65%)** | **$17.72** |

Unit economics: **$0.036 per successful rate** (direct API pricing), **$0.022 at batch**.

### Small utilities are not harder to parse

| Population Bucket | Parse Success Rate |
|---|---|
| 100k+ | 62% |
| 50k-100k | 54% |
| 25k-50k | 64% |
| 10k-25k | 75% |
| 3k-10k | 52% |
| 1k-3k | 67% |
| 500-1k | 69% |

The 500-1k bucket parses *better* than 50k-100k. Small-town rate pages are simple and clean. The bottleneck for small utilities is discovery (finding the URL), not parsing.

### The cascade is essential

Rank #2 and #3 URLs contribute **29% of all successful parses**. Single-URL approaches would lose nearly a third of yields. 3 candidates is the optimum — more than 3 shows declining returns (noisy results).

### Threshold tuning: 50→45 (implemented)

The 440 sweep logged 2,194 near-miss URLs scoring 30-50. Of these, **1,219 scored 45-49** — just below the old threshold. Lowering to 45 is now live in config. This is the single highest-leverage change before a comprehensive run.

---

## Coverage Expansion Scenarios

### Scenario A: All gap >=3k pop (recommended next step)

| Metric | Value |
|---|---|
| PWSIDs to process | ~5,000 |
| Estimated new rates | ~3,000 (at 60%) |
| Population gained | ~42M |
| **Projected pop coverage** | **73% → 86%** |
| Processing time | ~27 hours continuous |
| Anthropic cost (batch) | ~$45 |
| Serper cost | ~$20 |
| **Total cost** | **~$65** |

This is the highest-ROI segment. It reaches the diminishing returns threshold for population coverage.

### Scenario B: Scenario A + gap 1k-3k for top 10 states

| Metric | Value |
|---|---|
| Additional PWSIDs | ~2,000 (NY, TX, FL, MI, OH, IL, PA, TN, LA, CO) |
| Estimated new rates | ~700 (at 35% — lower discovery rate) |
| Additional pop gained | ~3.5M |
| **Projected pop coverage** | **86% → 87%** |
| Additional cost | ~$25 |

Marginal return: 1% pop coverage for $25. Worth doing only for high-value states where per-utility data matters for the product.

### Scenario C: Full >=500 pop nationwide

| Metric | Value |
|---|---|
| Additional PWSIDs (beyond A) | ~9,000 |
| Estimated new rates | ~4,500 |
| Additional pop gained | ~6M |
| **Projected pop coverage** | **86% → 88%** |
| Additional cost | ~$110 |

Diminishing returns. The 500-1k segment parses well (69%) but discovery is harder and population gain is minimal. Consider only for states where visual map coverage matters (e.g., ND/SD demo case).

---

## What Can't Be Scraped

| Segment | Systems | Population | Constraint |
|---|---|---|---|
| <500 pop | 20,796 | 3.5M (1.1%) | No web presence — rural, HOA, mobile home parks |
| Scanned PDF tariffs | ~100-300 est. | Unknown | Need OCR — not implemented |
| Portal/login-gated rates | ~200-500 est. | Unknown | Behind customer login or utility billing platform |
| JS-heavy state portals | CO, NV, MT | ~8.4M | .colorado.gov blocking, special district structures |

**Practical ceiling for automated scraping: ~88-90% population coverage.** The remaining 10-12% requires either bulk data partnerships (state PSCs, EFC expansions) or manual curation.

---

## State-Level Priorities

### Highest impact (gap pop >3M, >=3k systems)

| State | Gap >=3k | Gap Pop | Notes |
|---|---|---|---|
| TX | 714 | 5.8M | Largest gap. Many small towns, good parse potential. |
| NY | 274 | 5.4M | Large utilities, complex tariffs. |
| MI | 270 | 3.9M | Good potential (63% parse rate today). |
| CO | 155 | 3.8M | **Hard state** — 38% parse rate, special districts. |
| TN | 240 | 3.8M | Strong performer (71% today). |
| OH | 176 | 3.4M | Not yet attempted at scale. |

### Quick wins (high parse rate, moderate gap)

| State | Gap >=3k | Today's Parse Rate | Notes |
|---|---|---|---|
| IL | 230 | 97% | Exceptional parse rate — run immediately. |
| IN | 157 | 88% | Very clean utility websites. |
| LA | 238 | 88% | High success, large gap. |
| ID | 53 | 82% | Small gap but high yield. |

### Hard states (structural issues, needs investigation)

| State | Gap >=3k | Today's Parse Rate | Issue |
|---|---|---|---|
| CO | 155 | 38% | .colorado.gov blocking, special districts, JS-heavy. |
| MT | 37 | 29% | Very small utilities, thin web presence. |
| NV | 35 | 25% | Large utilities behind portals. |

---

## Pipeline Tuning Status

| Change | Status | Impact |
|---|---|---|
| Score threshold 50→45 | **Done** (Sprint 25) | +5-8% discovery yield |
| source_url on best_estimate | **Done** (Sprint 25) | Enables spot-check QA |
| Tier labels (bulk/premium/free) | **Done** (Sprint 25) | Dashboard display |
| 3-URL cascade | Already calibrated | 71% success at 3 candidates |
| Reactive deep crawl | Already implemented | 34% rescue rate when triggered |
| Playwright escalation | Already implemented | Handles JS-heavy pages |

### Not yet done (potential improvements)

| Change | Estimated Impact | Effort |
|---|---|---|
| Batch API for parse calls | 50% cost reduction | Medium — architecture change |
| State-specific search keywords (CO, NV) | +10-15% for hard states | Low |
| PDF section extraction (large tariffs) | Recovers ~100-300 rates | Medium |
| OCR for scanned PDFs | Recovers ~50-100 rates | Medium |
| Subdomain following in deep crawl | Unknown | Low |

---

## Decision Points

1. **Run Scenario A now?** ~$65 all-in, 27 hours, gets to 86% pop coverage. This is the obvious next step.

2. **Batch API vs. direct?** Batch cuts Anthropic cost 50% but adds latency (results in ~24 hours vs real-time). For a 27-hour run, batch makes sense since you're waiting anyway.

3. **Invest in hard states (CO, NV, MT)?** These need state-specific keyword tuning, not more brute-force scraping. Worth a focused investigation session before including in a comprehensive sweep.

4. **What population floor for the product?** The >=3k floor covers 86% of pop. Dropping to >=500 adds 2% more pop for 3x more PWSIDs. The answer depends on whether the product is selling coverage breadth (system count) or population reach.

5. **When does bulk data become more efficient than scraping?** For states with state-level PSC data (like WV, KY, IN already), a single bulk ingest covers hundreds of PWSIDs instantly. Identifying which state PSCs publish bulk rate data would be higher-leverage than scraping those states utility-by-utility.

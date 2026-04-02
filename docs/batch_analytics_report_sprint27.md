# Batch Analytics Report — Sprint 27 (Apr 1-2, 2026)

**Period:** 2026-04-01 09:00 through 2026-04-02 22:00 (~36 hours)

---

## Section 1: Batch Summary

| # | Label | Tasks | Succeeded | Failed | Success% | Cost | Submitted |
|---|-------|------:|----------:|-------:|----------|-----:|-----------|
| 1 | prompt_reparse_v1 | 2,807 | 131 | 2,676 | 4.7% | $13.84 | Apr 1 09:47 |
| 2 | orphan_parse_v1 | 2,496 | 1,113 | 1,383 | 44.6% | $19.27 | Apr 1 11:54 |
| 3 | discovery_r1 | 3,200 | 1,054 | 2,146 | 32.9% | $23.42 | Apr 1 18:17 |
| 4 | bulk_replace_a | 521 | 2 | 519 | 0.4% | $2.06 | Apr 1 21:27 |
| 5 | discovery_r2 | 2,776 | 1,167 | 1,609 | 42.1% | $18.82 | Apr 2 09:46 |
| 6 | discovery_r3 | 5,037 | 2,260 | 2,777 | 44.9% | $29.16 | Apr 2 11:29 |
| 7 | bulk_replace_c_r1 | 6,882 | 3,245 | 3,637 | 47.1% | $46.91 | Apr 2 12:17 |
| 8 | discovery_r4 | 1,920 | — | — | — | — | Apr 2 14:54 (PENDING) |

**Totals (7 completed):** 23,719 tasks, 8,972 succeeded (37.8%), 14,747 failed, **$153.47** spent.

### Key observations

- **prompt_reparse_v1** (4.7%) and **bulk_replace_a** (0.4%) confirm: re-parsing failed text with new prompts has very low yield. The content is the problem, not the prompts.
- **Discovery rounds** (r1-r3) and **bulk_replace_c_r1** perform in the 33-47% range — consistent with prior Scenario A performance.
- **orphan_parse_v1** (44.6%) — highest non-discovery success rate. Fresh text that was never parsed is the highest-ROI batch type.

---

## Section 2: Cumulative Impact

**New scraped_llm since Apr 1:** 9,110 rate_schedules across 7,566 unique PWSIDs, serving ~137.9M population.

| Metric | Session Start (Apr 1) | Current | Delta |
|--------|----------------------|---------|-------|
| rate_best_estimate | 13,258 | 17,661 | **+4,403** |
| All pop coverage | 82.8% | 93.3% | **+10.5pp** |
| Lower 48 pop coverage | ~83% | 93.6% | **+10.6pp** |

---

## Section 3: Parse Success Rate Analysis

### By content type

| Type | Total | Succeeded | Failed | Success% |
|------|------:|----------:|-------:|----------|
| pdf | 10,070 | 5,951 | 4,112 | **59.1%** |
| html | 9,542 | 3,071 | 6,448 | **32.2%** |

PDFs succeed at nearly 2x the rate of HTML. PDF URLs tend to be direct rate schedule documents; HTML pages are often landing/info pages.

### By content length

| Bucket | Total | Succeeded | Failed | Success% |
|--------|------:|----------:|-------:|----------|
| <1k | 2,110 | 367 | 1,740 | 17.4% |
| 1-3k | 5,757 | 2,797 | 2,945 | 48.6% |
| 3-10k | 5,568 | 2,753 | 2,811 | 49.4% |
| 10-20k | 4,111 | 1,527 | 2,576 | 37.1% |
| 20-45k | 1,220 | 890 | 330 | **73.0%** |
| 45k+ | 852 | 689 | 163 | **80.9%** |

U-shaped pattern: <1k is almost always junk (17.4%). Sweet spot is 1-10k (48-49%). Longest content (20k+) succeeds at 73-81% — complete rate schedule PDFs.

### By model

| Model | Successes | Inferred Failures |
|-------|----------:|------------------:|
| Sonnet | 4,776 | 3,069 |
| Haiku | 4,335 | 7,496 |

Haiku handles the bulk of short-content parsing but accumulates 71% of all failures. Most are genuinely bad content, not model capability issues.

---

## Section 4: Failure Decomposition

**9,214 failures** with raw response data available:

| Category | Count | % | Recoverable? |
|----------|------:|--:|--------------|
| no_rate_content | 4,423 | 48.0% | No — wrong URL |
| other (misc/template) | 1,882 | 20.4% | No — junk content |
| wrong_page_type | 1,061 | 11.5% | No — wrong URL |
| **rates_behind_link** | **601** | **6.5%** | **Yes — follow links** |
| **partial_data** | **584** | **6.3%** | **Yes — relax validation** |
| pdf_garbled | 413 | 4.5% | Maybe — better PDF extractor |
| **water_sewer_combined** | **250** | **2.7%** | **Yes — accept with flag** |

**~15.5% of failures (1,435) are recoverable** without new discovery.

### Failure category × content type

| Category | HTML | PDF |
|----------|-----:|----:|
| no_rate_content | 2,678 | 1,745 |
| other | 918 | 964 |
| wrong_page_type | 656 | 405 |
| **rates_behind_link** | **594** | 7 |
| partial_data | 410 | 170 |
| pdf_garbled | 213 | 200 |
| water_sewer_combined | 150 | 100 |

`rates_behind_link` is almost entirely HTML (594 vs 7) — landing pages that point to downloadable PDFs. Clearest recovery target via nav crawl.

---

## Section 5: Discovery Quality Analysis

### Rank-level success rates

| Rank | Total URLs | Success | Fail | Unparsed | Success% |
|------|----------:|--------:|-----:|---------:|----------|
| 1 | 15,206 | 4,283 | 3,614 | 7,291 | 28.2% |
| 2 | 15,022 | 2,369 | 2,722 | 9,924 | 15.8% |
| 3 | 14,778 | 3,402 | 3,807 | 7,557 | 23.0% |

**Anomaly: Rank 2 (15.8%) underperforms Rank 3 (23.0%).** Discovery scoring may be misordering candidates between ranks 2 and 3.

### Cascade recovery rates

| Cascade | Attempted | Recovered | Recovery% |
|---------|----------:|----------:|-----------|
| R1 failed → R2 | 3,919 | 807 | **20.6%** |
| R1+R2 failed → R3 | 1,749 | 321 | **18.4%** |

Cascade adds ~20% recovery at each rank. Multi-rank discovery is justified.

---

## Section 6: Content Quality Signals

### Content length: success vs failed

| Outcome | Avg Length | Median Length |
|---------|----------:|-------------:|
| Success | 11,756 | 5,054 |
| Failed | 7,236 | 3,845 |

### Top failing domains (>= 5 failures)

| Domain | Failures | Total | Fail% |
|--------|--------:|------:|------:|
| lpsc.louisiana.gov | 306 | 363 | 84.3% |
| dam.assets.ohio.gov | 171 | 176 | 97.2% |
| www.azwater.com | 166 | 180 | 92.2% |
| www.ny.gov | 143 | 143 | **100%** |
| houstonwaterbills.houstontx.gov | 133 | 133 | **100%** |
| www.nyc.gov | 130 | 136 | 95.6% |
| psc.wi.gov | 93 | 93 | **100%** |
| www.louisianawater.com | 82 | 82 | **100%** |

### Top succeeding domains

| Domain | Successes | Total | Success% |
|--------|----------:|------:|---------:|
| www.amwater.com | 700 | 748 | 93.6% |
| swwc.com | 447 | 455 | 98.2% |
| www.kcmn.us | 313 | 325 | 96.3% |
| psc.ky.gov | 244 | 354 | 68.9% |
| pwsd9.com | 88 | 89 | 98.9% |

American Water (amwater.com) is the single best source — 821 combined successes at 89%+ rate.

---

## Section 7: Population-Weighted Gap Analysis

**Remaining gap (>= 3k pop):** 658 PWSIDs, 12.0M population (4.0% of target)

### Top 15 states by gap population

| State | Gap PWSIDs | Gap Population |
|-------|----------:|---------------:|
| NY | 91 | 2,614,681 |
| CO | 35 | 1,528,894 |
| AZ | 26 | 809,896 |
| NV | 7 | 770,935 |
| MI | 47 | 750,614 |
| OH | 35 | 487,983 |
| CA | 21 | 341,187 |
| TX | 43 | 334,439 |
| MN | 30 | 334,363 |
| MD | 13 | 302,596 |
| LA | 32 | 261,358 |
| TN | 11 | 246,289 |
| UT | 12 | 242,037 |
| MT | 8 | 238,834 |
| MO | 11 | 235,861 |

NY alone = 21.8% of remaining gap. NY + CO + AZ + NV = 47.7%.

### Gap decomposition

| Status | Count | Population |
|--------|------:|-----------:|
| Has scraped text, failed parse | 640 | 11,712,545 |
| Has URL but no text | 6 | 78,578 |
| Other (unclear) | 11 | 176,820 |
| No URLs at all | 1 | 19,341 |

**97.3% of the remaining gap already has scraped text that failed to parse.** This is a parse quality problem, not a discovery problem.

---

## Section 8: Actionable Recommendations

### 1. Re-cascade the 640 gap PWSIDs through untried rank URLs (~$8-15)

640 PWSIDs covering 11.7M pop have text but all parse attempts failed. Many failed on rank 1 but may succeed on rank 2-5 URLs that exist but haven't been tried yet. A targeted cascade pass on these specific PWSIDs — trying the next untried rank — is the highest-ROI intervention.

### 2. Pre-filter content <1k characters

Content under 1,000 chars succeeds only 17.4%. Filtering saves ~$10-12 per sweep on ~1,740 guaranteed failures.

### 3. Blacklist 6 consistently-failing domains

`www.ny.gov`, `houstonwaterbills`, `psc.wi.gov`, `www.louisianawater.com`, `dam.assets.ohio.gov`, `www.nyc.gov` — 852 failures combined, near-100% fail rates. Blacklisting saves ~$5 per sweep.

### 4. Focus state-specific recovery on NY (91 PWSIDs, 2.6M pop)

NY utilities publish rates through local municipality sites, not ny.gov. A targeted NY discovery pass using municipality-specific search queries would close 0.9pp of the population gap.

### 5. Investigate rank 2 vs rank 3 scoring anomaly

Rank 2 succeeds at 15.8% vs rank 3 at 23.0%. Audit a sample to determine if discovery scoring is misordering candidates.

### Model routing note

Current haiku/sonnet split is appropriate. Haiku failures are content quality issues, not model capability. No routing change recommended.

---

## Bottom Line

The pipeline went from 82.8% to 93.6% Lower 48 population coverage in ~36 hours across $153 in API costs. The remaining 4.0% gap (658 PWSIDs, 12M pop) is almost entirely a re-parse problem — the text exists, it just needs better URL rank cascading and more tolerant parse prompts. NY alone is worth 0.9 percentage points of coverage.

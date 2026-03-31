# Cascading Pipeline Funnel Analysis

**Date:** 2026-03-31 | **Post-Scenario A + water_rates sync**

## The Pipeline Funnel

Starting from the full universe of community water systems (pop >= 3k) that don't yet have LLM-scraped rates:

| Stage | Count | Conversion | Cumulative | Drop |
|---|---|---|---|---|
| **0. Universe** (no scraped_llm, pop >= 3k) | 6,260 | — | 100% | — |
| **1. Serper searched** | 6,286* | — | — | — |
| **1a. URL above threshold** (score > 45) | 6,286 | 100% of searched | — | 0% |
| **2. Content retrieved** (rank 1, >100 chars) | 5,430 | 86.4% of 1a | 86.4% | 856 PWSIDs lost |
| **2b. Non-trivial content** (>500 chars) | 4,988 | 91.9% of 2 | 79.4% | 442 thin pages |
| **3. Parse attempted** | ~4,540** | ~91% of 2b | 72.5% | 448 not submitted |
| **4. Parse succeeded** (scraped_llm written) | ~2,070 | 45.6% of 3 | 33.1% | 2,470 failed |

*Some PWSIDs searched but not in current gap universe (they succeeded and left the gap set).
**Scenario A batch size.

**Net pipeline yield: 33% of viable PWSIDs convert to rates.** Two-thirds of the addressable universe is lost at various stages.

## Where Volume Is Lost

```
Universe: 6,260 PWSIDs
    │
    ├── Never searched by Serper: 3,231 (51.6%)
    │       ├── Has non-Serper URL: 1,381
    │       └── Completely untouched: 1,850
    │
    └── Serper searched: 3,029 remaining in gap
            │
            ├── No content retrieved: 514
            ├── Thin content (<500ch): 442
            │
            └── Content available: 2,073
                    │
                    ├── Parse failed: ~1,400
                    │       ├── LLM: "no rates" (low/failed conf): ~1,200
                    │       ├── Bill consistency → low: ~130
                    │       └── JSON error: ~70
                    │
                    └── Not yet attempted: ~673
```

**The biggest gap is at the top: 3,231 PWSIDs (51.6%) were never searched by Serper at all.** These aren't failures — they're unexplored territory. The pipeline hasn't reached them yet.

## The Rank Depth Finding

This is the most actionable finding in the analysis.

### Discovery diagnostics show rank 2/3 wins are enormous

For PWSIDs processed through the **cascade pipeline** (`process_pwsid()`), which tries rank 1 → rank 2 → rank 3 sequentially:

| Winning Rank | Count | % of Successes |
|---|---|---|
| Rank 1 | 258 | 51.3% |
| Rank 2 | 150 | 29.8% |
| Rank 3 | 95 | 18.9% |
| Deep crawl | 9 | 1.8% |
| **Total** | **503** | |

**49% of successful parses came from rank 2 or rank 3.** Rank 1 alone fails to find rates for nearly half of parseable PWSIDs.

### The batch path only tries rank 1

Scenario A's batch submission (`run_scenario_a.py` line 314-318) selects **only the highest-scored candidate per PWSID** and submits that single URL to the batch. There is no cascade — if rank 1 fails, the PWSID is marked as failed. Period.

This is a structural limitation. The batch path has a **theoretical ceiling of ~51% success** (the rank 1 win rate), while the cascade path can reach ~100% of parseable content across all three ranks.

### What rank 2/3 URLs look like for failed PWSIDs

For the 3,063 PWSIDs where rank 1 failed to produce a scraped_llm rate:

| Metric | Count | % |
|---|---|---|
| Has rank 2 URL | 2,828 | 92.3% |
| Has rank 3 URL | 2,529 | 82.6% |
| Rank 2/3 with viable text (>500ch) | 2,184 | 71.3% |
| Rank 2/3 with strong rate signal ($+CCF/per-1000/water rate) | 1,411 | 46.1% |
| Rank 2/3 has rate signal where rank 1 doesn't | 927 | 23.6%* |

*This is the clearest signal: for ~24% of failures, a lower-ranked URL has rate-table content that the top-ranked URL lacks. The scorer ranked the wrong URL higher.

### Recovery estimate

Applying the cascade pipeline's 49% rank 2/3 win rate to the 2,184 failed PWSIDs with viable rank 2/3 text:

**~1,070 additional PWSIDs recoverable** by trying rank 2/3 on current failures.

At ~$0.004/parse attempt (batch pricing, 2 extra attempts), the cost is: 2,184 × 2 × $0.002 = **~$8.70** for an estimated 1,070 new rates.

## Discovery Score Distribution: Failures vs Successes

| Score Band | Failed | % | Succeeded | % |
|---|---|---|---|---|
| 90+ | 561 | 15.5% | 724 | 21.1% |
| 70-89 | 1,572 | 43.5% | 1,692 | 49.4% |
| 50-69 | 1,152 | 31.9% | 838 | 24.5% |
| 45-49 | 2 | 0.1% | 1 | 0.0% |
| <45 | 325 | 9.0% | — | — |

The score distributions overlap heavily. A PWSID with a score of 75 is nearly as likely to fail as to succeed. **The discovery score is a weak predictor of parse success.** High scores don't guarantee rates are in the content — they just mean the URL looks like it should contain rates.

## Serper Query Funnel (per-PWSID averages)

| Stage | Avg Count |
|---|---|
| Queries run | 4.0 |
| Raw results (10/query) | 36.4 |
| After dedup | 26.7 |
| Above threshold (>45) | 4.8 |
| Written to registry | 2.5 |

The pipeline runs 4 queries per PWSID and gets ~37 raw results, but only writes 2.5 URLs to the registry. There are 4.8 candidates above threshold but only 2.5 get written (top 3 limit). **There may be viable URLs sitting at position 4-5 that are never tried.**

## URL Depth Distribution

| URLs per PWSID | Count | % |
|---|---|---|
| 1 URL | 355 | 5.6% |
| 2 URLs | 673 | 10.7% |
| 3 URLs | 5,162 | 82.1% |
| 4+ URLs | 96 | 1.5% |

82% of PWSIDs have the full 3 URLs. The 355 single-URL PWSIDs are particularly vulnerable — if that one URL fails, there's no fallback.

## Strategic Implications

### 1. The batch path needs a cascade (Biggest lever)

The current batch path sends 1 URL per PWSID. The cascade path's data proves that trying all 3 ranks recovers 49% more successes. Two approaches:

**Option A: Submit 3 tasks per PWSID to the batch** (3× cost, ~$0.006/PWSID vs $0.002). For 4,540 PWSIDs that's $27 vs $9. The first success wins; the rest are wasted. But the waste rate is bounded: if rank 1 succeeds (51%), rank 2/3 cost is wasted. If rank 1 fails, rank 2/3 have a 49% combined success rate. Expected cost per success drops because you're recovering PWSIDs that would otherwise be total losses.

**Option B: Two-pass batch submission.** First batch = rank 1 only (cheap). Second batch = rank 2/3 for PWSIDs where rank 1 failed (targeted). This is what `process_pwsid()` does synchronously — the batch version just splits it into two sequential batches with a 24hr gap between them.

**Option C: Route failures to cascade pipeline.** After batch processing, route failed PWSIDs through `process_pwsid()` (direct API, not batch). More expensive per-task ($0.004 vs $0.002) but the cascade handles rank 2/3 and deep crawl automatically. At 2,470 failures × $0.004 × 2 extra attempts = ~$20.

### 2. The scorer is ranking wrong URLs high (Second lever)

24% of failures have rate content in rank 2/3 that rank 1 lacks. The keyword-based scorer rewards URLs that *look like* rate pages (domain, path, title) but can't tell whether the actual content contains extractable rates. Two directions:

**Positive signal boosting** (pull right answers up):
- Boost URLs where scraped text contains `$X.XX per`, `CCF`, `per 1,000 gal`, `tier 1/2/3`, `residential`
- Boost URLs with table-like structure in the text (headers + numeric rows)
- This requires a post-scrape re-scoring step that looks at content, not just URL/title

**Content-aware re-ranking** after scrape:
- The current re-score (line 290-299 in run_scenario_a.py) uses only the first 200 chars of scraped text as a snippet proxy
- Expanding to check for rate-signal keywords in the full text would be cheap and high-impact
- This doesn't require changing the scorer — just changing how the batch path selects the "best" candidate

### 3. 3,231 PWSIDs haven't been searched yet (Third lever)

Half the universe hasn't been searched by Serper. These are likely:
- States not yet targeted by discovery sweeps
- PWSIDs below the population cutoff used in prior sweeps
- PWSIDs that failed the coverage check in pwsid_coverage

Running Serper discovery for these 3,231 PWSIDs costs ~3,231 × 4 queries × $0.001 = **~$13** and would produce an estimated 2,500+ new URLs based on the 86% content retrieval rate.

### 4. Expand to N > 3 URLs per PWSID (Fourth lever)

Currently the pipeline writes top 3 URLs. But Serper returns 4.8 above-threshold candidates per PWSID on average. The 4th and 5th URLs are discarded. For the 355 single-URL PWSIDs and the many failures, having more candidates to try would increase the cascade's surface area.

The marginal cost is zero (Serper already returned these results; they're just not written to the registry).

## Priority Ranking by Impact

| Priority | Lever | Est. Recovery | Est. Cost | ROI |
|---|---|---|---|---|
| 1 | **Cascade batch failures through process_pwsid** | ~1,070 PWSIDs | ~$20 | 54 PWSIDs/$ |
| 2 | **Search the 3,231 unsearched PWSIDs** | ~900-1,200 PWSIDs | ~$13 + parse | ~70 PWSIDs/$ (discovery only) |
| 3 | **Content-aware re-ranking** (post-scrape) | Quality improvement | $0 (code change) | Free |
| 4 | **Expand to 5 URLs per PWSID** | ~200-400 PWSIDs | $0 (data already in Serper results) | Free |
| 5 | **Two-pass batch** (rank 1 then rank 2/3) | ~1,070 PWSIDs | ~$9 second batch | 119 PWSIDs/$ |

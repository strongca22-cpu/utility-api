# Rank Conversion Assessment — Scenario A + Cascade Batch Combined

**Date:** 2026-04-01 | **Post-Scenario A + Truncation + Cascade batches**

## Full Pipeline Funnel

| Stage | Count | Conversion | Note |
|---|---|---|---|
| Total CWS | 44,643 | — | |
| Serper searched | 11,452 | 25.7% of CWS | 74% never searched |
| Got at least 1 URL | 11,413 | 99.7% of searched | Discovery hit rate is excellent |
| Has scraped text (>200ch) | 8,238 | 72.2% of URLs | 28% lost at scrape |
| Has scraped_llm rate | 5,505 | 66.8% of text | Two-thirds of viable text parses |
| In best_estimate | 13,255 | | (includes bulk sources) |

**Key takeaway:** The pipeline converts 66.8% of viable text into rates. The two biggest losses are upstream: 74% of CWS never searched, 28% of URLs never scraped.

## Rank Win Distribution

### From discovery_diagnostics (cascade pipeline, n=503)

| Rank | Count | % | Avg Score |
|---|---|---|---|
| Rank 1 | 258 | 51.3% | 70.3 |
| Rank 2 | 150 | 29.8% | 67.0 |
| Rank 3 | 95 | 18.9% | 64.2 |

### From rate_schedules source_url matching (all batches, n=2,614)

| Rank | PWSIDs Parsed | % | Cumulative |
|---|---|---|---|
| Rank 1 | 713 | 27.3% | 27.3% |
| Rank 2 | 985 | 37.7% | 65.0% |
| Rank 3 | 916 | 35.0% | 100.0% |

**This is the most important finding.** In the batch data (which includes the cascade batch), **rank 2 produced MORE successful parses than rank 1** (985 vs 713). Rank 3 is nearly as productive (916). The conventional assumption that rank 1 is the best candidate is wrong for this pipeline — it's the best-scored URL by the keyword heuristic, but not the most likely to contain parseable rates.

**Implication:** The URL scorer is systematically ranking the wrong URLs high. Rank 2/3 URLs with lower heuristic scores parse at a higher rate. The content-aware re-scoring (just implemented) should help, but the underlying scorer needs attention.

## Rank Recovery

When rank 1 fails, rank 2-5 recovers the PWSID **53.9% of the time:**

| Metric | Value |
|---|---|
| PWSIDs with rank 1 text | 7,625 |
| Rank 1 succeeded | 4,097 (53.7%) |
| Rank 1 failed | 3,528 (46.3%) |
| Recovered by rank 2+ | 1,901 (53.9% of failures) |
| Rank 2 recovered | 985 |
| Rank 3 recovered | 916 |

**This validates the multi-rank strategy.** Without rank 2/3, the pipeline would have 1,901 fewer PWSIDs. The cascade/shotgun batch approach pays for itself many times over.

## Discovery Score vs Parse Success

| Score Band | Total | Succeeded | Success % |
|---|---|---|---|
| 90+ | 2,217 | 1,024 | 46.2% |
| 70-89 | 5,926 | 2,457 | 41.5% |
| 50-69 | 3,802 | 1,282 | 33.7% |
| 30-49 | 169 | 3 | 1.8% |

**The score is weakly predictive.** A 90+ URL succeeds only 46% of the time — barely better than 70-89 at 42%. The heuristic score tells you the URL *looks like* a rate page, not that it *is* one. The 30-49 band has almost zero success, confirming the threshold at 30 is appropriate as a floor.

**Opportunity:** The 33.7% success rate in the 50-69 band means ~2,500 PWSIDs at moderate scores were parseable. The scorer should be tuned to promote these higher — many of them are rank 2/3 URLs that only get tried after rank 1 fails.

## Content Type: PDF vs HTML

| Type | Total | Succeeded | Success % |
|---|---|---|---|
| PDF | 5,090 | 2,915 | **57.3%** |
| HTML | 2,942 | 1,357 | **46.1%** |

**PDFs parse better than HTML.** This is counterintuitive but makes sense: a PDF rate schedule is a self-contained document with the rate table, while an HTML page might be a utility homepage, a general "services" page, or a contact page that mentions rates without containing them.

**Implication:** The scorer should boost PDF URLs that contain rate keywords. Currently PDF gets a small bonus in the heuristic, but the data suggests it should be stronger.

## Text Length vs Parse Success

| Length | Total | Succeeded | Success % |
|---|---|---|---|
| <500 chars | 610 | 254 | 41.6% |
| 500-2k | 1,250 | 582 | 46.6% |
| 2k-5k | 1,996 | 1,012 | 50.7% |
| 5k-15k | 1,611 | 862 | 53.5% |
| 15k-45k | 1,988 | 1,140 | 57.3% |
| 45k+ | 659 | 444 | **67.4%** |

**Longer text parses better.** The success rate climbs monotonically from 42% at <500 chars to 67% at 45k+. This confirms:
1. The 15k→45k text cap raise was high-value (57% vs 54% in the truncation zone)
2. Thin content (<500 chars) still converts at 42% — worth attempting, not worth filtering pre-submission
3. The LLM handles long documents well

## URL Quality by Rank

| Rank | URLs | Has Text | Avg Score | Has "water rate" | Has CCF | Sewer-only | PDF |
|---|---|---|---|---|---|---|---|
| 1 | 12,114 | 66% | 74.8 | 3,082 | 2,961 | 2,200 | 65% |
| 2 | 11,946 | 40% | 65.1 | 2,110 | 1,804 | 1,392 | 58% |
| 3 | 11,720 | 37% | 59.4 | 1,920 | 1,716 | 1,188 | 53% |
| 4 | 6,880 | 0% | 53.3 | — | — | — | 50% |
| 5 | 6,941 | 0% | 50.0 | — | — | — | 45% |

Rank 4-5 have zero scraped text — they were just added by the URL depth expansion and haven't been scraped yet. Rank 1 has the highest average score but also the most sewer-only contamination (2,200 — 18% of rank 1 URLs).

## Rank 1 Failure Content Analysis (n=3,820)

| Category | Count | % | Interpretation |
|---|---|---|---|
| Has "residential" + "rate/charge" | 2,314 | **61%** | **Strong rate signal — parse should have worked** |
| Has "water rate" | 1,369 | 36% | Right topic, LLM couldn't extract |
| Has volumetric (CCF/per-1000) | 1,205 | 32% | Likely has actual rates |
| Sewer-only | 1,093 | 29% | Discovery ranked wrong utility type |
| Thin content (<500ch) | 356 | 9% | Scrape failure |
| Error/404 pages | 137 | 4% | Dead URL |
| Meeting/agenda docs | 41 | 1% | Discovery ranked wrong page type |

**61% of rank 1 failures have "residential" + "rate/charge" in the text.** These pages talk about rates but the LLM couldn't extract a structured rate table. This is the biggest lever for parse improvement — better prompting, rate table detection, or two-pass extraction for complex formats.

**29% sewer-only** — the discovery scorer doesn't distinguish water from sewer. Adding water/sewer disambiguation would eliminate ~1,000 wasted parse attempts.

## Strategic Priorities

### 1. Scorer overhaul — rank 2 outperforms rank 1

The data shows rank 2/3 producing more successful parses than rank 1 in the batch data. The keyword heuristic is systematically overvaluing certain URL patterns (homepage-style URLs, generic utility pages) and undervaluing actual rate documents. The content-aware boost (just implemented) is a first step, but the base scorer needs:
- Stronger PDF bonus when combined with rate keywords
- Water/sewer disambiguation (negative weight for sewer-only URLs)
- Domain-path pattern refinement (e.g., `/rates/` in path is strong, `/about/` is weak)

### 2. Parse improvement for "right content, wrong extraction" (61% of failures)

2,314 rank 1 failures have residential rate language in the text. These are the right pages — the LLM just couldn't extract the data. Possible improvements:
- Two-pass extraction (extract structure first, compute bills second)
- Table-detection preprocessing (identify rate tables before LLM)
- Prompt refinement for complex rate structures (conservation tiers, seasonal)

### 3. Search coverage — 74% of CWS never searched

Only 11,452 of 44,643 CWS have been searched. The current sweep adds ~5,000, but that still leaves ~28,000 unsearched. Most are small systems (<1k pop), but there's a long tail of coverage value.

### 4. Scrape coverage — 28% of URLs lack text

8,238 of 11,413 PWSIDs with URLs have scraped text. The 28% gap is from:
- Rank 4-5 URLs (newly written, not yet scraped)
- JS-heavy pages that fail even with Playwright
- Dead links / 403s / download-only PDFs

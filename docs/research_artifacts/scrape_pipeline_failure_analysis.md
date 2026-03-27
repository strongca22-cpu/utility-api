# Scrape Pipeline Failure Analysis

**Date:** 2026-03-26
**Trigger:** Portland metro curation yielded 5/27 (19%) — prompted wider assessment

---

## The Five Key Problems

### Problem 1: Domain Guesser URLs are Almost Useless (2% success rate)

4,737 URLs from domain_guesser. Only 108 succeeded (2.3%).

| Outcome | Count | % |
|---------|-------|---|
| Never scraped / 0 bytes | 1,424 | 30% |
| Parse failed | 1,217 | 26% |
| Parse skipped | 1,137 | 24% |
| Thin content (<200 chars) | 717 | 15% |
| Success | 108 | 2% |
| Other | 134 | 3% |

**Root cause:** Domain guesser produces city/county .gov homepages, not utility rate pages. These are 2-5 clicks away from any rate data. The deep crawl can't navigate JS-heavy CivicPlus/Granicus platforms, and when it does find a page, it's often a billing info page (not a rate schedule).

**Scale of waste:** These 4,737 URLs represent the single largest segment of the registry. At 2% success, ~4,630 entries are dead weight consuming pipeline cycles.

### Problem 2: Duke Reference URLs are 4-5 Years Stale (12% success, 38% untouched)

3,718 URLs from Duke's 2020-2021 water affordability study.

| Outcome | Count | % |
|---------|-------|---|
| Never fetched | 1,639 | 44% |
| Success | 451 | 12% |
| Parse failed | 612 | 16% |
| HTTP 404 dead | 381 | 10% |
| Fetched 0 bytes | 313 | 8% |
| Parse skipped | 267 | 7% |
| Other | 55 | 1% |

**Root cause:** Duke URLs pointed to utility websites as of 2020. ~10% are now 404. Many more have redesigned, moved rate pages, or switched to CivicPlus. The 1,639 never-fetched are in `pending_retry` status (deferred to 2026-06-01).

**Scale of opportunity:** 451 successes = 12% yield. If the remaining 1,639 have similar distribution (~12% would work), that's ~200 more records. But the 88% failure rate on attempted URLs suggests most of the remaining will also fail.

### Problem 3: Deep Crawl URLs Sit Unprocessed (3,335 entries, 0% parsed)

3,335 `deep_crawl` entries exist with `status=active` but `last_parse_result=NULL`. These are secondary URLs discovered during deep crawling of other pages. They were registered in the scrape_registry but never fed into the parse pipeline.

**Root cause:** The orchestrator's task queue prioritization doesn't systematically revisit deep_crawl URLs. They were discovered as follow-up links but the pipeline moved on to the next PWSID. They're registered but abandoned.

**Scale of opportunity:** Unknown. These URLs were found during deep crawling because they looked rate-relevant (scored high on link keywords). Some may be the actual rate pages that were 1-2 clicks deep from the original URL. But without parsing them, we can't know.

### Problem 4: Parse Failures on Substantial Content (1,477 entries with >1K chars)

2,031 total parse failures. 1,477 had >1,000 characters of content — meaning the scraper got something, but the LLM couldn't extract rate data from it.

Top failure domains:
- Generic `.org` / `.com` city sites: ~574 (landing pages, billing info pages without rate tables)
- `amwater.com`: 38 (American Water legal tariff format — 130+ page PDFs)
- `municode.com` / `franklinlegal.net` / `citycode.net`: 60 (municipal code sites with ordinance text, not rate schedules)
- `nexbillpayonline.com`: 17 (bill payment portals, no rate data)

**Root cause:** Two sub-problems:
- **Wrong page type:** The scraper found a billing/utility page but not the rate schedule. The content has water-related keywords but no extractable rate structure.
- **Complex format:** Some pages (especially legal tariff PDFs, municipal code ordinances) have rate data but in a format Haiku can't reliably parse. These might work with Sonnet or a tariff-specific prompt.

### Problem 5: Most States Have Dismal Pipeline Success Rates

| Tier | States | Avg Success Rate | Pattern |
|------|--------|-----------------|---------|
| Good (>10%) | TX, PA, WA, NJ | 13-17% | States with curated URLs or EFC data |
| Poor (2-8%) | CA, VA, KS, NM, KY, ME | 2-8% | Mix of domain-guessed + Duke URLs |
| Near-zero (<2%) | AR, AL, IN, AZ, CO, LA, IA, CT, IL, AK | 0-2% | Almost entirely domain-guessed URLs |

**Root cause:** States with curated URLs (`curated` source: 93% success) do well. States relying on domain guesser or stale Duke URLs don't. The pipeline's success rate is almost entirely a function of input URL quality, not LLM parsing capability.

---

## The Numbers That Matter

| Metric | Count |
|--------|-------|
| Total scrape_registry entries | 12,678 |
| Successfully parsed | 699 (5.5%) |
| Parse failed | 2,031 (16%) |
| Parse skipped | 3,538 (28%) |
| Never touched | ~6,410 (50.5%) |
| Dead (404) | 651 (5.1%) |
| Pending retry (deferred) | 2,072 (16.3%) |

**The curated source benchmark:** 106 curated URLs → 99 successes (93%). This proves the LLM parse pipeline works well when given the right URL. The problem is upstream: URL quality.

---

## Root Cause Summary

The pipeline has **one fundamental bottleneck: URL quality.** The LLM parsing works (93% on curated URLs). The deep crawling works when it finds something. The PWSID matching works (95%+ for all sources). But:

1. **Domain guesser** produces city homepages, not utility rate pages → 2% success
2. **Duke URLs** are 4-5 years stale → 12% success (and declining as sites redesign)
3. **Deep crawl discoveries** are registered but never revisited → 0% processed
4. **No systematic URL curation** for most states → states default to domain guesser

The path to higher coverage is not "fix the parser" or "improve the scraper." It's: **get better URLs into the registry** — through manual curation (93% success), state directory mining, or targeted search queries (SearXNG: 21% success).

---

## Quantified Opportunities

| Action | Estimated New Parses | Effort |
|--------|---------------------|--------|
| Process 3,335 deep_crawl URLs through parse pipeline | ~200-400 (if 6-12% yield) | Low — just run parser on existing content |
| Re-attempt 1,477 parse failures >1K chars with Sonnet | ~100-200 (10-15% upgrade) | Medium — API cost ~$2-3 |
| Complete Duke 1,639 pending_retry URLs | ~150-200 (12% yield) | Low — just unblock the retry |
| Curation campaign: 10 states × 20 URLs each | ~170-185 (93% yield on curated) | High — requires research per state |
| Purge dead weight: mark 4,630 failed domain_guesser as dead | 0 new parses, but cleaner queue | Low |

---

## Registry Hygiene Issues

- **3,335 deep_crawl entries** registered but never parsed — pipeline leak
- **4,630 failed domain_guesser entries** still `active` — should be `dead` or `stale`
- **1,639 Duke pending_retry** deferred to 2026-06-01 — arbitrary date, could process now
- **Parse=skipped (3,538):** Need to understand skip reasons — are these "no content" or "already covered"?

# Rate Pipeline Failure Analysis

**Date:** 2026-03-26
**Scope:** End-to-end analysis of failure modes across scraping, PDF extraction, LLM parsing, and rate validation.

---

## Pipeline Overview

The rate extraction pipeline has four stages, each with distinct failure modes:

```
URL Discovery → Fetch/Scrape → LLM Parse → Validation & Write
     ↓              ↓              ↓              ↓
  (registry)    (raw text)    (structured)    (water_rates)
```

**Current state of the pipeline (13,184 registry entries):**

| Status | Count | % |
|--------|------:|---|
| Never attempted | 5,454 | 41.4% |
| Skipped (pre-filter) | 3,851 | 29.2% |
| Failed (post-LLM) | 2,912 | 22.1% |
| **Success** | **967** | **7.3%** |

The overall conversion rate from registered URL to extracted rate is **7.3%**. Of the 7,730 entries that have been attempted (skipped + failed + success), the success rate is **12.5%**. Excluding skips (which are mostly stub pages that correctly filter out), the parse success rate on substantive content is **24.9%** (967 / 3,879).

---

## Stage 1: URL Discovery & Source Quality

Different URL sources produce dramatically different success rates:

| Source | Total | Attempted | Success | Success Rate |
|--------|------:|----------:|--------:|-----------:|
| curated | 106 | 100 | 99 | **99.0%** |
| duke_reference | 3,718 | 1,618 | 451 | **27.9%** |
| deep_crawl | 3,491 | 1,180 | 253 | **21.4%** |
| searxng | 85 | 59 | 21 | **35.6%** |
| state_directory | 630 | 85 | 22 | **25.9%** |
| domain_guesser | 5,004 | 4,635 | 115 | **2.5%** |
| domain_guess | 55 | 37 | 0 | **0.0%** |
| civicplus_crawler | 22 | 4 | 1 | **25.0%** |

**Key observations:**

- **Curated URLs are nearly perfect** (99%) — hand-verified links to actual rate pages. This is the gold standard but doesn't scale.
- **Duke reference** is the best-performing automated source at 28% — these are URLs from a structured reference database.
- **SearXNG** performs well (36%) but on small volume — meta-search finds targeted results.
- **Domain guesser is the worst performer at 2.5%** — guessing `water.cityname.gov/rates` and similar patterns mostly hits stubs, login walls, or irrelevant pages. Its 3,147 skips (63%) reflect how many of these guesses land on empty or non-rate pages.
- **Deep crawl at 21%** — expected, since deep crawl is inherently speculative (following links from pages that were themselves thin).

### Failure: Domain Guesser Dilutes the Registry

The domain_guesser contributes 5,004 of 13,184 entries (38%) but only 115 successes (12% of all successes). Its high skip/fail rates inflate the pipeline's apparent failure rate. Worth considering: tighter URL pattern requirements for domain-guessed URLs before they enter the registry.

---

## Stage 2: Fetch & Scrape Failures

### 2a. Static HTTP Failures (httpx)

| Failure Type | Impact | Visibility |
|---|---|---|
| Connection timeout / DNS failure | URL marked for retry, exponential backoff | Logged |
| 404 Not Found | URL immediately marked `dead` | Logged |
| 403 Forbidden | Triggers Playwright fallback automatically | Logged |
| 5xx Server Error | Retry scheduled in 1 day | Logged |
| SSL certificate errors | Generic error, retry in 1 day | Logged |
| Redirect loops (>5 hops) | Error result | Logged |

These are well-handled. The registry's retry/dead logic is sound.

### 2b. Playwright (Headless Browser) Failures

Playwright is the fallback for JavaScript-heavy pages and 403s. It has subtler failure modes:

| Failure Type | Impact | Visibility |
|---|---|---|
| **`networkidle` never reached** | Silent timeout — site fires continuous XHR (analytics, chat widgets) and never settles. Content may have rendered but is never captured. | **Low** — appears as a generic timeout |
| **Error pages invisible** | Playwright doesn't expose HTTP status codes. A 403/404 rendered as a styled error page looks like a successful scrape with garbage text. | **None** — parse agent receives error page text |
| **2-second settle heuristic** | Lazy-loaded rate tables may not render within the fixed 2s post-idle wait. | **None** — silently incomplete content |
| **Browser resource leaks** | Missing `finally:` block means browser processes aren't cleaned up on exceptions. Under bulk runs, orphaned Chromium processes accumulate. | **None** until OOM |

### 2c. PDF Extraction Failures

PDF extraction is a significant pain point. Current stats show PDFs actually parse *better* than HTML (47% vs 23% success rate on attempted parses), but the failures that do occur are harder to diagnose:

| Failure Type | Impact | Visibility |
|---|---|---|
| **Scanned PDFs (no text layer)** | `pymupdf` returns empty strings for image-based PDFs. Result has `is_pdf=True`, `char_count=0`. Passes through to parse agent as empty text. | **Low** — no error flag, no OCR fallback |
| **Smart page extraction misses rate pages** | The rate-page detector looks for `$X.XX` patterns + keywords. Rates expressed as cents/gallon (`0.884¢/gal`), bare numbers (`Rate: 3.42 per 1,000 gallons`), or tables without dollar signs are missed. When missed, falls back to first 15K chars — usually table of contents and definitions, not rate tables. | **None** — silently extracts wrong pages |
| **Double-download on content-type detection** | When a non-`.pdf` URL returns `Content-Type: application/pdf`, the file is downloaded once via httpx, then re-downloaded by `_extract_pdf_text()`. Doubles bandwidth and doubles transient error exposure. | **None** |
| **pymupdf not installed** | Returns placeholder text `"[PDF document — pymupdf not installed]"` instead of an error. This text passes content-length checks and reaches the parse agent, which fails silently. | **None** — looks like a successful scrape |
| **Large tariff PDFs (130+ pages)** | Smart extractor caps at 45K chars (rate pages) or 15K chars (fallback). A tariff where rates appear in section 7 (page 80+) after the cap will never have its rates seen by the LLM. | **None** — appears as `no_tier_1_rate` failure |

**The NJ American Water tariff is the canonical example:** 130 pages, rates buried in specific service-area sections. The smart extractor finds "rate pages" but can't distinguish which service area applies to which PWSID. The parser sees 45K chars of mixed service-area rates and gives up.

### 2d. Content Detection & Deep Crawl

| Failure Type | Impact | Visibility |
|---|---|---|
| **Thin-content false positives** | Pages with rate data in compact HTML tables (few keywords, low char count) are flagged as "thin" and trigger a deep crawl away from the actual rate page. | **None** — original page abandoned |
| **Subdomain links blocked** | Deep crawl only follows same-hostname links. `www.cityofx.gov` won't follow to `water.cityofx.gov`. Many municipal water departments are on subdomains. | **None** — rate pages 1-2 clicks away are missed |
| **`.docx` files not filtered** | Deep crawl skips `.doc` and `.xlsx` but not `.docx`. Binary `.docx` content passes through BeautifulSoup as garbled text, gets registered as a valid deep-crawl URL. | **Low** — parse agent fails silently |
| **Best-candidate fallback** | When no substantive page is found, the "best seen" page (>200 chars) is returned. Often a navigation or overview page, not rate data. | **Low** — `deep_crawled=True` but no quality signal |

---

## Stage 3: LLM Parse Failures

### 3a. Pre-Parse Filter (No LLM Call)

The filter runs before any API call, producing `parse_result="skipped"`:

| Filter Rule | Trigger | Count Impact |
|---|---|---|
| Empty / whitespace only | `len(text.strip()) == 0` | Minimal |
| Too short | `< 100 chars` | Many domain-guess stubs |
| Parked domain detected | GoDaddy, Wix, Squarespace placeholder signals | Moderate |
| Short + no water keywords | 100-500 chars, no `water/utility/sewer/rate/fee` | Many domain-guess stubs |
| No financial content | No `$N` pattern AND no rate keywords | Moderate |

**3,851 entries (29%) are caught here.** The filter is deliberately conservative — it only blocks obviously useless content. The false-negative risk (blocking a real rate page) is low. The 3,147 domain_guesser skips are appropriate.

### 3b. LLM Extraction Failures

After the LLM runs, the result goes through `validate_parse_result()`:

| Validation Check | Failure Reason | Prevalence |
|---|---|---|
| **`no_tier_1_rate`** | LLM returned null/0/"" for `tier_1_rate` | **~98% of all failures** |
| `tier_N_rate_too_low` | Rate < $0.10/CCF | Rare |
| `tier_N_rate_too_high` | Rate > $50.00/CCF | Rare |
| `fixed_charge_high` | Monthly fixed > $500 | Very rare |
| `confidence_failed` | LLM self-reported `confidence: "failed"` | Usually paired with `no_tier_1_rate` |

**The dominant failure mode is `no_tier_1_rate` + `confidence_failed` combined (98.2% of failures).** This means the LLM looked at the text and correctly determined there was no parseable residential tiered rate structure. These are true negatives, not parser bugs — the pages genuinely don't contain the data we're looking for.

### 3c. The 15K Character Truncation Problem

**This is the single most impactful silent failure mode.**

All text sent to the LLM is capped at 15,000 characters (HTML) or 45,000 characters (large PDFs via smart extraction). When truncation cuts off the rate section:

1. The LLM sees introductory content, definitions, general policies
2. It correctly reports `confidence: "failed"` and `tier_1_rate: null`
3. The retry adds a "look harder" prompt, but the truncated text still doesn't contain rates
4. Both attempts fail — no indication that the data exists but was truncated
5. Logged as a standard `no_tier_1_rate` failure, indistinguishable from a genuinely rateless page

**Scale of impact:** Unknown, but likely significant for:
- Legal tariff PDFs (NJ, PA, VA regulated utilities)
- Long policy documents where rates are in an appendix
- Combined water/sewer/electric tariffs where water rates come after other services

### 3d. Retry Logic Gaps

The retry fires only when:
- `no_tier_1_rate` is in the issues list, **AND**
- `raw_text > 2,000 chars`

This means:

| Scenario | Retried? | Problem |
|---|---|---|
| No tier-1 rate found, substantive text | Yes | Working as intended |
| No tier-1 rate found, short text | No | Correct — short text won't improve on retry |
| Rate returned but too high (>$50/CCF) | **No** | Some legitimate rates exceed $50/CCF (very rural, very small systems) |
| Rate returned with `$` prefix as string | **No** | `tier_1_rate: "$3.45"` is truthy, passes the check, but fails float conversion downstream |
| Low confidence with valid-looking data | **No** | Low-confidence parses are computed but never written to DB |

### 3e. Model Routing Edge Cases

| Text Size | Complexity | Routed To | Risk |
|---|---|---|---|
| <10K chars | Simple rate table | Haiku | Low — Haiku handles these well |
| <10K chars | Complex tiered + seasonal | Haiku | **Medium** — may miss nuances |
| >10K chars | Any | Sonnet | Low — appropriate escalation |
| Budget-based / seasonal keywords | Any | Sonnet | Low — correct detection |

### 3f. Non-Standard Confidence Values in DB

The database contains confidence values outside the expected set:

| Confidence | Count | Expected? |
|---|---|---|
| high | 868 | Yes |
| failed | 2,794 | Yes |
| partial | 95 | **No** — not in the prompt's valid values |
| medium | 41 | Yes |
| low | 42 | Yes |
| success | 20 | **No** — LLM confused confidence with status |
| moderate | 15 | **No** — synonym for medium, not in valid set |
| good | 3 | **No** |
| complete | 1 | **No** |

134 entries (1.7% of attempted) have non-standard confidence values. These likely represent early LLM outputs before the prompt was tightened, or Haiku occasionally ignoring the enum constraint. The non-standard values all map to "probably good" — `partial`, `success`, `moderate`, `good`, `complete` — so they may represent lost successes if the validation code doesn't recognize them.

---

## Stage 4: Validation & Write Failures

### 4a. BestEstimateAgent Redundancy

When a parse succeeds, `BestEstimateAgent().run(state=pwsid[:2])` is called — this recomputes the best estimate **for the entire state**, not just the newly parsed PWSID. In a batch processing 52 Texas utilities, this means 52 redundant full-state recomputations. Not a data quality issue, but a significant performance drag.

### 4b. Silent Write Failures

Both `_update_registry()` and `_register_deep_url()` catch and swallow database exceptions at `DEBUG` level. If the DB is briefly unavailable:
- The rate data is extracted and validated correctly
- The DB write fails silently
- The registry still shows `last_parse_result = NULL`
- The entry will be re-attempted on the next run, wasting API cost

---

## Cross-Cutting Issues

### Issue 1: No "Manual Review" Queue

There is no intermediate state between "success" and "failed." Low-confidence parses with plausible data are computed (bills calculated, tiers built) but never persisted. Pages with truncated content that likely contain rates are indistinguishable from genuinely empty pages. There's no way to flag entries for human review.

### Issue 2: Content Not Persisted

The `scrape_registry` stores URL, hash, and content length — but not the actual page text. Every re-parse requires re-fetching the URL. This means:
- URLs that go offline between scrape and parse-attempt lose their data permanently
- Batch re-processing (like the deep crawl backlog run) re-downloads everything
- There's no way to analyze failure patterns on the actual content without re-fetching

### Issue 3: No Unit Conversion Verification

The LLM prompt instructs conversion to $/CCF, but validation only checks range bounds ($0.10–$50.00/CCF). A rate passed through unconverted — e.g., $5.00/1,000 gallons reported as $5.00/CCF instead of the correct $3.74/CCF — would pass all checks. There's no cross-check between the reported unit and the numeric value.

### Issue 4: Duplicate Parse Results

Some PWSIDs appear in multiple registry entries (different URLs, or same URL from different sources). The pipeline processes each independently. If two different URLs for the same utility both parse successfully with different rates, both are written to `water_rates` — the BestEstimateAgent then picks one, but the selection logic may not have enough signal to choose correctly.

---

## Quantified Impact Summary

| Category | Entries | Impact |
|---|---|---|
| **Never attempted** | 5,454 | Untapped potential — largest bucket |
| **Skipped correctly** (stubs, parked) | ~3,500 | Appropriate filter, no action needed |
| **Failed: genuinely no rate data** | ~2,500 | True negatives — URL contained no rates |
| **Failed: truncation** | Unknown (~100-300 est.) | Rates exist but were cut off at 15K chars |
| **Failed: scanned PDF** | Unknown (~50-100 est.) | No OCR, no text extracted |
| **Failed: wrong pages extracted from PDF** | Unknown (~50-150 est.) | Smart extractor grabbed non-rate pages |
| **Failed: subdomain rate pages missed** | Unknown | Deep crawl couldn't follow cross-subdomain links |
| **Non-standard confidence (possible lost successes)** | 134 | May contain valid parses classified as failures |
| **Successfully extracted** | 967 | Current yield |

---

## Prioritized Improvements

These are ordered by estimated impact per engineering effort:

### High Impact, Moderate Effort

1. **Process the 5,454 untouched entries.** Largest single opportunity. Many are `duke_reference` (2,100) and `deep_crawl` (2,311) — sources with proven 21-28% yield.

2. **Persist raw content to disk or DB.** Cache fetched content so re-parses don't require re-fetching. Eliminates data loss from URLs going offline and removes bandwidth waste on re-runs.

3. **Add a "manual review" parse status.** Low-confidence parses with plausible data, truncated large documents, and non-standard confidence values should be flagged rather than silently dropped.

### Medium Impact, Low Effort

4. **Normalize non-standard confidence values.** Map `partial`→`medium`, `success/good/complete`→`high`, `moderate`→`medium` in a one-time DB update. May recover ~100 rate records.

5. **Fix subdomain link following in deep crawl.** Check base domain (not hostname) when filtering deep-crawl links. e.g., `water.cityofx.gov` and `www.cityofx.gov` share base domain `cityofx.gov`.

6. **Add `.docx` to deep-crawl skip list.** One-line fix preventing garbled binary from entering the registry.

### Medium Impact, Higher Effort

7. **Improve PDF smart page extraction.** Expand the rate-page regex to catch `cents/gallon`, bare decimal rates, and `per 1,000 gallons` patterns alongside `$X.XX`. Consider a two-pass approach: regex first, then LLM page scoring.

8. **Add OCR fallback for scanned PDFs.** When `pymupdf` returns zero text from a PDF, run `pytesseract` or similar. Many small utility tariffs are scanned paper documents.

9. **Increase the 15K char cap with targeted extraction.** Instead of blindly truncating, use a table-of-contents parser to find the rate section and extract only that portion, allowing the LLM to see relevant content from any position in the document.

### Lower Priority

10. **Batch BestEstimateAgent calls.** Accumulate successful parses per state and run one BestEstimate call per state at end of batch, not per-PWSID.

11. **Fix Playwright browser leak.** Add `finally:` block to `browser.close()`. Prevents OOM on long bulk runs.

12. **Retry on rate-too-high failures.** Some legitimate rural systems charge >$50/CCF. Either widen the bound or add a retry with explicit instruction to verify.

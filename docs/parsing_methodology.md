# Parsing Approach & Methodology

## Overview

The pipeline converts water utility rate page URLs into structured rate data through a four-stage process: **URL Discovery → Scrape → Parse → Integration**. Each stage has its own methodology, failure modes, and quality controls.

---

## Stage 1: URL Discovery (Metro Research Agent)

### Purpose
Find the specific web page URL where each utility publishes its water rate schedule.

### Method
- **Model:** Claude Sonnet (`claude-sonnet-4-20250514`) with `web_search` tool enabled
- **Batch size:** 5 utilities per API call
- **Execution mode:** Anthropic Message Batches API (50% cost reduction, async)

### Prompt Design
The system prompt instructs the model to:
- Search for **actual rate pages** with dollar amounts, tier structures, or fee schedules
- Reject general "about our water" pages — find the specific rates/fees page
- Use search strategies adapted to utility type:
  - City utilities: `"{city name} water rates"`
  - Water districts: `"{district name} rate schedule"`
- Prefer `.gov` and `.org` domains
- Return only URLs confirmed via search results (no hallucinated URLs)

### Output Format
The model returns YAML with structured results per utility:

```yaml
- pwsid: "XX1234567"
  url: "https://exact-url-to-rate-page"
  confidence: high    # high / medium / low / none
  notes: "Found on city website rates page"
```

### Response Parsing
The API response contains interleaved content blocks (text, tool_use, tool_result) from web searches. The YAML results are extracted from the **last text block** after all searches complete. The parser handles:
- YAML wrapped in markdown code fences
- Results wrapped in a dict (e.g., `{"results": [...]}`)
- Single-dict responses (one utility result)

### Dedup Controls
Before researching, the template generator excludes PWSIDs that:
- Already have `has_rate_data = TRUE` (rate data exists in the database)
- Already have pending/active URLs in `scrape_registry` (any source)
- Were previously researched by `metro_research` (any status, including dead) — prevents cross-metro duplicate API spend

### Known Failure Modes
- **~50% of batches** return prose commentary instead of structured YAML in the final text block. The model completes web searches but narrates findings instead of formatting as YAML. This is a prompt engineering issue — the batches that do produce YAML have 90%+ hit rates.
- Military installations, very small systems, and private utilities often have no public rate pages.

### Cost
- Immediate mode: ~$0.75 per batch of 5 utilities (web search injects 100-300K input tokens)
- Batch API mode: ~$0.38 per batch (50% discount)

---

## Stage 2: Scrape (ScrapeAgent)

### Purpose
Fetch the raw text content from the discovered URL.

### Method
- **No LLM** — pure HTTP fetching with Playwright fallback for JavaScript-rendered pages
- **Deep crawl:** Multi-level (default depth 3) when initial page is thin
- **JS detection:** If initial fetch returns <100 characters, retries with headless Playwright browser
- **Content change detection:** SHA-256 hash comparison against `last_content_hash` in scrape_registry

### Deep Crawl Logic
When the landing page doesn't contain rate data directly (common for government homepages):

1. **Level 1:** Broad navigation scoring — follows links with keywords like "water", "utility", "departments", "services"
2. **Level 2+:** Rate-focused scoring — follows links with "rate", "fee", "billing", "tariff"
3. Each level follows 1-3 same-domain links ranked by keyword relevance
4. Stops when substantive content is found (rate numbers detected) or max depth reached

### Thin Content Detection
A page is classified as "thin" (needs deeper crawling) when it:
- Contains fewer than ~3,000 characters, AND
- Lacks actual rate numbers (`$/unit` patterns)

This catches corporate landing pages that discuss rates conceptually but link to actual tariff documents.

### Scrape Registry Updates
After each fetch, updates `scrape_registry` with:
- `last_fetch_at`, `last_http_status`, `last_content_hash`, `last_content_length`
- Status transitions: pending → active (on success), pending → dead (404), pending → blocked (403)
- Retry scheduling: 5xx errors get `retry_after` set to 6 hours

### Known Failure Modes
- JS-heavy sites with client-side rendering (even Playwright can miss dynamic content)
- PDFs that require OCR (not currently handled — text-selectable PDFs work)
- Rate calculators (interactive tools, no static rate page)
- Sites behind login walls or CAPTCHA

---

## Stage 3: Parse (ParseAgent)

### Purpose
Extract structured water rate data from raw scraped text using an LLM.

### Pre-Parse Content Filter
Before calling the LLM, a heuristic filter rejects content that obviously doesn't contain rate data. This saves ~74% of API costs. Content is **skipped** if:

| Condition | Threshold |
|---|---|
| Empty content | 0 chars |
| Too short | < 100 chars |
| Parked/placeholder domain | "domain is for sale", "coming soon", "welcome to nginx", etc. |
| Short + no water keywords | < 500 chars and no mention of water/utility/rate/fee/billing |
| No financial content | No dollar signs (`$\d`) AND zero rate keywords |

The filter is deliberately conservative — ambiguous pages still go to the parser.

### LLM Rate Extraction

**Model routing** based on text complexity:
- **Haiku** (`claude-haiku-4-5-20251001`): ~70-80% of calls. Used for simple rate structures (short text, few tier keywords).
- **Sonnet** (`claude-sonnet-4-20250514`): ~20-30% of calls. Used for complex structures (>10K chars, 6+ tier keywords, or signals like "budget-based", "drought", "seasonal", "allocation").

**Prompt design:**
- System prompt defines the extraction task and unit conversion rules
- Extracts **residential rates only** (not commercial, industrial, or irrigation)
- Uses smallest standard residential meter size (typically 5/8" or 3/4")
- All volumetric rates normalized to **$/CCF** (1 CCF = 100 cubic feet = 748 gallons)
- Tier limits normalized to **CCF**
- Fixed charges normalized to **monthly** (bimonthly ÷ 2, quarterly ÷ 3)

**Unit conversion rules embedded in prompt:**

| Source Unit | Conversion to $/CCF |
|---|---|
| $/gallon | × 748 |
| $/1,000 gallons | × 0.748 |
| $/HCF | = $/CCF (identical) |
| Gallons (tier limits) | ÷ 748 → CCF |
| Kgal (tier limits) | × 1.337 → CCF |

**Response format:** JSON object with forced prefix (`{"role": "assistant", "content": "{"}`) to ensure JSON output. Fields:

| Field | Type | Description |
|---|---|---|
| `rate_effective_date` | string | YYYY-MM-DD or null |
| `rate_structure_type` | enum | flat, uniform, increasing_block, decreasing_block, budget_based, seasonal, unknown |
| `billing_frequency` | enum | monthly, bimonthly, quarterly |
| `fixed_charge_monthly` | number | Base/service charge normalized to $/month |
| `meter_size_inches` | number | Meter size (e.g., 0.625 for 5/8") |
| `tier_N_limit_ccf` | number | Upper limit of tier N in CCF (null for last tier) |
| `tier_N_rate` | number | Volumetric rate for tier N in $/CCF |
| `parse_confidence` | enum | high, medium, low, failed |
| `notes` | string | Extraction notes, assumptions, edge cases |

Up to 4 tiers supported.

### Retry Logic
When the first parse attempt fails to find `tier_1_rate` but the content is substantive (>2,000 chars), a **retry with rate-search addendum** is attempted. The retry prompt adds explicit instructions to look for:
- Rates expressed as $/gallon, $/ccf, $/1,000 gallons, per unit
- Monthly service charges or base charges
- Water charges in fee schedules or budget documents
- Rates embedded in tables or list formats

If the retry produces a valid result with high/medium confidence, it replaces the original.

### Validation
After extraction, the parsed result is validated:

| Check | Rule |
|---|---|
| Tier 1 rate present | `tier_1_rate` must not be null |
| Rate bounds | Each tier rate must be $0.10–$50.00 per CCF |
| Fixed charge bound | Must be ≤ $500/month |
| Confidence | Must not be "failed" |

### Bill Computation
From the parsed tiers, bills are computed at three benchmark consumption levels:

| Benchmark | Gallons | CCF Equivalent | Purpose |
|---|---|---|---|
| `bill_5ccf` | 3,740 | 5 | Low-use household |
| `bill_10ccf` | 7,480 | 10 | Average household |
| `bill_20ccf` | 14,960 | 20 | High-use household |

Bill = `fixed_charge_monthly` + Σ (volume in tier × rate per 1,000 gal) for each tier.

### Conservation Signal
For utilities with 2+ tiers, the **conservation signal** is the ratio of the highest to lowest volumetric rate:

```
conservation_signal = max(tier_rates) / min(tier_rates)
```

A ratio >1.0 indicates increasing-block pricing (conservation incentive). Higher ratios = stronger price signal.

### Confidence Gate
Only results with `parse_confidence` of **"high" or "medium"** are written to `rate_schedules`. Low and failed results are logged but not integrated — they update `scrape_registry` with the failure status for potential retry or manual review.

### Batch API Mode
When running via `BatchAgent`, all parse tasks are submitted as a single Anthropic Message Batch (50% cost reduction). The batch processor applies the same validation, bill computation, and DB write logic as the synchronous path. Results are stored in a `batch_jobs` table with full task detail JSONB for crash recovery.

### Cost
- Haiku: ~$0.001-0.003 per parse (80% of calls)
- Sonnet: ~$0.005-0.015 per parse (20% of calls)
- Batch API: 50% off both models
- Pre-parse filter saves ~74% of calls that would produce no usable data

---

## Stage 4: Integration

### Database Writes
Successfully parsed rates are written to `rate_schedules` with:
- `source_key = 'scraped_llm'`
- Full JSONB tier structures (`fixed_charges`, `volumetric_tiers`)
- Computed bills at 5/10/20 CCF
- Conservation signal
- Source URL and parse timestamp
- Confidence and model metadata

Idempotent via `ON CONFLICT (pwsid, source_key, vintage_date, customer_class) DO UPDATE`.

### Best Estimate Selection
After each successful parse, `BestEstimateAgent` runs for the affected state. It selects the single best rate estimate per PWSID from all available sources using a configured priority ranking:

| Priority | Source | Description |
|---|---|---|
| 1 | `swrcb_ear_2022` | CA state-reported eAR data |
| 1 | `efc_nc_2025` | NC EFC survey data |
| 2 | `swrcb_ear_2021` | Prior year eAR |
| 3 | `scraped_llm` | LLM-parsed web scrapes |
| 4 | `owrs` | CA OWRS data |
| 5 | `swrcb_ear_2020` | Older eAR |

State-reported survey data takes priority over LLM-parsed scrapes.

### Coverage Refresh
After best estimate updates, `pwsid_coverage` is refreshed with derived columns: `has_rate_data`, `rate_source_count`, `best_bill_10ccf`, etc.

---

## End-to-End Success Rates

Based on the current pipeline run:

| Stage | Input | Output | Success Rate |
|---|---|---|---|
| **URL Discovery** (metro batch) | 197 utilities | 57 URLs | 29% (YAML parsing issues account for most losses) |
| **Scrape** | 126 URLs | 54 texts | 92% (JS sites and dead URLs account for failures) |
| **Parse** | 54 texts | 16 rate records | 30% (pre-parse filter + low-confidence results) |
| **End-to-end** | 197 utilities | 16 rate records | 8% |

The URL discovery YAML parsing issue is the largest single bottleneck — if all 46 batch responses had been parseable, the expected yield would be ~120 URLs → ~100 scraped → ~30 parsed = **~15% end-to-end**, roughly doubling yield.

---

## Assumptions & Limitations

1. **Residential only.** The parser extracts residential rates exclusively. Commercial, industrial, and irrigation rates are ignored even when present on the same page.

2. **Up to 4 tiers.** The extraction schema supports a maximum of 4 volumetric tiers. Utilities with more complex structures (e.g., California budget-based rates with 5+ tiers) are flagged as "budget_based" or "seasonal" and typically receive lower confidence.

3. **Static pages only.** Interactive rate calculators, which require user input (address, meter size, usage) to display rates, cannot be scraped. These are flagged as thin content.

4. **No OCR.** Scanned PDF tariffs are not currently handled. Text-selectable PDFs work via pdfplumber/pymupdf extraction.

5. **Point-in-time snapshot.** Rates are scraped as-of the fetch date. Historical rates are not tracked unless multiple vintages are available from the source.

6. **Unit conversion accuracy.** All unit conversions follow the documented formulas embedded in the system prompt. The LLM performs the conversions — misinterpretation of ambiguous units (e.g., "per unit" without specifying CCF vs gallon) is the most common source of parse error.

7. **Confidence as quality gate.** Only high/medium confidence results enter the dataset. This means some valid rate data is excluded (false negatives) to avoid including incorrect data (false positives). The trade-off favors data quality over coverage.

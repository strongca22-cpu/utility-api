# Right Content, Wrong Extraction — Deep Analysis

**Date:** 2026-04-01 | **n = 2,373 PWSIDs**

These are rank 1 failures where the scraped text contains "residential" + "rate/charge" — the pipeline found the right page but the LLM couldn't extract a structured rate.

## Summary

The 2,373 "right content" failures break into three root causes:

| Root Cause | Count | % | Fix |
|---|---|---|---|
| **Combined water+sewer confusion** | 1,185 | 50% | Prompt: extract water-only charges, ignore sewer |
| **Rate table present but format unrecognized** | 992 | 42% | Two-pass extraction or table detection |
| **Rate mentioned, no actual table** | 341 | 14% | Wrong page — discovery improvement |

*Categories overlap.*

## Profile

| Metric | Value |
|---|---|
| PDF | 1,524 (64%) |
| HTML | 849 (36%) |
| Median text length | 8,775 chars |
| Has dollar amounts | 2,005 (84%) |
| Has "per 1,000 gal" | 950 (40%) |
| Has fixed/base charge | 1,268 (53%) |
| Has "monthly" | 1,595 (67%) |
| Has tier/block language | 252 (11%) |
| Has "CCF" | 149 (6%) |

**84% have dollar amounts. 53% mention fixed/base charges. 40% mention per-1000-gallon pricing.** These are pages with actual rate data — the information is there.

## Root Cause 1: Combined Water+Sewer (50%)

**1,185 failures (50%) have both "water" and "sewer" with dollar amounts on the same page.**

This is the single largest parse failure mode. When a utility publishes water and sewer rates on the same page or PDF, the LLM has to:
1. Identify which charges are water-only
2. Ignore sewer charges
3. Not double-count combined billing

The current parse prompt says "extract the water rate structure" but doesn't explicitly instruct the LLM to separate water from sewer. When the page has a combined water+sewer bill table, the LLM either:
- Returns `confidence: "failed"` because it can't cleanly separate them
- Returns the combined rate (which would be caught by bill outlier checks as too high)
- Returns the sewer rate instead of water

**Examples from the data:**
- St. Charles MO (pop 100k): URL is literally `/Water-Sewer-Rates` — combined page
- WV American Water (pop 207k): Tariff PDF has sewer rate schedule as primary content, water buried later
- Anne Arundel County MD (pop 290k): "NEW UTILITY BILL RATES" card with water + wastewater combined

**Fix:** Add explicit prompt language: "If the page contains both water and sewer rates, extract ONLY the water rates. The sewer/wastewater charges should be ignored. If you cannot separate water from sewer charges, set confidence to 'low' and explain in notes." The LLM can do this — it just needs the instruction.

## Root Cause 2: Rate Table Present But Unrecognized (42%)

**992 PWSIDs have dollar amounts + CCF/per-1000/tier language — a clear rate table — but the LLM didn't extract it.**

These break into sub-categories:

### 2a. PDF table extraction failure (majority)
64% of right-content failures are PDFs. PDF text extraction often produces garbled table layouts where column alignment is lost. A rate table that looks like:

```
Tier 1    0-5 CCF    $3.50/CCF
Tier 2    5-15 CCF   $4.75/CCF
```

Gets extracted as:

```
Tier 1 0-5 CCF $3.50/CCF Tier 2 5-15 CCF $4.75/CCF
```

The LLM sees a run of text with rates in it but can't reconstruct the table structure.

**Example — Chester Water Authority (PA, pop 140k):**
```
Volumetric Rates
Village Green East
Village Green West
Tier 1
$5.08
$6.53
Tier 2
$5.52
$7.09
```
The tier rates are there but split across lines without clear column association. The LLM can't tell which dollar amount goes with which tier for which service area.

**Example — Las Vegas Valley Water District (NV, pop 1.5M):**
The PDF starts with backflow charges, not residential rates. The residential rate table is buried on a later page that may be beyond the 45k text cap. The LLM sees plumbing fees and gives up.

### 2b. Ordinance/legal format
189 PWSIDs (8%) have rates embedded in municipal ordinance language:

```
Section 52-44. Water rates.
(a) The following rates shall be charged for water service:
    (1) First 2,000 gallons or fraction thereof: $12.50 minimum charge
    (2) Next 3,000 gallons: $4.25 per 1,000 gallons
    (3) All over 5,000 gallons: $5.75 per 1,000 gallons
```

This is perfectly parseable but the legal formatting (section numbers, subsections, "thereof") confuses the LLM's rate extraction.

### 2c. Rate study / cost-of-service documents
123 PWSIDs (5%) are rate studies — long analytical documents that discuss rates in context of cost modeling. The actual rate table may be on page 47 of a 60-page PDF. The LLM sees 45k chars of analysis and can't find the rate table.

**Fix for 2a-c:** Two-pass extraction:
- **Pass 1** (cheap, Haiku): "Does this text contain a residential water rate table? If yes, extract just the section containing the rates (the table, the tier structure, the charges). Return only that section."
- **Pass 2** (on extracted section): Standard rate parse prompt on the focused section.

This would cost ~$0.003 extra per PWSID but dramatically improve extraction on long PDFs and ordinances by removing context noise.

## Root Cause 3: Rate Mentioned, No Table (14%)

**341 PWSIDs have "residential" + "rate" in the text but no actual rate table.**

These are:
- Utility homepages that say "view our rates" with a link (not the actual page)
- Budget documents that mention rate increases without the rate schedule
- Application forms that reference rates

**Example — Sugar Land TX (pop 91k):**
```
Water Utility Rates
The fiscal year 2026 budget includes increases to water and wastewater
rates beginning January 1, 2026...
```
This mentions rates but doesn't contain them. The actual rates are behind a link.

**Fix:** This is a discovery/deep-crawl problem, not a parse problem. The reactive deep crawl in `process_pwsid` should follow links from these pages. Ensuring the deep crawl triggers on "rate mentioned but no dollar amounts" would catch these.

## Text Length Distribution

| Length | Count | % |
|---|---|---|
| <500 chars | 154 | 6% |
| 500-2k | 170 | 7% |
| 2k-5k | 596 | 25% |
| 5k-15k | 550 | 23% |
| 15k-45k | 697 | 29% |
| 45k+ | 206 | 9% |

The distribution is spread evenly — no single length bucket dominates. The 29% in the 15k-45k range were previously truncated at 15k and may still be affected by rate tables being deep in the document.

## State Concentration

Top states for "rate table likely but parse failed":

| State | Count | Notes |
|---|---|---|
| TX | 212 | Large MUD landscape, many small districts |
| LA | 96 | Many combined water+sewer pages |
| SC | 51 | |
| FL | 45 | |
| CA | 45 | Complex rate structures (conservation, budget-based) |
| MO | 37 | |
| OK | 35 | |

TX dominates (212 — 21%) because of the Municipal Utility District landscape. Hundreds of small MUDs with rates published in various formats.

## Recommended Solutions — Ranked by Impact

### 1. Prompt fix for water/sewer separation (~1,185 PWSIDs)

**Effort:** Low (prompt change only)
**Impact:** 50% of right-content failures

Add to the parse system prompt:
```
IMPORTANT: If the page contains both water and sewer/wastewater rates,
extract ONLY the residential water supply rates. Ignore all sewer,
wastewater, stormwater, and solid waste charges. If you cannot
clearly separate water charges from combined charges, set
parse_confidence to "low" and explain in notes.
```

This costs nothing (same API call) and directly addresses the #1 failure mode. Can be tested on a sample of 50 combined-page PWSIDs before batch deployment.

### 2. Two-pass extraction for long documents (~700+ PWSIDs)

**Effort:** Medium (new extraction step)
**Impact:** Primarily the 15k+ text group (903 PWSIDs, 38%)

Pass 1 (section extraction): "Find the residential water rate table in this document and return only that section — the tier structure, dollar amounts, and any base/fixed charges."

Pass 2 (standard parse): Run the standard parse prompt on the extracted section.

Cost: ~$0.003 extra per PWSID. For 903 candidates: $2.71.

### 3. PDF table structure preservation (~600+ PWSIDs)

**Effort:** Medium-High (PDF processing change)
**Impact:** The 64% that are PDFs with garbled tables

Improve the PDF text extraction to preserve table column alignment. Options:
- Use `pdfplumber` instead of `pdfminer` for table-aware extraction
- Pass PDF as image to a vision-capable model
- Pre-process with tabula-py to extract tables as structured data

### 4. Ordinance pattern recognition (~189 PWSIDs)

**Effort:** Low (prompt addition)
**Impact:** 8% of right-content failures

Add to prompt: "Rates may be embedded in legal/ordinance format with section numbers (e.g., 'Section 52-44'). Extract the rate values regardless of legal formatting."

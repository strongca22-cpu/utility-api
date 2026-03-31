# Metro Top-20 Targeted Scan — v0

## Context

The utility-api project has a metro research pipeline (`scripts/run_metro_scan.py`) that uses Claude API + web search for targeted URL discovery on high-priority utilities. Three metros have been completed (Denver, Atlanta, Portland). The pipeline feeds into the standard scrape/parse flow via `scrape_registry` with `url_source='metro_research'`.

The Scenario A batch (4,540 PWSIDs, all gap >=3k pop) is currently processing at Anthropic. When it returns, many of these top-20 PWSIDs may have been resolved. **This scan must deduplicate against Scenario A results before processing.**

Additionally, the Serper discovery pipeline has been upgraded since the metro research agent was last used:
- Score threshold lowered from 50 → 45
- Discovery queries improved for multi-area PWSIDs (CA service area fix)
- 15k → 45k LLM text cap raise
- PDF section extraction for multi-area tariffs
- Rate structure type normalization (6 canonical types)
- Bill computation fix (fixed charge for empty tiers)

These improvements should be integrated into or leveraged by the metro research pipeline.

## Target PWSIDs

The top 25 uncovered PWSIDs by population (>=50k pop, no valid scraped_llm rate):

| PWSID | St | Pop | County | Current Source | Utility |
|---|---|---|---|---|---|
| TX1010013 | TX | 2,202,531 | Harris | Duke | CITY OF HOUSTON |
| NV0000090 | NV | 1,539,277 | Clark | none | LAS VEGAS VALLEY WATER DISTRICT |
| NY5110526 | NY | 1,100,000 | Suffolk | none | SUFFOLK COUNTY WATER AUTHORITY |
| TX2200012 | TX | 955,900 | Tarrant | Duke | CITY OF FORT WORTH |
| NY2701047 | NY | 496,753 | Orleans | none | MCWA |
| CO0103005 | CO | 487,365 | Arapahoe | none | AURORA CITY OF |
| CO0121150 | CO | 464,111 | El Paso | none | COLORADO SPRINGS UTILITIES |
| CA3010092 | CA | 444,800 | Orange | Duke | IRVINE RANCH WATER DISTRICT |
| MN1270024 | MN | 425,300 | Hennepin | none | Minneapolis |
| CT0930011 | CT | 418,900 | New Haven | Duke | REGIONAL WATER AUTHORITY |
| KS2017308 | KS | 395,699 | Sedgwick | Duke | WICHITA, CITY OF |
| UTAH18026 | UT | 381,174 | Salt Lake | none | SALT LAKE CITY WATER SYSTEM |
| NV0000175 | NV | 376,515 | Clark | none | NORTH LAS VEGAS UTILITIES |
| NV0000076 | NV | 336,534 | Clark | none | HENDERSON CITY OF |
| VA4087125 | VA | 292,000 | Henrico | none | HENRICO COUNTY WATER SYSTEM |
| MD0020017 | MD | 290,606 | Anne Arundel | none | GLEN BURNIE-BROADNECK |
| TX0430007 | TX | 288,800 | Collin | Duke | CITY OF PLANO |
| MD0130002 | MD | 286,158 | Howard | none | HOWARD COUNTY D.P.W. DISTRIBUTION |
| TX1520002 | TX | 275,041 | Lubbock | Duke | LUBBOCK PUBLIC WATER SYSTEM |
| TX0570050 | TX | 264,546 | Dallas | Duke | CITY OF IRVING |
| NJ0906001 | NJ | 262,000 | Hudson | Duke | JERSEY CITY MUA |
| TX2400001 | TX | 260,046 | Webb | Duke | CITY OF LAREDO |
| TN0000116 | TN | 251,864 | Montgomery | none | CLARKSVILLE WATER DEPARTMENT |
| TX0570010 | TX | 248,822 | Dallas | Duke | CITY OF GARLAND |
| AZ0407090 | AZ | 247,328 | Maricopa | none | CHANDLER CITY OF |

**Combined population: ~13M.** These 25 PWSIDs alone would add significant population coverage.

## Known Complexity Notes

Several of these are expected to be **parse-hard, not search-hard**:

- **Las Vegas Valley (NV0000090), North Las Vegas (NV0000175), Henderson (NV0000076):** Southern Nevada Water Authority region. Drought-tier pricing with conservation-based rate structures, seasonal adjustments, water budget allocations. These utilities have public rate pages — the issue is parse complexity, not URL discovery. The 45k cap raise and improved prompts may help. If standard parsing still fails, these are prime candidates for two-pass extraction (extract structure → compute bill deterministically).

- **Colorado Springs (CO0121150), Aurora (CO0103005):** CO has structural issues — .colorado.gov blocking, JS-heavy sites, special districts. Discovery query improvements may help. 38% parse success rate in CO from Sprint 25 runs.

- **Irvine Ranch (CA3010092):** Multi-district CA utility. CA service area discovery fix just shipped — re-discovery should find the correct rate page now.

- **Houston (TX1010013):** Largest uncovered PWSID by far. Has Duke data but no scrape text at all. Rates are publicly posted at houstonwaterbills.houstontx.gov.

## Tasks

### Task 1: Deduplicate Against Scenario A Batch

Before running the metro scan:
1. Check Scenario A batch status (`python scripts/run_scenario_a.py --check-status`)
2. If complete: process the batch first (`--process-batch`), then re-query the top 25 to see which are now covered
3. If still processing: proceed with the metro scan but flag PWSIDs that are in the Scenario A target set. These may resolve on their own — prioritize PWSIDs that were NOT in Scenario A (those with Duke or other existing data that Scenario A skipped).

The Scenario A batch targeted **gap PWSIDs only** (no rate data at all). PWSIDs with `selected_source = duke_nieps_10state` were NOT in Scenario A. So the Duke-sourced ones (Houston, Fort Worth, Irvine Ranch, Wichita, etc.) are safe to scan immediately without dedup concerns.

### Task 2: Update Metro Research Agent for Current Pipeline

The metro research agent (`scripts/metro_research_agent.py`) may need updates:

1. **Serper integration:** Check whether the research agent uses Serper or Claude's web_search tool. If it uses Claude's web_search, it should work as-is. If it has its own search logic, ensure it uses the updated Serper config (score threshold 45, paid mode).

2. **URL import:** `metro_url_importer.py` writes to `scrape_registry`. Verify it sets `url_source='metro_research'` and that the pipeline processes these URLs with the 45k cap and section extraction.

3. **Pipeline flow:** After URL import, the PWSIDs need to go through `process_pwsid()` which now has the 45k cap, section extraction, bill computation fixes, and rate_structure_type normalization.

### Task 3: Run Targeted Metro Scan

Run `run_metro_scan.py` for the top 25 PWSIDs. This is NOT a full metro scan (which covers all utilities in a metro area) — it's a targeted scan of specific high-population PWSIDs.

Options:
- **Option A:** Add these 25 as a custom target list in `metro_targets.yaml` (new section: `priority_pwsids`)
- **Option B:** Run the research agent directly for each PWSID, bypassing the metro config
- **Option C:** Create a lightweight wrapper script that takes a PWSID list and runs discovery → import → process for each

Option C is probably cleanest — a `scripts/run_targeted_research.py` that:
1. Takes a list of PWSIDs (from CLI or a config file)
2. For each: runs the metro research agent to find URL candidates
3. Imports URLs to scrape_registry
4. Runs process_pwsid() with all current pipeline improvements
5. Reports results

### Task 4: Handle Parse Complexity

For PWSIDs that fail standard parsing (especially NV, CO, CA):

1. **First attempt:** Standard pipeline with 45k cap + section extraction
2. **If that fails:** Two-pass approach — first pass extracts rate structure as JSON (tiers, base charges, seasonal flags), second pass computes bill deterministically in code
3. **If that fails:** Flag for manual curation in the metro curation JSON format

The goal is to push the parse boundary on these complex cases. Every failure teaches us something about the edge cases the LLM struggles with.

### Task 5: Rebuild After Processing

After the scan completes:
1. Rebuild `rate_best_estimate` for affected states
2. Re-export dashboard data
3. Report: how many of the top 25 are now covered, combined population gained

## Key Files

- `scripts/metro_research_agent.py` — Claude API research tool
- `scripts/run_metro_scan.py` — End-to-end orchestrator
- `scripts/metro_template_generator.py` — Template generation from SDWIS
- `scripts/metro_url_importer.py` — URL import to scrape_registry
- `config/metro_targets.yaml` — Metro definitions and priority
- `src/utility_api/pipeline/process.py` — `process_pwsid()` with all current improvements

## What NOT to Do

- Do not re-run PWSIDs that Scenario A will cover — check batch status first
- Do not modify the batch processing pipeline — the metro scan uses the existing direct API path
- Do not curate rates manually — the research agent + pipeline should handle it
- Do not run a full metro scan (hundreds of PWSIDs per metro) — target only the top 25 by population

# Session Summary — Sprint 27: Prompt Consolidation & Coverage Recovery

**Date:** 2026-04-01
**Scope:** Unify parse prompts, add high-impact extraction rules, promote scraped_llm source priority, recover orphan PWSIDs

## What Was Done

### 1. Prompt Consolidation (commit 7e19ef4)

**Problem:** Three code paths (rate_parser.py direct, ParseAgent, BatchAgent) had inconsistent user messages. Only the direct path included utility context, text delimiters, and JSON-only instructions.

**Fix:** Created `build_parse_user_message()` in rate_parser.py — single shared function called by all three paths. Includes:
- Utility name + state code context line
- `--- BEGIN/END SCRAPED TEXT ---` delimiters
- Explicit JSON field list
- Failure handling instructions

All three paths + retry now produce identical message structure.

### 2. System Prompt Rules Added (commit 7e19ef4)

Three new rules targeting ~1,400 recoverable PWSIDs from the "right content, wrong extraction" analysis:

- **Water/sewer separation** (50% of failures, 1,185 PWSIDs): Extract water-only charges, ignore sewer/wastewater/stormwater. If combined bill shown, find water-only components.
- **Ordinance/legal format** (8%, 189 PWSIDs): Recognize rates in `Section 52-44(a)(1)` format. Ordinance "First X gallons: $Y" = standard tiered structure.
- **PDF table awareness**: Garbled table formatting from PDF extraction is expected. Reconstruct rate structure from context clues.

### 3. Retry Addendum Strengthened (commit 7e19ef4)

Retry prompt now includes water/sewer reminder, ordinance acceptance, PDF reconstruction guidance, and permissive partial extraction hint ("medium confidence > failed extraction").

### 4. Raw LLM Response Logging (commit 7e19ef4, migration 024)

Added `last_parse_raw_response` TEXT column to scrape_registry. Stored at all parse call sites (ParseAgent first attempt, retry, batch processing). 50k char cap at write time. Enables post-batch failure diagnosis without re-running individual tasks.

### 5. Utility Metadata Plumbing (commit 7e19ef4)

- `batch_task_builder.py`: Now passes `utility_name` and `state_code` through parse task dicts (fields were available in the query but not passed)
- `ParseAgent`: Added `_lookup_utility_name()` method querying `cws_boundaries.pws_name`

### 6. Source Priority Hierarchy Change (commit 7a6d163)

**Changed:** `scraped_llm` promoted from priority 3 to **priority 1**. Government bulk (eAR, EFC) demoted from priority 1-2 to **priority 3** (fallback). Duke/TML remain at priority 8.

**Rationale:** Head-to-head analysis of 1,396 PWSIDs with both sources:
- Scraped vintage is newer in 69% of cases (59% are 2024+ vs 8% for bulk)
- Scraped completeness slightly better across all fields
- Bulk sources are 80% older than 2022

**CA anchor logic updated:** Divergence from eAR flagged for QA but no longer demotes scraped data.

**Impact:** Only 139 PWSIDs would switch best_estimate source (hierarchy was already close). Strategic signal is the main value.

### 7. Prompt Reparse Batch Submitted (commit 50f345c)

`msgbatch_01JArCR8gqMf7XnVe3etqS67` — 2,807 tasks / 1,693 PWSIDs / ~$13
- Targets: all scrape_registry rows with `last_parse_result = 'failed'`, substantive text, no existing scraped rate
- All url_sources included (serper 2,164, deep_crawl 585, searxng 30, etc.)
- Script: `scripts/run_prompt_reparse.py`

### 8. Orphan Parse Batch Submitted (commit 0b945a8)

`msgbatch_01SEayyJbh6c8prBNDo7dy2T` — 2,496 tasks / 2,274 PWSIDs / 13M pop / ~$12
- Targets: scraped text that was **never sent to the parser** (last_parse_result IS NULL)

**Root cause diagnosed:** The Mar 30 discovery sweep scraped rank 1 URLs for ~4,400 PWSIDs, but the subsequent batch submissions used `--since` date filters that excluded these older rows. The current pipeline's discovery sweep also skips them because they already have `serper` rows in scrape_registry (`NOT EXISTS scrape_registry WHERE url_source = 'serper'` filter).

**Step 2 plan:** After this batch processes, PWSIDs still lacking rates get fed back into the full pipeline with `--force` flag to bypass the "already discovered" exclusion.

Script: `scripts/run_orphan_parse.py`

## Batches In Flight

| Batch | ID | Tasks | PWSIDs | Est. Cost | Label |
|-------|----|-------|--------|-----------|-------|
| Prompt reparse | msgbatch_01JArCR8g... | 2,807 | 1,693 | ~$13 | prompt_reparse_v1 |
| Orphan parse | msgbatch_01SEayyJb... | 2,496 | 2,274 | ~$12 | orphan_parse_v1 |
| Priority pipeline rank 1 | (pending submission) | ~3,500 | ~3,500 | TBD | discovery_r1 |

Plus rank 2-5 scrape and batch from the priority pipeline still to come.

## Files Modified

| File | Change |
|------|--------|
| `src/utility_api/ingest/rate_parser.py` | System prompt rules + `build_parse_user_message()` + refactored direct path |
| `src/utility_api/agents/parse.py` | Shared builder + `_lookup_utility_name()` + `raw_response` in `_update_registry()` |
| `src/utility_api/agents/batch.py` | Shared builder + raw response wiring |
| `src/utility_api/ops/batch_task_builder.py` | utility_name + state_code in task dicts |
| `src/utility_api/models/scrape_registry.py` | `last_parse_raw_response` column |
| `src/utility_api/ops/best_estimate.py` | Anchor logic changed to flag-only |
| `config/source_priority.yaml` | scraped_llm → priority 1, bulk → fallback |
| `migrations/versions/024_add_parse_raw_response.py` | New column migration |
| `scripts/run_prompt_reparse.py` | New script |
| `scripts/run_orphan_parse.py` | New script |

## Next Actions

1. **Process batches** (~24hr): `run_prompt_reparse.py --process-batch` and `run_orphan_parse.py --process-batch`
2. **Process priority pipeline** rank 1 batch when it completes
3. **Analyze recovery rates** by failure category (water/sewer, ordinance, PDF)
4. **Step 2 orphan recovery:** Feed remaining gaps into full pipeline with `--force`
5. **Rebuild best_estimate + dashboard** after all batches processed

## Key Findings

- **13,140 scraped-but-never-parsed rows** existed in the DB — major gap from `--since` filter
- **28,141 PWSIDs** (63% of all CWS) have never been through discovery at all (mostly < 1k pop)
- **Scraped LLM is clearly superior** to bulk sources on vintage (59% from 2024+) and completeness
- **5,229 PWSIDs >= 1k** have rate data exclusively from bulk — can't sunset bulk until scraping covers them
- **Current pipeline covers all >= 1k** (min pop ~581 in current sweep)

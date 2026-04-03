# Sprint 29 — Coverage Gap Strategy

**As of:** 2026-04-03
**Coverage:** 94.3% systems / 96.7% pop (9,531 / 10,109 CWS, pop >= 3k)
**Gap:** 578 PWSIDs, 10.0M pop remaining

## Gap by State — Top 20 by Population

| State | Total | Has Rate | Gap | Gap Pop | Coverage |
|-------|-------|----------|-----|---------|----------|
| CO | 185 | 155 | 30 | 1,435,667 | 83.8% |
| NY | 323 | 268 | 55 | 809,733 | 83.0% |
| NV | 37 | 32 | 5 | 729,599 | 86.5% |
| AZ | 151 | 128 | 23 | 666,123 | 84.8% |
| PR | 52 | 26 | 26 | 563,678 | 50.0% |
| MI | 306 | 267 | 39 | 558,501 | 87.3% |
| OH | 332 | 300 | 32 | 475,832 | 90.4% |
| AK | 26 | 13 | 13 | 337,015 | 50.0% |
| CA | 685 | 666 | 19 | 302,117 | 97.2% |
| TX | 1,246 | 1,213 | 33 | 255,641 | 97.4% |
| UT | 125 | 113 | 12 | 240,127 | 90.4% |
| TN | 285 | 275 | 10 | 238,120 | 96.5% |
| MT | 38 | 31 | 7 | 231,334 | 81.6% |
| MN | 194 | 174 | 20 | 230,720 | 89.7% |
| LA | 270 | 243 | 27 | 214,688 | 90.0% |
| MD | 76 | 66 | 10 | 206,810 | 86.8% |
| SC | 170 | 157 | 13 | 196,965 | 92.4% |
| VA | 154 | 139 | 15 | 184,887 | 90.3% |
| OK | 160 | 139 | 21 | 183,972 | 86.9% |
| MO | 231 | 224 | 7 | 163,110 | 97.0% |

## Pipeline Status

All 578 gap PWSIDs are in the pipeline. No PWSIDs with scraped text are truly orphaned — every unparsed-text PWSID has at least one serper URL from the TC discovery wave (Mar 30+) that is being processed through the automated scrape → batch → parse chain.

| Stage | PWSIDs | Pop |
|-------|--------|-----|
| Has scraped text, awaiting parse | 566 | 9.7M |
| In registry, no text yet | 9 | 234k |
| Not yet discovered | 3 | 45k |

## Opportunities (Ranked by Impact)

### 1. CO Locality Discovery — 30 PWSIDs, 1.4M pop
**Status:** Chat prompt created (`sprint_29_co_deep_dive_v0.md`), active in separate chat. Sprint 29 added CO-specific handling (CITY OF suffix, MD suffixes, FT/MT abbreviations, state mismatch penalty).

**Key targets:** Aurora (487k), Fort Collins (180k), Broomfield (106k), Highlands Ranch (103k), Loveland (95k). Mix of municipal and metropolitan districts. Denver metro pending in `metro_targets.yaml`.

### 2. NV Targeted Investigation — 5 PWSIDs, 730k pop
**Status:** Chat prompt created (`sprint_29_nv_targeted_investigation_v0.md`).

**Key targets:** North Las Vegas (377k), Henderson (337k). Structurally complex — SNWA/LVVWD wholesale system, municipal retail rates. Only 5 PWSIDs; surgical manual investigation justified.

### 3. AZ Locality + PSC Tariffs — 23 PWSIDs, 666k pop
**Status:** Not started.

**Key targets:** Chandler (247k), Arizona Water Co - Pinal Valley (134k), EPCOR San Tan (87k). Split: municipal PWSIDs should respond to locality discovery; private utilities (AZ Water Co, EPCOR) need ACC (Arizona Corporation Commission) tariff lookup.

### 4. PR Custom Approach — 26 PWSIDs, 564k pop
**Status:** Not started. Structural gap.

50% coverage. PRASA/AAA is a single island-wide authority. Rate pages likely in Spanish on `.pr.gov`. Different problem than mainland discovery — may need custom scraping/parsing approach.

### 5. MI/OH Combined — 71 PWSIDs, 1.0M pop
**Status:** Not started.

MI: Ann Arbor (118k) is biggest. OH: Aqua Ohio Massillon (96k), Lorain (64k). Mix of municipal (locality discovery) and private (Aqua → PSC tariff). Standard pipeline should handle most municipals.

### 6. AK — 13 PWSIDs, 337k pop
**Status:** Not started.

Anchorage (221k) dominates. AK utilities may have limited web presence. 50% coverage suggests systematic discovery problems.

### 7. Re-Parse Failures — 511 PWSIDs, ~1,200 URLs with >2k chars
**Status:** Deferred.

These had substantive text but the parser couldn't extract rates. Options: refined prompt, model upgrade (Opus for complex rate structures), or acceptance that some pages genuinely don't contain parseable rate data.

## Active Automation

| Pipeline | Status | Details |
|----------|--------|---------|
| NY chain | Running (tmux `ny_chain`) | Step 3: bug-fix rescrape in progress. Steps 4-7 queued. |
| TC-R2 batch | At Anthropic | `bulk_replace_c_r2` submitted |
| TC-R3+ scrape | Running | Automated chain |
| CO deep dive | Active chat | Sprint 29 handling live |

## Recommended Execution Order

1. **CO** (active) — highest pop gap, locality discovery ready
2. **NV** (prompt ready) — surgical, 730k pop from 5 PWSIDs
3. **AZ** — split municipal/private, design approach
4. **MI/OH** — bulk locality discovery run
5. **Submit TC tail batch** — once TC chain completes R5 scraping, submit all unparsed text
6. **PR/AK** — structural gaps, need custom approaches
7. **Re-parse sweep** — diminishing returns, save for later

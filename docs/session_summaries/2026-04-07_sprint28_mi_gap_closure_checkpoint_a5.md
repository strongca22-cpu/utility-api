# Sprint 28 — MI Gap Closure (Checkpoint A.5 Pause)

**Date:** 2026-04-07
**Status:** PAUSED at Checkpoint A.5 — awaiting PWSID matchup bug fix in separate chat
**Sprint:** 28
**Focus:** Michigan gap closure (35 PWSIDs / 506,756 pop)

---

## What was accomplished

### Task 1: Full MI gap audit (complete)
- **35 MI PWSIDs >=3k pop lack rate_best_estimate** (506,756 pop)
- Prompt originally said 39/559k — verified delta is from organic closures during week of Mar 30–Apr 2 (bulk parse wave), NOT from the tail sweep
- The tail sweep contributed ~0 to MI (only 2 PWSIDs/3 rows in week of Apr 6)
- Audit script: `/tmp/mi_gap_audit.py` (run from project root)
- Full output: `/tmp/mi_gap_audit.out`, `/tmp/mi_gap_audit_v2.out`

### Task 1.5: Parse result vocabulary + failure mode analysis (complete)
- DB stores only three `last_parse_result` values: `NULL` (231), `'failed'` (86), `'skipped'` (2)
- Sprint 27 doc's taxonomy (`wrong_url`, `no_rates`, `rates_behind_link`) is conceptual only — not in DB
- Real failure modes identified from per-URL eyeball of top 10:

| Bucket | Description | Pop | Recovery path |
|---|---|---|---|
| **A. Right page, parser said failed** | Content is there, parser choked | ~140k | Reparse (cheapest win) |
| **B. County aggregator contamination** | County fee schedule, not city water rates | ~145k | Locality discovery + blacklist |
| **C. Wrong-utility name collision** | Charlotte MI→NC, Escanaba→Delta Twp | ~40k | Locality discovery |
| **D. Consultant PDF** | Third-party rate study | ~13k | Locality discovery |
| **E. Never-parsed (still pending)** | 231 URLs never sent to parser | — | Re-trigger (tail sweep gone) |

### Key systems identified for manual top-5 (Task 7, not yet started):
- **MI0000220 Ann Arbor (118k)** — all URLs are washtenaw county contamination, needs fresh locality discovery for `a2gov.org` or `cityofannarbor.org`
- **MI0007260 Ypsilanti CUA (54k)** — has ycua.org + revize PDF, parse=failed on both
- **MI0003190 Holland BPW (50k)** — `hollandbpw.com/rates/business` has 15,042 chars, parse=failed; also thin `hct.holland.mi.us` URL (112ch) is playwright_timeout victim
- **MI0001500 Coldwater (14k)** — `coldwater.org/1883/Rules-Regulations-Rates` has 45,000 chars (!), parse=failed
- **MI0006235 Spring Lake Twp (9k)** — `springlaketwp.org/water-and-sewer-rates/` looks correct, parse=failed

### DOMAIN_BLACKLIST additions (applied)
Added 7 MI domains to `src/utility_api/ingest/rate_parser.py:98-112`:
- `oaklandcountymi.gov` (100%), `oaklandcounty.org` (100%), `oaklandcounty.com` (100%)
- `waynecounty.org` (100%), `waynecounty.com` (100%)
- `stclaircounty.org` (92%)
- `www.michigan.gov` (96% — includes EGLE Playwright download bug)

Blacklist total: 6 → 13 entries.

### Domains considered and rejected:
- `allendalemi.gov` (92%, only 13 URLs, real township) — wait for locality
- `miottawa.org` (24% fail) — legit for some Ottawa Co utilities
- `publicsectorconsultants.com` (33%) — third-party consultant, low failure rate
- `wcwsa.org` (43%) — might be Wayne Co Water & Sewer Authority (legit)
- `bloomfieldtwpmi.gov` (52%) — real township site

### Bug fix: rescrape_diagnose.py
- `c.population_served` → `cb.population_served` in pop_filter (line 56)
- Script was never run with `--min-pop` before; latent SQL alias bug

### Task 2: rescrape_diagnose dry-run (complete)
- **143 candidate URLs across 93 PWSIDs**
- P1 (gap closure): 39 URLs
- P3 (confidence upgrade): 104 URLs
- Bug types: playwright_timeout (114), thin_html (28), form_stripping (1)

### Source_url integrity (resolved in parallel chat)
- Source_url fix landed: global 9% → 86.1% populated
- MI >=3k pop: 0% → 63.5% populated
- selection_notes: 100% empty → 100% populated
- 99 MI rows still NULL source_url — handled by separate thread
- **Tail sweep (PIDs 3034485+) killed during source_url fix** — no longer running

---

## What was NOT done (remaining tasks)

### Task 3: rescrape_recover.py --state MI (READY to run)
```bash
python scripts/rescrape_recover.py --state MI --min-pop 3000 --workers 10
```
143 URLs, 93 PWSIDs, ~1-3 min estimated. No collision risk (tail sweep gone).

### Task 4: MI parse batch (after Task 3)
Build batch from MI gap PWSIDs with substantive text (>=500ch) and no scraped_llm rate.
**DRY-RUN FIRST** — report cost estimate, top 10, wait for approval before submission.

### Task 5: Locality discovery (after Task 4)
```bash
python scripts/run_locality_discovery.py --state MI --dry-run
python scripts/run_locality_discovery.py --state MI
```
Critical for Ann Arbor (118k), county-contaminated systems, and name-collision systems.

### Task 6: Scrape locality URLs + mi_locality_r1 parse batch
Scrape new locality URLs → submit labeled batch → process returns.

### Task 7: Manual top-5 investigation
Priority list (by population):
1. Ann Arbor MI0000220 (118k) — needs `a2gov.org` or city water URL
2. Ypsilanti MI0007260 (54k) — ycua.org exists, parse failed
3. Holland BPW MI0003190 (50k) — hollandbpw.com 15k chars, parse failed
4. Monroe South MI0004455 (34k) — monroemi.gov has content, JS-recovered
5. Coldwater MI0001500 (14k) — coldwater.org 45k chars, parse failed

### Task 8: Final report + commit + next_steps + session summary

---

## Checkpoints remaining

| Checkpoint | When | What |
|---|---|---|
| B | After Task 3 | Review re-scrape recovery count |
| B.5a | Before Task 4 batch submit | Dry-run cost, top 10, wait for go |
| B.5b | Before Task 6 batch submit | Dry-run cost, top 10, wait for go |
| C | After Task 6 | Review remaining gap |
| D | Before Task 7 | Confirm manual top-5 list + approach |

---

## Key files touched

- `src/utility_api/ingest/rate_parser.py` — DOMAIN_BLACKLIST (7 entries added)
- `scripts/rescrape_diagnose.py` — pop_filter bug fix (line 56)

## Key files for reference (NOT touched)

- `scripts/rescrape_recover.py` — next to run (Task 3)
- `scripts/run_locality_discovery.py` — Task 5
- `scripts/run_gap_cascade.py` — pattern for batch submission
- `src/utility_api/agents/scrape.py` — ScrapeAgent (all 4 Sprint 27 bug fixes)
- `src/utility_api/agents/parse.py` — ParseAgent
- `src/utility_api/agents/locality_discovery.py` — LocalityDiscoveryAgent

## Temp files (on filesystem, not in repo)

- `/tmp/mi_gap_audit.py` — audit query script
- `/tmp/mi_gap_audit.out` / `mi_gap_audit_v2.out` — audit results
- `/tmp/mi_gap_delta.py` — delta verification script
- `/tmp/mi_delta_v2.out` — delta results
- `/tmp/mi_parse_vocab.out` — parse result vocabulary + per-URL drilldown
- `/tmp/mi_rescrape_diagnose.out` — diagnose dry-run output
- `/tmp/source_url_status.out` — source_url fix verification

## Critical context

- **Tail sweep is DEAD** (killed during source_url audit). No collision risk for Sprint 28 work. May need to be restarted after MI closure if there's remaining national backlog.
- **PWSID matchup bug identified** — discovery may be filing correct rates to wrong utilities (same class as Bear Creek/North Lincoln/Palisade contamination noted in Sprint 30). Separate chat is investigating. This is WHY Sprint 28 is paused — running locality discovery while the matchup bug exists could propagate contamination.
- **michigan.gov/egle/-/media/... Playwright download bug** — systematic, affects every MI gap PWSID. Root cause: Playwright's `page.goto()` can't handle `Content-Disposition: attachment` responses. Low value (EGLE docs are water-loss audits, not rate schedules). Filed as note, not fixing in Sprint 28.
- **Ann Arbor is the keystone** — 118k pop, 23% of the MI gap. All existing URLs are county fee schedule contamination. Recovery requires locality discovery to find the real city water rate page.

## Success criteria (unchanged from prompt)
- MI gap under 15 PWSIDs / under 200k pop
- Ann Arbor (118k) recovered with high or medium confidence
- L48 coverage moves from 95.8% toward 96.0%+
- All recoveries pipeline-traceable (no manual injection)

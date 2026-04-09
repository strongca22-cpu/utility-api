# MI Gap Closure Sprint 28 — Resume from Checkpoint A.5 v1

## Context

This chat resumes Sprint 28 MI gap closure work that was paused at **Checkpoint A.5** on 2026-04-07. The pause was to fix a PWSID matchup bug in a separate chat (discovery→PWSID filing contamination — same class as Bear Creek/North Lincoln/Palisade from Sprint 30).

**That bug is now resolved.** Resume the MI gap closure pipeline from Task 3.

## What's already done (DO NOT redo)

Full details: `docs/session_summaries/2026-04-07_sprint28_mi_gap_closure_checkpoint_a5.md`

1. **Task 1 (audit):** 35 MI PWSIDs >=3k pop / 506,756 pop in the gap. Failure modes: right-page-but-parse-failed (~140k pop), county-aggregator contamination (~145k pop), wrong-utility name collision (~40k pop), consultant PDFs, never-parsed.
2. **Task 1.5 (parse vocab):** DB stores `NULL`/`failed`/`skipped` only. Per-URL drilldown of top 10 done.
3. **DOMAIN_BLACKLIST additions (applied):** 7 MI county domains added to `src/utility_api/ingest/rate_parser.py` — Oakland, Wayne, St Clair county + michigan.gov. Total 13 entries.
4. **rescrape_diagnose.py bug fix (applied):** `c.population_served` → `cb.population_served` in pop_filter (line 56).
5. **Task 2 (rescrape_diagnose dry-run):** 143 candidate URLs / 93 PWSIDs. 39 P1 (gap), 104 P3 (upgrade). Bug types: playwright_timeout (114), thin_html (28), form_stripping (1).

## What to do now — pick up from Task 3

### Task 3: Re-scrape bug-fix beneficiaries

```bash
python scripts/rescrape_recover.py --state MI --min-pop 3000 --workers 10
```

143 URLs, ~1-3 min. No collision risk (tail sweep killed during source_url audit, not restarted). This is scrape-only — no batch submission.

**Stop at Checkpoint B:** Report how many URLs recovered substantive text (>=500ch). Report per-PWSID recovery for P1 candidates specifically.

### Task 4: Submit recovered + never-parsed batch

Build a batch from MI gap PWSIDs that:
- Have substantive text (>=500ch) after re-scrape
- Have no existing scraped_llm rate
- Best URL per PWSID (longest text)

**DRY-RUN FIRST (Checkpoint B.5a).** Report: task count, model (sonnet), estimated cost, top 10 PWSIDs. **Wait for explicit user approval before submission.**

Pattern: same as `scripts/run_gap_cascade.py` but filtered to MI.

### Task 5: Locality discovery

```bash
python scripts/run_locality_discovery.py --state MI --dry-run
python scripts/run_locality_discovery.py --state MI
```

**Dry-run first**, report Serper query count and cost estimate. Critical for:
- **Ann Arbor MI0000220 (118k pop)** — all existing URLs are Washtenaw County fee schedule contamination. Locality discovery must find `a2gov.org` or city water URL.
- County-contaminated systems (Allendale, Grand Haven Twp)
- Name-collision systems (Charlotte MI→NC, Escanaba→Delta Twp)

### Task 6: Scrape locality URLs + parse batch

Scrape new locality URLs (10 workers, MI locality only). Submit parse batch labeled `mi_locality_r1`. **Dry-run before submission (Checkpoint B.5b).**

### Task 7: Manual top-5 investigation (Checkpoint D first)

For top 5 by population still lacking rates after automated pipeline. Likely candidates:
1. **Ann Arbor MI0000220 (118k)** — if locality discovery didn't find it
2. **Ypsilanti MI0007260 (54k)** — ycua.org exists, parse=failed on both HTML and PDF
3. **Holland BPW MI0003190 (50k)** — `hollandbpw.com/rates/business` has 15,042 chars but parse=failed. Also `hct.holland.mi.us` is a playwright_timeout victim.
4. **Monroe South MI0004455 (34k)** — `monroemi.gov/.../fees` has 6,478ch (JS-recovered), parse=failed
5. **Coldwater MI0001500 (14k)** — `coldwater.org/1883/Rules-Regulations-Rates` has **45,000 chars**, parse=failed. Big page — investigate whether content is actually rates or generic regs.

Confirm the list at Checkpoint D before starting investigation.

### Task 8: Final report + commit + next_steps + session summary

Per-PWSID outcome table for top 10. Coverage delta. Update `docs/next_steps.md`. Write session summary. Git commit.

## Checkpoint cadence (agreed in prior session)

| Checkpoint | When | Action |
|---|---|---|
| B | After Task 3 | Review re-scrape recovery |
| B.5a | Before Task 4 batch submit | Dry-run cost, top 10, wait for go |
| B.5b | Before Task 6 batch submit | Dry-run cost, top 10, wait for go |
| C | After Task 6 | Review remaining gap |
| D | Before Task 7 | Confirm manual top-5 list + approach |

## Success criteria

- MI gap reduces from 35 PWSIDs / 507k pop to **under 15 PWSIDs / under 200k pop**
- Ann Arbor (118k) recovered with high or medium confidence
- L48 coverage moves from 95.8% toward 96.0%+
- All recoveries pipeline-traceable (no manual injection)

## What NOT to do

- Do NOT modify Sprint 27 bug fixes in `rate_scraper.py`
- Do NOT manually inject text into `scrape_registry`
- Do NOT submit batches without dry-run + user approval
- Do NOT run multiple parallel scrapers on overlapping URL sets
- Do NOT modify source priority hierarchy (`config/source_priority.yaml`)
- Do NOT redo Tasks 1-2 or re-run `rescrape_diagnose.py` — results are current

## Key files

- `scripts/rescrape_recover.py` — Task 3
- `scripts/run_locality_discovery.py` — Task 5
- `scripts/run_gap_cascade.py` — batch submission pattern
- `src/utility_api/ingest/rate_parser.py` — DOMAIN_BLACKLIST (already updated)
- `scripts/rescrape_diagnose.py` — already run, bug fixed
- `docs/session_summaries/2026-04-07_sprint28_mi_gap_closure_checkpoint_a5.md` — full prior session context

## Sprint 27 bug fixes (context — already in codebase, already working)

1. `wait_until="load"` instead of `networkidle` — `rate_scraper.py:365`
2. `<form>` excluded from STRIP_TAGS — `rate_scraper.py:57-64`
3. PDF 403 browser UA retry — `rate_scraper.py:428-460`
4. Short-line filter `>3` + preserve `$` lines — `rate_scraper.py:279-286`

## NEW finding from prior session (context only, not Sprint 28 scope)

**michigan.gov/egle/-/media/... Playwright download bug** — Playwright's `page.goto()` crashes on URLs with `Content-Disposition: attachment` response (browser starts download, call throws). Affects every MI gap PWSID but recovery value is ~zero (EGLE docs are water-loss audits, not rate schedules). Already blacklisted via `www.michigan.gov` addition. Separate bug to file later.

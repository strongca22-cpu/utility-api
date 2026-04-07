# source_url Integrity Audit v0 — utility-api rate_best_estimate

## What this chat is for

A focused, **read-mostly** audit of how `source_url` (and other provenance fields) is populated on `utility.rate_best_estimate` and the `utility.rate_schedules` rows feeding it. The goal is to determine whether we have a **data-loss problem** (URLs are gone, can't be reconstructed) or a **join/population problem** (URLs exist somewhere upstream and are simply not being copied/matched onto the best-estimate row).

This chat is **NOT** the MI gap closure (Sprint 28). That work is happening in a separate chat and is paused at Checkpoint A waiting on the outcome of *this* audit. Don't run scrapers or submit batches here.

Working repo: `~/projects/utility-api/`. Schema: `utility` (configurable via `settings.utility_schema`). Use `from utility_api.db import engine` for DB access.

## Why we care — the trigger

While auditing the MI rate gap, a sanity-check query on `rate_best_estimate` returned this:

```
rate_best_estimate.source_url:
  total:     18,506
  NULL:      16,844   (91.0%)
  empty '':  0
  populated: 1,662    (9.0%)
```

`selection_notes` was also empty across the MI rows sampled. `built_at` turned out to be a bulk-rebuild timestamp (15,865 of 18,506 rows = 2026-04-06), not a row-creation timestamp — so it cannot be used to date a closure.

The Sprint 28 closure plan explicitly requires the data trail to include `rate_best_estimate.source_url`, and the project rule is "no manual injection — everything goes through ScrapeAgent → ParseAgent → BestEstimateAgent." If 91% of rows lack `source_url`, either:

- (a) **Data loss**: BestEstimateAgent never had the URL when it built the row, and the underlying `rate_schedules` row also lacks it. Bad outcome — we'd need to re-derive it.
- (b) **Join/copy bug**: The URL exists on the upstream `rate_schedules` row (or somewhere recoverable like `scrape_registry`), but BestEstimateAgent doesn't copy it onto the best-estimate row. Better outcome — backfill is a one-shot SQL update.
- (c) **Hybrid**: some sources populate it, some don't. The 1,662 populated rows (9%) suggest a non-uniform code path.

The user's prior is strongly toward (b)/(c): scraped text is stored, so the URL it came from should be recoverable from `scrape_registry` even in the worst case.

## Tasks

### Task 1 — Read-only schema map

For each of these tables in the `utility` schema, list columns + types and identify which columns plausibly hold a source URL or provenance pointer:

- `rate_best_estimate`
- `rate_schedules`
- `scrape_registry`
- Any other table with `url` or `source` in a column name

Run via `information_schema.columns` query. Report back as a small table.

### Task 2 — Coverage of `source_url` by source_key

Break down the 16,844 NULL rows in `rate_best_estimate` by `selected_source`:

```sql
SELECT selected_source,
       count(*) AS total,
       count(*) FILTER (WHERE source_url IS NULL) AS null_url,
       count(*) FILTER (WHERE source_url IS NOT NULL AND source_url != '') AS has_url
FROM utility.rate_best_estimate
GROUP BY 1 ORDER BY 2 DESC;
```

The hypothesis to test: is this defect concentrated in `scraped_llm` (where the URL very much does exist upstream), or does it span all source types? The 1,662 populated rows — what `selected_source` are they? That tells us which code path *does* populate it correctly, which is the template for fixing the others.

### Task 3 — Upstream URL availability on `rate_schedules`

For the same 16,844 NULL `rate_best_estimate` rows, check whether the corresponding `rate_schedules` row(s) have a URL:

```sql
SELECT rs.source_key,
       count(*) AS rs_rows,
       count(*) FILTER (WHERE rs.source_url IS NULL) AS null_url,
       count(*) FILTER (WHERE rs.source_url IS NOT NULL) AS has_url
FROM utility.rate_schedules rs
JOIN utility.rate_best_estimate rbe
  ON rbe.pwsid = rs.pwsid
WHERE rbe.source_url IS NULL
GROUP BY 1 ORDER BY 2 DESC;
```

(Confirm `rate_schedules.source_url` exists first via Task 1; if the column has a different name, substitute.)

This is the critical question: **does the URL still exist one layer down?** If yes, the fix is a one-shot backfill `UPDATE rate_best_estimate SET source_url = ...`. If no, dig further to Task 4.

### Task 4 — Recoverability from `scrape_registry`

For any `scraped_llm` `rate_schedules` rows that lack a `source_url`, can we reconstruct it from `scrape_registry`? The join key would presumably be `pwsid` plus some shared identifier (scrape id, content hash, parse_run_id, etc. — discover what's actually there). Sample 20 cases where:

- `rate_best_estimate.source_url IS NULL`
- `selected_source = 'scraped_llm'`
- look up all `scrape_registry` rows for that PWSID where `last_parse_result` is something parse-success-like
- show: how many scrape_registry rows per PWSID, do any have the same content_length / parse_date / content_type as the rate_schedules row, and is there a unique mapping?

If there's a clean join (e.g. `scrape_registry.id` referenced in `rate_schedules`), great — backfill is trivial. If matching is fuzzy (multiple candidate URLs per PWSID), report the disambiguation problem and propose a heuristic (e.g., longest content among URLs scraped before the rate_schedules.created_at).

### Task 5 — Find where source_url IS populated correctly

For the 1,662 rows that DO have `source_url`, sample 10 and trace the code path that wrote them. Look in:

- `src/utility_api/agents/best_estimate.py` (or wherever `BestEstimateAgent` lives)
- `src/utility_api/ingest/` for any backfill scripts
- `scripts/` for any ad-hoc backfill / repair scripts that might have populated a subset

Report which code path populated them. That code path is the template for fixing the rest.

### Task 6 — Same audit on `selection_notes`

Repeat Task 2 for `rate_best_estimate.selection_notes`. If it's also broadly empty, note it; the same fix likely closes both. Don't deep-dive — just confirm scope.

### Task 7 — Recommendation

Based on Tasks 1–6, recommend ONE of:

- **Option A — One-shot SQL backfill**: URLs exist downstream, simple `UPDATE ... FROM` query. Provide the proposed query, dry-run row counts, and any safety caveats. Do **not** execute — leave for user review.

- **Option B — Code fix + rebuild**: BestEstimateAgent has a bug. Identify the specific function and the missing line(s). Propose the patch. Do **not** apply — leave for user review. Note that rebuild will be required after the patch.

- **Option C — Combined**: backfill + code fix (most likely). Sequence them.

- **Option D — Genuine data loss**: URLs unrecoverable for some subset. Quantify the subset and propose how to triage (re-discover via locality_discovery? Mark as low-confidence? Drop?).

## Constraints

- **Read-only by default.** No `INSERT`/`UPDATE`/`DELETE`. No scrapers. No batch submissions. No git commits.
- **Do not modify the MI gap closure work** running in a separate chat. The MI tail sweep (PIDs 3034485+) is also still running — leave it alone.
- **No premature fixes.** Even if you spot the bug in Task 5, do not apply it. Report and wait. The user wants to weigh fix-first vs. document-defer vs. hybrid before any code change.
- **Stop and report after each task** if you find something surprising. Don't burn through the whole list silently.
- **Quote real numbers** — no rounding, no eyeballing. Sprint 27 had a "5% recovery" mistake from skipping dry-runs; same discipline applies here.

## What to deliver at the end

A single report with:

1. The defect's true scope (not just MI — global)
2. The root cause (data loss vs. join bug vs. code bug)
3. The recommended fix (Option A/B/C/D above) with the actual SQL or code diff
4. A list of any other provenance fields with the same defect (`selection_notes`, etc.)
5. Estimated effort to apply the fix
6. Any blockers or open questions for the user

The user will then decide whether to apply the fix before resuming MI gap closure (Sprint 28) or after.

## Scratch context / known facts

- `rate_best_estimate` columns observed: `pwsid, utility_name, state_code, selected_source, bill_estimate_10ccf, bill_5ccf, bill_10ccf, bill_6ccf, bill_12ccf, fixed_charge_monthly, rate_structure_type, rate_effective_date, n_sources, anchor_source, anchor_bill, confidence, selection_notes, built_at, source_url`
- All 271 MI rows ≥3k pop have `selected_source = scraped_llm`, `confidence = medium`, `source_url = NULL`, `built_at = 2026-04-06`, `selection_notes` empty
- `rate_schedules.created_at` IS a real row-create timestamp (verified — distinct values per row)
- `rate_best_estimate.built_at` is NOT a row-create timestamp — bulk-rebuilt 15,865 rows on 2026-04-06
- `rate_schedules.source_key` uses values like `scraped_llm` (and others — enumerate in Task 2)
- The `DOMAIN_BLACKLIST` in `src/utility_api/ingest/rate_parser.py` confirms multiple distinct upstream sources exist
- Schema name lives in `settings.utility_schema` (default appears to be `utility`)

## Done condition

A complete report (Tasks 1–7) delivered to the user, no DB writes performed, no code changes applied, no commits made. End the chat after delivery.

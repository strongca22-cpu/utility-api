# source_url Integrity Audit + Orphan Recovery (R0)

**Date:** 2026-04-06
**Focus:** Audit `rate_best_estimate.source_url` 91% NULL defect, identify root cause, recover URL provenance without re-parsing
**Coverage delta:** `rate_best_estimate.source_url` 9.0% → **95.1%** populated (1,662 → 17,658 of 18,575)

---

## Trigger

Sprint 28 (MI gap closure) sanity-check on `rate_best_estimate` revealed:

```
rate_best_estimate.source_url:
  total:     18,506
  NULL:      16,844 (91.0%)
  populated: 1,662  (9.0%)
```

The Sprint 28 closure plan required `source_url` as part of the data trail. Discovery: the defect spanned **all source types**, not just MI. Even bulk sources (Duke, EFC, WV PSC, etc.) that have URL populated 88-100% upstream in `rate_schedules` were 0% populated downstream in `rate_best_estimate`.

---

## Root Cause — stale Python imports in long-running daemons

The on-disk code in `src/utility_api/ops/best_estimate.py` was **correct** — end-to-end repro on `NJ0707001` produced the right URL. Yet the latest per-state rebuilds at 2026-04-06 20:15 produced NULL.

**Cause:** Two long-running Python processes had imported `ops/best_estimate.py` *before* commit `0de853f` (Sprint 25, 2026-03-30 10:11) which added the `source_url` propagation logic. Python does not auto-reload modules — these processes held cached pre-Sprint-25 bytecode for over a week.

| Process | Started | What it did wrong |
|---|---|---|
| `parse_sweep.py --interval 1800` (PID 1901967) | 2026-03-29 | Imported `BestEstimateAgent`, ran per-state rebuilds after every parse cycle using stale code |
| `bulk_scrape_parallel.py --workers 20` (PID 3034485 + 20 children) | 2026-04-04 | Long-running trailing scraper, "MI tail sweep" |

The temporal pattern of NULL `source_url` by `built_at` day proved this:

| Day | Total rows built | source_url populated | Rate |
|---|---:|---:|---:|
| 2026-04-03 | 693 | 603 | 87% |
| 2026-04-04 | 1,359 | 1,045 | 77% |
| **2026-04-05** | **535** | **0** | **0%** |
| **2026-04-06 (bulk rebuild)** | **15,865** | **0** | **0%** |

The bulk rebuild on Apr 6 reprocessed 15,865 previously-populated rows with the broken cached code, wiping their URLs.

---

## Fix Sequence

1. **Killed `bulk_scrape_parallel`** — parent + 20 worker children (`pkill -TERM -f bulk_scrape_parallel.py`)
2. **Killed `parse_sweep.py`** — bash parent + python child (PID 1901965 + 1901967)
3. **Snapshot `rate_best_estimate`** to `data/backups/rate_best_estimate_pre_rebuild_20260407T050330Z.csv` (18,506 rows, 3 MB)
4. **Re-ran `BestEstimateAgent().run()`** from a fresh Python process — restored URL propagation via the corrected code path
   - Result: 18,575 rows, 86.1% URL coverage (15,984 populated)
5. **Spot-checked `NJ0707001`** → `https://ecode360.com/35312120` ✓

The remaining 13.9% NULL was concentrated in 2,643 `scraped_llm` rows from a 2026-03-31 batch sweep that wrote with NULL `source_url` AND NULL `raw_text_hash` from the start (separate code path defect, distinct from the stale-import regression).

---

## R0 Orphan Recovery — fingerprint matching, $0 cost

### Key insight

The 2,643 orphan `rate_schedules` rows were **provenance-loss only**, not data loss. Each row had:
- ✅ Valid bills (`bill_5ccf`, `bill_10ccf`, etc.)
- ✅ Valid `fixed_charges` and `volumetric_tiers` JSONB structures
- ✅ Specific `parse_notes` mentioning locality (e.g., "La Porte, TX 3/4 inch meter", "Pennsylvania-American Water Rate Zone 1")
- ❌ `source_url` (NULL — only thing missing)

For the 2,450 affected PWSIDs:
- **100%** had at least 1 candidate URL in `scrape_registry` (discovery state intact)
- **8,250** total candidates with cached `scraped_text` (discovery + scrape state preserved)

The fix was to **reconstruct the join key from existing evidence** by searching cached scraped text for fingerprint tokens from each orphan row. **Zero re-parsing, zero web fetches, zero LLM calls.**

### Matcher (`/tmp/r0_matcher_v2.py`)

For each orphan rs row, build fingerprint tokens:
- **Numeric tokens:** distinctive bill amounts, fixed charge amounts, volumetric tier rates (with format variants `$X.XX`, `X,XXX.XX`, integer fallback). Excludes common values (`$5.00`, `$10.00`, etc.).
- **Phrase tokens:** capitalized multi-word phrases extracted from `parse_notes` (proper nouns, hyphenated names like `Pennsylvania-American`, city/state pairs like `La Porte, TX`)

Score each candidate `scrape_registry` row by token presence in `scraped_text`. Phrase hits are weighted 5× numeric hits.

Confidence tiers:
- **`match_phrase`** — phrase hit AND clear winner over runner-up (highest confidence)
- **`match_strong_numeric`** — ≥3 numeric hits, clear winner
- **`match_medium_numeric`** — 2 numeric hits, clear winner
- **`match_unique_candidate`** — only one candidate available
- **`match_weak_unique`** — 1 numeric hit, no competition (deferred — cross-state contamination risk)
- **`skip_tie_or_weak`** / **`skip_zero_hits`** — unmatched

### Dry-run results (2,643 orphan rows)

| Confidence | Count | % |
|---|---:|---:|
| match_phrase | 1,018 | 38.5% |
| match_strong_numeric | 492 | 18.6% |
| match_weak_unique | 319 | 12.1% |
| skip_tie_or_weak | 264 | 10.0% |
| match_medium_numeric | 228 | 8.6% |
| skip_zero_hits | 177 | 6.7% |
| match_unique_candidate | 142 | 5.4% |
| skip_no_tokens | 3 | 0.1% |
| **Total matched (any tier)** | **2,199** | **83.2%** |

### Execution decision

Executed **only the trusted tiers** (`match_phrase`, `match_strong_numeric`, `match_medium_numeric`, `match_unique_candidate`) = **1,880 UPDATEs** with 0 failures. Deferred the 319 `match_weak_unique` cases — these had 1 numeric hit and no competition, but the dry-run showed cases like `kcmn.us` (Minnesota water rates PDF) winning 1-hit matches for non-MN PWSIDs (cross-state contamination via generic round-number matches).

After UPDATE, re-ran `BestEstimateAgent().run()` to propagate URLs to `rate_best_estimate`.

---

## Final State

| Metric | Pre-audit | Post-rebuild | **Post-R0** |
|---|---:|---:|---:|
| `rate_best_estimate.source_url` populated | 1,662 (9.0%) | 15,984 (86.1%) | **17,658 (95.1%)** |
| NULL | 16,844 (91.0%) | 2,591 (13.9%) | **917 (4.9%)** |
| `scraped_llm` URL coverage | 8.2% | 83% | **95%** |
| `rate_schedules.scraped_llm.source_url` | 84.4% | — | **95.5%** |

### Per-source post-R0

All sources except `tx_tml_2023` are now at 95-100%:
- All EFC vintages (Duke, eAR, OWRS, WV PSC, NM NMED, KY PSC, IN IURC): **100%**
- `scraped_llm`: **95%**
- `tx_tml_2023`: **0%** (genuine source-level loss — TX TML ingest never had URL upstream)

### 917 NULL residual breakdown

| Bucket | Count | Status |
|---|---:|---|
| `tx_tml_2023` | 171 | Genuine source-level loss (separate from this audit) |
| `scraped_llm` weak_unique deferred | ~319 | R0 found candidate with 1 hit but cross-state risk |
| `scraped_llm` ties + zero hits | ~444 | R0 couldn't disambiguate — needs v3 matcher OR targeted reparse |
| Misc (NaN, no_estimate, long-tail) | ~83 | Pre-existing data quirks |

---

## Files Produced

- `data/backups/rate_best_estimate_pre_rebuild_20260407T050330Z.csv` — 18,506 rows, 3 MB
- `data/backups/rate_schedules_orphans_pre_R0_20260407T053447Z.csv` — 2,643 rows, 2.3 MB
- `/tmp/r0_matcher_v2.py` — fingerprint matcher (not yet moved into repo)
- `/tmp/chat_prompt_utility_api_backups_v0.md` — backup system chat prompt (drafted, not executed)

`data/` is gitignored, so backup CSVs are local-only. They will move to `~/backups/utility-api/` when the backup system is implemented.

---

## Key Findings

1. **Long-running Python daemons are a silent regression vector.** A daemon imported before a code fix will keep using the broken code indefinitely. Restart on every meaningful code change is required, OR the daemon needs an explicit module-reload mechanism.

2. **Two compounding bugs masked each other:**
   - `parse_sweep` running stale `BestEstimateAgent` was wiping correctly-populated URLs
   - 2026-03-31 batch sweep was inserting `rate_schedules` rows with NULL URLs from the start
   Without process isolation, both showed up as "rate_best_estimate.source_url is NULL" without distinguishing themselves.

3. **The full provenance chain (URL → text → parse → rate) was preserved in the database — just stored in two places without a foreign key.** The rates, the URLs, and the source page text were all intact in `rate_schedules` and `scrape_registry`. Only the JOIN key was missing. **Recovery cost: $0** (vs $41 batch reparse, vs $74 fetch+reparse, vs Serper+fetch+reparse).

4. **Confirmed defensive bug** in `src/utility_api/agents/parse.py:439-450` and `src/utility_api/agents/batch.py:511-522`: the `INSERT INTO rate_schedules ... ON CONFLICT DO UPDATE SET` clause does not refresh `source_url` or `scrape_timestamp` on conflict. Re-parses with new URLs silently keep the old URL. Not the cause of the current state but will cause future drift.

5. **Cross-state contamination** flagged in MEMORY.md (`feedback_search_keyword_optimization`) is real but bounded. Sample inspection of `kcmn.us` matches confirms it: a Minnesota water rates PDF was being weakly matched to South Carolina, Tennessee, and other PWSIDs via generic round-number matches.

6. **Sprint 28 (MI gap closure) is fully unblocked** — `source_url` is now populated for all MI rows whose underlying scrape produced a URL.

---

## Memory Updates

- (added) Long-running daemons with stale imports as a regression vector — should restart `parse_sweep` and similar after any `ops/best_estimate.py` or `agents/best_estimate.py` change.

---

## Next Steps (deferred to discrete chats)

1. **Move `/tmp/r0_matcher_v2.py` into repo** as `scripts/recover_orphan_urls.py` with proper CLAUDE.md-style header (housekeeping)
2. **R0 v3 matcher** for the 763 residual scraped_llm orphans — better scoring rules, content-length normalization, multi-document detection. Could recover another 200-400.
3. **Backup system implementation** — chat prompt at `/tmp/chat_prompt_utility_api_backups_v0.md`. Three tiers: daily pg_dump, pre-write snapshots, offsite copy.
4. **Defensive `ON CONFLICT` patch** — 4-line fix in `parse.py:439` and `batch.py:511` to refresh `source_url` and `scrape_timestamp` on conflict.
5. **Resume Sprint 28 MI gap closure** from Checkpoint A.

---

## What we did NOT do

- Did NOT move `/tmp/r0_matcher_v2.py` into the repo (deferred to next chat for proper headers + commit)
- Did NOT apply the `ON CONFLICT` defensive patch (deferred — known issue, no immediate harm)
- Did NOT investigate the exact code path that wrote the 2,643 NULL-URL rows on 2026-03-31 (forensically interesting but action-irrelevant — recovery doesn't depend on it)
- Did NOT touch the dashboard (read-only consumer; would benefit from restart for hygiene but not urgent)
- Did NOT install the backup system (requires user choice on Tier 3 storage)

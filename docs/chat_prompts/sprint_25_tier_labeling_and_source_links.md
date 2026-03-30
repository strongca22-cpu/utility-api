# Sprint 25: Product Tier Relabeling + Source URL Propagation

## Context

The utility-api project has a `source_catalog` table with a `tier` field (`free_open | free_attributed | premium | internal_only`) that controls API distribution. Currently:

- **Duke reference data** (`duke_nieps_10state`) is labeled `free_attributed` but treated as "reference only" — deprioritized to priority 8 in `config/source_priority.yaml`. It actually has 10CCF bill estimates for 1,917 PWSIDs. There is no reason to withhold this from the free product tier.
- **EFC, state PSC, and other government bulk sources** have `tier=None` in `source_catalog`. These are the backbone of the dataset and currently unlabeled.
- **Scraped LLM rates** (`scraped_llm`) also have `tier=None`.

The `rate_best_estimate` table (the final aggregated output) has no `source_url` column. But `rate_schedules` — where scraped rates land — stores `source_url` for every record (100% populated for LLM-scraped data). This URL is the original utility rate page (PDF, website) and is essential for spot-checking.

## Tasks

### Task 1: Relabel product tiers in `source_catalog`

**Current state of `source_catalog.tier`:**
| source_key | current tier | current behavior |
|---|---|---|
| `duke_nieps_10state` | `free_attributed` | Deprioritized (priority 8), but has 10CCF bills |
| `efc_*` (all states) | `None` | Government bulk, highest trust |
| `swrcb_ear_*` | `None` | CA government |
| `scraped_llm` | `None` | LLM-scraped from utility websites |
| `owrs` | `None` | Curated survey |
| `wv_psc_2026`, `nm_nmed_*`, `in_iurc_*`, `ky_psc_*`, `tx_tml_*` | Not in catalog yet | State government/survey sources |

**Changes needed:**

1. **Duke → `free`** (not "reference only"). It has 10CCF rates. It should be in the free product tier and provide rate data like any other source. Keep it at priority 8 in `source_priority.yaml` (other sources still beat it when they exist), but remove any "reference only" / "internal only" framing. The data is usable.

2. **Government/EFC/state sources → `bulk`**. Nearly all rate data ultimately comes from government sources (state PSCs, EFC surveys, SWRCB eAR, NMED, IURC, etc.). The tier label should be `bulk` — not "free_tier" or "government" — because that's what they are: bulk-ingested datasets. Update the tier enum to include `bulk` if needed.

3. **Scraped LLM → keep separate tier label** (e.g., `scraped` or `automated`). These are algorithmically discovered and parsed, which is a different trust/provenance profile than bulk government ingestion. Decide on the right label.

4. **Update the `tier` enum** in `source_catalog` model if needed. Current valid values are `free_open | free_attributed | premium | internal_only`. The new scheme needs at minimum: `free`, `bulk`, and whatever label applies to scraped data.

5. **Backfill tiers** for all sources currently showing `tier=None` and for state sources not yet in `source_catalog`.

**Key files:**
- `src/utility_api/models/source_catalog.py` — tier enum/column definition
- `config/source_priority.yaml` — priority ordering (no changes needed, but verify consistency)
- `src/utility_api/ingest/duke_reference_ingest.py` — remove "REFERENCE ONLY" framing
- Any migration needed for enum changes

### Task 2: Propagate `source_url` to `rate_best_estimate`

**Current state:**
- `rate_schedules` has `source_url` (text) — 100% populated for `scraped_llm` records
- `rate_best_estimate` has NO `source_url` column
- When BestEstimate builder picks a winning source, it records `selected_source` (e.g., "scraped_llm") and `selection_notes`, but does not carry the URL

**Changes needed:**

1. **Add `source_url` column** to `rate_best_estimate` table (Alembic migration).

2. **Update `BestEstimateAgent`** to propagate `source_url` from the winning `rate_schedules` row when building the best estimate. If the winning source is from a bulk/government table (not `rate_schedules`), `source_url` can be NULL — that's fine, those are institutional sources that don't have per-utility URLs.

3. **Backfill existing records** — run the BestEstimate builder for states with scraped data to populate `source_url` on existing `rate_best_estimate` rows.

**Key files:**
- `src/utility_api/ops/best_estimate.py` — BestEstimateAgent, the selection logic
- `src/utility_api/models/` — rate_best_estimate model (if ORM-mapped)
- Alembic migration for the new column

**Why this matters:** The source URL enables a spot-check workflow on the dashboard — a reviewer can click through to the original utility rate page and verify the parsed rate is correct. Without it, QA requires manually searching for each utility's rate schedule.

## What NOT to Do

1. **Do not change `source_priority.yaml` rankings** — Duke stays at priority 8, government stays at 1-2. This task is about labeling, not reordering.
2. **Do not rebuild `rate_best_estimate`** for all states — only backfill `source_url` for states that have scraped data.
3. **Do not delete or rename existing `source_catalog` rows** — update in place.
4. **Do not modify the parse pipeline or discovery logic** — this is a metadata/schema task only.

## Key Files

- `src/utility_api/models/source_catalog.py` — tier column definition
- `src/utility_api/ops/best_estimate.py` — BestEstimateAgent
- `src/utility_api/ingest/duke_reference_ingest.py` — Duke "reference only" framing
- `config/source_priority.yaml` — verify consistency (read-only)
- `migrations/` — Alembic migrations for schema changes

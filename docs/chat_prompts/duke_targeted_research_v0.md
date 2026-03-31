# Duke-Sourced Targeted Research Batch — v0

## Context

Sprint 26 (2026-03-31) built the targeted research pipeline (`scripts/run_targeted_research.py`) and completed the water_rates → rate_schedules sync (Phases 1-2). The best_estimate hierarchy is now correct. Scenario A batch (4,540 gap PWSIDs) has been processed.

**This chat:** Run the 11 Duke-sourced PWSIDs (~5.3M combined population) through the cascade parse pipeline to replace Duke (priority 8) with scraped_llm (priority 3) data.

## Why These PWSIDs Matter

These are the 11 highest-population PWSIDs whose best_estimate currently uses `duke_nieps_10state` — the lowest-priority source. Duke provides only a single aggregate bill amount with no rate structure, tiers, or fixed charges. Replacing with scraped_llm gives:
- Actual tier structure (increasing_block, uniform, etc.)
- Fixed charges separated from volumetric
- Rate page URL for spot-checking
- Higher confidence in best_estimate

Combined population: ~5.3M. These alone would significantly improve data quality for the 10-state Duke coverage area.

## Target PWSIDs

| PWSID | Utility | Pop | State | Notes |
|---|---|---|---|---|
| TX1010013 | CITY OF HOUSTON | 2,202,531 | TX | Rates at houstonwaterbills.houstontx.gov |
| TX2200012 | CITY OF FORT WORTH | 955,900 | TX | |
| CA3010092 | IRVINE RANCH WATER DISTRICT | 444,800 | CA | **Wrong URLs in registry** — has Newport Beach, OCSAN, Yorba Linda. Needs --force re-discovery |
| CT0930011 | REGIONAL WATER AUTHORITY | 418,900 | CT | Now has EFC as best_estimate (hierarchy fix). Scraped_llm would still be an upgrade (priority 3 > 2 only with anchor agreement) |
| KS2017308 | WICHITA, CITY OF | 395,699 | KS | |
| TX0430007 | CITY OF PLANO | 288,800 | TX | Has TML at same priority as Duke |
| TX1520002 | LUBBOCK PUBLIC WATER SYSTEM | 275,041 | TX | Has TML at same priority as Duke |
| TX0570050 | CITY OF IRVING | 264,546 | TX | Has TML at same priority as Duke |
| NJ0906001 | JERSEY CITY MUA | 262,000 | NJ | URLs point to NJ American Water tariff, not Jersey City MUA |
| TX2400001 | CITY OF LAREDO | 260,046 | TX | URL rank 1 is pending_retry |
| TX0570010 | CITY OF GARLAND | 248,822 | TX | |

All 11 already have URLs in `scrape_registry` from prior discovery runs.

## Pre-flight Status (from Sprint 26 audit)

**scrape_registry state:**
- All 11 have URLs, but some are wrong or low-quality:
  - CA3010092: all 3 URLs are for wrong utilities
  - NJ0906001: rank 1 is NJ American Water (wrong utility)
  - TX2400001: rank 1 is pending_retry
  - Several have `rank=None, score=None` URLs from legacy metro research (pre-Serper)

**rate_schedules state:**
- All 11 have `duke_nieps_10state` entries
- TX1010013, TX0430007, TX1520002, TX0570050 also have `tx_tml_2023` entries (same priority 8)
- CT0930011 also has `efc_ct_2018` (priority 2, now the best_estimate winner)

## Tasks

### Task 1: Process-Only Pass

Run the existing URLs through cascade parse:

```bash
python scripts/run_targeted_research.py --batch top25_duke_sourced --process-only
```

This skips discovery and runs `process_pwsid()` on existing scrape_registry URLs. Expected outcomes:
- TX utilities with good URLs should parse successfully
- CA3010092, NJ0906001 will likely fail (wrong URLs)
- TX2400001 may fail (pending_retry URL)

### Task 2: Force Re-Discovery for Failures

For PWSIDs that failed Task 1:

```bash
python scripts/run_targeted_research.py --pwsids CA3010092 NJ0906001 TX2400001 --force
```

`--force` re-runs Serper discovery even though URLs exist, then processes the new URLs. The CA service area fix in DiscoveryAgent should now find the correct Irvine Ranch rate page.

### Task 3: Review Results

Check which PWSIDs now have scraped_llm as best_estimate:

```sql
SELECT pwsid, selected_source, bill_10ccf
FROM utility.rate_best_estimate
WHERE pwsid IN ('TX1010013','TX2200012','CA3010092','CT0930011','KS2017308',
                'TX0430007','TX1520002','TX0570050','NJ0906001','TX2400001','TX0570010')
ORDER BY pwsid;
```

For any remaining Duke-only PWSIDs, inspect the discovery_diagnostics to understand why parsing failed, and decide whether manual URL curation is needed.

### Task 4: Gap-Sourced Batch (if Scenario A didn't cover them)

After the Duke batch, check which of the 14 gap-sourced PWSIDs in `config/targeted_research.yaml` still need processing:

```bash
python scripts/run_targeted_research.py --batch top25_gap_sourced --dry-run
```

Many of these may now be covered by Scenario A results. Only run the ones that still show Duke or no data.

## Key Files

- `scripts/run_targeted_research.py` — orchestrator
- `config/targeted_research.yaml` — PWSID batch definitions
- `src/utility_api/pipeline/process.py` — process_pwsid() cascade
- `src/utility_api/agents/discovery.py` — DiscoveryAgent (Serper)

## What NOT to Do

- Do not run full metro scans — this is targeted, not area-wide
- Do not manually curate URLs unless cascade parse + re-discovery both fail
- Do not re-run PWSIDs that Scenario A already covered with scraped_llm

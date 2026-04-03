# CO Deep Dive — Locality Discovery + Gap Audit v0

## Context

Sprint 28 built a locality discovery pipeline (municipality-name fallback search) and piloted it on NY, recovering 6 new rates from rank 1 alone with more in the chain. CO is the #1 state gap: 30 PWSIDs, 1.4M pop, 83.8% coverage. Major Front Range cities (Aurora 487k, Fort Collins 180k, Broomfield 106k, Highlands Ranch 103k, Loveland 95k) are in the gap despite having scraped text in the registry.

Denver metro is listed as `pending` in `config/metro_targets.yaml`. The locality discovery agent (`src/utility_api/agents/locality_discovery.py`) and batch runner (`scripts/run_locality_discovery.py`) are committed and tested. Sprint 29 added CO-specific handling: "X CITY OF" western SDWIS convention, MD/WWWA suffixes, FT/MT/ST abbreviation expansion, state mismatch penalty (-40 for wrong-state .gov), and full state names in queries.

## Objective

1. **Audit CO gap** — For all 30 gap PWSIDs (pop >= 3k, no `rate_best_estimate`):
   - What's in `scrape_registry`? How many URLs per PWSID, what sources, what parse results?
   - Which have substantive scraped text (>2k chars) that was never parsed or failed parsing?
   - Which are private/IOU (Aqua, EPCOR, Denver Water wholesale customers) vs municipal?
   - What URLs did standard discovery find? Why did they fail?

2. **Classify the gap** — Categorize each PWSID into:
   - **Unparsed text**: has scraped text, never sent to parser → submit batch
   - **Parse failure**: was parsed, failed → check if bug-fix rescrape applies or needs locality discovery
   - **Wrong URLs**: standard discovery found the wrong pages → locality discovery candidate
   - **Private/IOU**: needs PSC/PUC tariff lookup, not web scraping
   - **Wholesale**: served by Denver Water or Aurora Water wholesale → rate is the wholesaler's

3. **Execute** — Based on the audit:
   - Run `scripts/run_locality_discovery.py --state CO --dry-run` first
   - Review municipality extractions and queries
   - Fix any extraction issues (CO has districts like "HIGHLANDS RANCH WSD", "CASTLE ROCK WSD")
   - Run live locality discovery
   - Scrape iteratively by rank (rank 1 first, then 2, etc.)
   - Submit parse batch
   - Run bug-fix rescrape audit (same 4 Sprint 27 bugs) on CO prior scrape attempts
   - Queue rescrape if warranted

4. **Stop and report** before executing parse batches — present findings for review.

## Key Files
- `src/utility_api/agents/locality_discovery.py` — LocalityDiscoveryAgent, extract_municipality(), build_locality_queries()
- `scripts/run_locality_discovery.py` — batch runner with --dry-run
- `scripts/rescrape_bugfix_ny.py` — template for bug-fix rescrape (adapt for CO)
- `config/metro_targets.yaml` — Denver metro entry (status: pending)
- `src/utility_api/agents/discovery.py` — standard DiscoveryAgent (Sprint 29: state mismatch penalty, full state names, "X CITY OF" pattern)

## Notes
- The NY chain (`tmux session ny_chain`) may still be running — check `logs/chain_ny_locality.log` for status before starting CO work
- Denver Water is the dominant wholesale provider on the Front Range — some gap PWSIDs may purchase from Denver Water and resell. Their rate page is `denverwater.org/your-water/water-rates`
- CO PUC regulates private water utilities — tariff filings at `puc.colorado.gov`
- Sprint 27 scraper bug fixes are live — all new scrapes use fixed code
- Sprint 29 changes are live: state mismatch penalty, full state names, CO suffixes (MD, WWWA), abbreviation expansion (FT→Fort, MT→Mount), "X CITY OF" western convention
- Commit and update `docs/next_steps.md` + session summary when done

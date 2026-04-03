# NV Targeted Investigation — 5 Gap PWSIDs, 730k Pop v0

## Context

Sprint 28 coverage stands at 94.3% systems / 96.7% pop (Lower 48, pop >= 3k). NV has only 5 gap PWSIDs but 730k population — the highest pop-per-PWSID gap in the country. NV overall is at 86.5% coverage (32/37 systems covered). The gap is dominated by two Las Vegas metro utilities: North Las Vegas (377k) and Henderson (337k).

Las Vegas metro was processed through the standard metro scan pipeline (`config/metro_targets.yaml`, status: `complete`) and the standard DiscoveryAgent, but these 5 PWSIDs didn't get rates. All 5 have scraped text in `scrape_registry` — the URLs were found, the pages were scraped, but parsing failed or the wrong pages were fetched.

The locality discovery pipeline (`src/utility_api/agents/locality_discovery.py`) is committed and tested on NY (70/76 extraction rate, 6 new rates from rank 1 alone). CO deep dive is in a separate chat. Sprint 29 added CO-specific suffixes, state mismatch penalty scoring, full state name in queries, and "X CITY OF" western SDWIS convention handling — all committed and live.

## Why NV Is Different

The Las Vegas Valley water system is structurally unusual:
- **SNWA** (Southern Nevada Water Authority) is the regional wholesaler — manages Lake Mead intake, treats, and sells to member agencies
- **LVVWD** (Las Vegas Valley Water District) is the retail arm for unincorporated Clark County — also operates SNWA infrastructure
- **Henderson**, **North Las Vegas**, and **Boulder City** buy wholesale from SNWA but set their own retail rates
- Retail rate pages may be on `cityofhenderson.com`, `cityofnorthlasvegas.com`, etc. — NOT on `snwa.com` or `lvvwd.com`
- NV PUC does NOT regulate municipal utilities — Henderson/NLV set rates via city council ordinance

This means standard PWSID-name discovery may have returned SNWA/LVVWD pages instead of the actual retail rate pages for Henderson and North Las Vegas.

## Objective

1. **Audit all 5 NV gap PWSIDs** — For each one:
   - What's in `scrape_registry`? What URLs were found, from what sources?
   - What scraped text exists? Was it parsed? What was the parse result?
   - Is the scraped text from the RIGHT utility (not SNWA/LVVWD wholesale rates)?
   - What is the actual retail rate page URL? (May need manual identification)

2. **Classify each PWSID**:
   - **Wrong URL**: standard discovery found SNWA/LVVWD instead of the retail utility → locality discovery or manual curation
   - **Right URL, parse failure**: page was correct but parser couldn't extract rates → check if bug-fix rescrape helps, or if the page format needs special handling
   - **Private/regulated**: if any are NV PUC regulated → need tariff lookup at `pucweb1.state.nv.us`
   - **Wholesale-only**: if the PWSID IS the wholesaler (SNWA/LVVWD itself) → rate page is different

3. **Execute** — Based on the audit:
   - Run `scripts/run_locality_discovery.py --state NV --dry-run` to preview municipality extractions
   - For any PWSIDs where locality discovery won't help (e.g., already have the right URL but parse failed), consider manual URL curation
   - Run live discovery, scrape, and submit parse batch
   - For the 2 big PWSIDs (Henderson 337k, North Las Vegas 377k), verify the rate page manually if automated approaches fail — these two alone are 710k pop

4. **Stop and report** before executing parse batches — present findings for review.

## Key Files
- `src/utility_api/agents/locality_discovery.py` — LocalityDiscoveryAgent
- `scripts/run_locality_discovery.py` — batch runner with --dry-run
- `config/metro_targets.yaml` — Las Vegas metro entry (status: complete, notes: "LVVWD and SNWA are dominant utilities — not regulated by NV PUC")
- `src/utility_api/agents/discovery.py` — standard DiscoveryAgent (Sprint 29: state mismatch penalty, full state names)

## Notes
- Check `logs/chain_ny_locality.log` for NY chain status before starting
- Check TC pipeline status — NV gap PWSIDs may have unparsed text from TC discovery that's about to be batched
- Only 5 PWSIDs — this is a surgical investigation, not a bulk pipeline run. Manual curation is justified for 730k pop.
- Henderson water rates are likely at `cityofhenderson.com` under utilities/public works
- North Las Vegas water rates are likely at `cityofnorthlasvegas.com` under utilities
- Sprint 29 changes are live: state mismatch penalty (-40 for wrong-state .gov), full state names in queries, "X CITY OF" western convention
- Commit and update `docs/next_steps.md` + session summary when done

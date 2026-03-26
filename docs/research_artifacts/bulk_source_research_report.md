# Bulk Water Rate Data Sources: Research Report

**Date:** 2026-03-25
**Scope:** Systematic search for downloadable water rate datasets across all 50 US states

---

## Executive Summary

The single most important finding is that the **UNC Environmental Finance Center (EFC) has water rate dashboards covering 24+ US states**, and their website explicitly states that "data displayed on the dashboard are available for download from links provided on the dashboard webpages." This represents a potential **5,000–7,000+ utility** dataset that can be accessed for free.

Combined with existing ingested sources (CA eAR, CA OWRS, NC EFC CSV), this could bring total bulk-sourced coverage from ~818 PWSIDs to an estimated **4,000–8,000 PWSIDs** — covering roughly 10-18% of all 44,643 community water systems with structured rate data at near-zero LLM cost.

## Key Findings

### Finding 1: EFC Dashboard Network (24+ States, 5,000–7,000 utilities)

The EFC at UNC operates rate dashboards for the following states: Alabama, Arizona, Arkansas, California (small systems), Connecticut, Delaware, Florida, Georgia, Hawaii, Illinois (NE only), Iowa, Maine, Massachusetts, Mississippi, Missouri, New Hampshire, North Carolina, Ohio, South Carolina, West Virginia, and Wisconsin.

Each dashboard contains rate structure data (tiers, fixed charges, volumetric rates), bill calculations at multiple consumption levels, PWSID linkage (via EPA SDWIS), and financial benchmarks. The dashboards share a common platform at `dashboards.efc.sog.unc.edu/{state_abbrev}`.

A 2017 EFC blog post mentioned 4,493 communities across 13 states. A 2025 blog post references a national affordability dashboard with "over 7,000 utilities." The EFC has clearly expanded significantly since 2017.

**Immediate action:** Visit each state dashboard URL and locate the download mechanism. The EFC may use a common download endpoint across all dashboards, or each may have a state-specific CSV/Excel link. If the dashboards use a common API, a single scraper could pull all states.

### Finding 2: Duke/Nicholas Institute Water Affordability Dashboard (2,349 utilities, 7 states)

Duke University's Nicholas Institute developed a separate water affordability dashboard covering 2,349 utilities across 7 states (NC, NJ, NM, GA, AZ, CA, WI). This includes detailed rate data plus service area boundary polygons, making it uniquely valuable for geospatial joins. The dashboard was published around 2021-2022.

**Status:** Need to check if data is downloadable. The Internet of Water blog post describes the methodology but doesn't link to a bulk download.

### Finding 3: Texas Municipal League Survey (168–237 cities, annual)

The TML publishes annual water and wastewater rate survey results covering 168–237 Texas cities. Results are available for 2019–2025. Data includes bill amounts at standard consumption levels, broken out by population group. No PWSIDs — would require city name matching. This covers a small fraction of Texas's 4,584 CWS, but captures the largest municipal systems.

### Finding 4: Wisconsin PSC — Uniquely Comprehensive State Regulation

Wisconsin's PSC regulates 96% of the population served by community water systems, including municipally-owned systems (unusual — most states only regulate IOUs). The PSC maintains a tariff database and annual report database covering ~577 utilities. This data feeds the EFC WI dashboard and the PSC's own quarterly bill comparison tool. Wisconsin is the most comprehensively regulated state for water rates in the US.

### Finding 5: AWWA/Raftelis Rate Survey (500 utilities, paywall)

The AWWA/Raftelis survey covers ~500 utilities nationally, recently redesigned with 6-month updates and a new digital subscription platform. This is behind a paywall and lacks PWSIDs. Lower priority than free EFC sources.

### Finding 6: States With No Bulk Source Found

The following high-priority states have no identified bulk rate data source: Texas (beyond TML's ~237 cities), Pennsylvania, New York, New Jersey, Indiana, Colorado, Minnesota, Michigan, and Washington.

These states collectively account for approximately 15,000 CWS (33% of total) and will require per-utility scraping via the domain guesser + LLM pipeline.

---

## Priority Ingest List (Top 10)

Ordered by (estimated utility count × data quality × ease of ingest):

| Rank | Source | State(s) | Est. Utilities | Ingest Difficulty | Notes |
|------|--------|----------|---------------|-------------------|-------|
| 1 | EFC WI Dashboard | WI | 577 | Easy | PSC data, explicitly downloadable |
| 2 | EFC GA Dashboard | GA | 450 | Moderate | Annual since 2007 |
| 3 | EFC IA Dashboard | IA | 690 | Moderate | Previously investigated |
| 4 | EFC OH Dashboard | OH | 400+ | Moderate | Large unserved state |
| 5 | EFC FL Dashboard | FL | 300+ | Moderate | Raftelis-funded |
| 6 | EFC CA Small Systems | CA | 200+ | Moderate | Complements existing CA data |
| 7 | EFC AL Dashboard | AL | 300 | Moderate | |
| 8 | EFC AZ Dashboard | AZ | 200 | Moderate | |
| 9 | TX TML Survey | TX | 237 | Moderate | No PWSIDs |
| 10 | EFC IL (NE) Dashboard | IL | 200 | Moderate | NE Illinois only |

**Recommended approach:** Build a generic EFC dashboard ingest module first, since all EFC dashboards share a common platform. Test on WI (easiest, PSC-backed data), then apply to all 20+ states. This single engineering investment could yield 4,000–5,000 new PWSIDs.

---

## Search Methodology

For each state, searched the following query patterns:
- `"{state}" water rate survey download csv`
- `"environmental finance center" "{state}" water rates`
- `site:efc.sog.unc.edu {state}`
- `"{state}" public utility commission water rates database`
- `"{state}" water rate comparison study data`

Also checked: state PUC/PSC websites, state environmental agency pages, state comptroller/treasurer data portals, and state rural water association resources.

## Coverage Summary

| Category | States | Est. Utilities | Status |
|----------|--------|---------------|--------|
| Already ingested | CA, NC | ~818 | Done |
| EFC dashboards (not yet ingested) | 20+ states | ~4,000-5,000 | Ready to ingest |
| Duke affordability dashboard | 7 states | ~2,349 | Need to check download |
| TX TML survey | TX | 237 | Ready to ingest |
| WI PSC tools | WI | 577 | Ready (overlaps with EFC) |
| AWWA/Raftelis | National | 500 | Paywall |
| **Total addressable** | | **~6,000-8,000** | |
| No bulk source found | TX(remainder), PA, NY, NJ, IN, CO, MN, MI, WA, VA | ~15,000 | Per-utility scraping needed |

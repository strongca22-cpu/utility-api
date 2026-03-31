# UAPI Research Session — Gap State PUC/PSC and Municipal League Rate Sources

---

## ⬇️ RESEARCH FOCUS (MODIFY THIS SECTION ONLY) ⬇️

### Direction

The academic research path has been exhausted — every significant US water rate dataset uses CC BY-NC-ND or is paywalled. The EFC dashboard network covers 19 states. The remaining ~18 gap states have zero commercial bulk coverage. The most promising untapped source category is **state-level government agencies and industry associations** that publish rate data as part of their regulatory or member-service missions.

The Wisconsin PSC proved the model: a state PUC that regulates nearly all water utilities and publishes a quarterly bill comparison tool. The Texas Municipal League proved the other model: a state association that surveys member cities annually and publishes results freely. This session systematically checks whether equivalents exist for the largest gap states.

### Targets (Top 10 Gap States by CWS Count)

Investigate each of these states for **state PUC/PSC rate databases, state municipal league or water association surveys, and any other state-level published rate data.** These are the states with the most CWS and zero commercial bulk coverage:

| Priority | State | CWS Count | Notes |
|----------|-------|-----------|-------|
| 1 | NY | 1,791 | Large IOU presence (SUEZ/Veolia, American Water). NY PSC regulates IOUs. |
| 2 | MI | 1,400+ | U Michigan MDWD team is here — check state sources they may have used. |
| 3 | IN | 1,100+ | IN IURC regulates IOUs. Indiana American Water is major. |
| 4 | CO | 1,000+ | CO PUC. Many special districts. |
| 5 | MN | 900+ | MN PUC. Strong state regulatory tradition. |
| 6 | KY | 800+ | KY PSC. Kentucky American Water (AWK subsidiary). |
| 7 | TN | 700+ | TN regulatory board. Some IOU presence. |
| 8 | LA | 700+ | LA PSC. Many small rural systems. |
| 9 | OK | 700+ | OK CC (Corporation Commission). |
| 10 | MD | 500+ | MD PSC. Baltimore-area utilities. |

For each state, investigate three source categories:

**Category A: State PUC/PSC** — Does the state public utility/service commission maintain a water rate database, bill comparison tool, or tariff search? Wisconsin PSC publishes a quarterly bill comparison covering 577 utilities. Does NY PSC, IN IURC, KY PSC, etc. do anything similar? Check the PUC's water utility section for rate tools, rate comparison reports, or downloadable tariff data. Note: most PUCs only regulate IOUs (not municipals), so coverage is partial — but IOUs serve significant populations.

**Category B: State municipal league or water utility association** — Does the state have a municipal league that conducts an annual water rate survey (like TX TML)? Does it have a state section of AWWA, a state rural water association, or an independent water utility association that publishes rate surveys? Search: `"{state}" municipal league water rate survey`, `"{state}" water utility association rate survey`, `"{state}" rural water association rate survey`.

**Category C: State environmental/health agency** — Does the state DEQ, DEC, or health department collect rate data as part of drinking water program oversight, affordability assessments, or financial capacity evaluations? Some states require utilities to report financial data (including rates) as a condition of operating permits or receiving state revolving fund loans.

### Key Questions

For each source found:
1. Is the data freely downloadable (CSV, Excel, HTML table) or locked in a search interface?
2. Does it use PWSIDs or only utility names/state-specific IDs?
3. How many utilities does it cover? (Must clear 50+ PWSIDs for ingest threshold)
4. What rate data is included — full tier structures, or just bill amounts at standard consumption?
5. What's the vintage? Current or historical?
6. Is it water-only or combined water+sewer?

### Priority

Spend the most time on NY, MI, and IN — these are the three largest gap states and the most likely to have structured regulatory data. If any of those three yields a bulk source, it's worth 200-500 PWSIDs each.

Spend moderate time on KY, CO, MN — mid-size states with active PUCs.

Spend less time on TN, LA, OK, MD — check the obvious sources (PUC website, municipal league website) but don't dig deep if nothing surfaces quickly.

If any state has a municipal league survey similar to TML, that's an immediate high-value find — it's the proven pattern for fast ingest with city-name matching.

## ⬆️ END RESEARCH FOCUS ⬆️

---

## Project Context (Immutable)

### What We're Building

UAPI is a geospatial water utility rate API. It maps water rates to EPA Community Water System (CWS) service area polygons across the United States. The core product is a REST API + bulk download that returns water rate data for any US location.

**Current coverage:** ~7,000+ PWSIDs across 30+ states (updates frequently — check the "Existing Sources" section below for the latest). 44,643 total CWS exist in the US. Population-weighted coverage is ~45%.

**Tech stack:** PostgreSQL + PostGIS, Python pipeline, Claude API for LLM parsing of utility websites. CLI tool `ua-ops` and `ua-ingest` for operations. Data lives in two tables: `rate_schedules` (canonical, JSONB tier structures) and `water_rates` (legacy flat bill amounts, being deprecated). A `best_estimate` layer unions both tables.

**The identifier:** EPA PWSID (Public Water System ID) is the canonical identifier. Format: two-letter state code + 7 digits (e.g., `TX1010001`). Every record in our database keys on PWSID. Sources that include PWSIDs are dramatically easier to ingest than sources requiring name matching.

### Source Provenance & Tier System

Every data source gets assigned a distribution tier that determines how the data can be used commercially:

| Tier | Description | Commercial Redistribution | API Access |
|------|-------------|--------------------------|------------|
| `free_open` | Public domain, no restrictions | Yes | Public, no auth |
| `free_attributed` | Free with attribution required | Yes (with attribution) | Public, no auth |
| `premium` | Commercially redistributable | Yes | Authenticated, subscription |
| `internal_only` | Reference/validation only, never redistributed | No | None — internal use |

**Tier assignment rules:**
- US government data (federal or state) → `premium` or `free_open`
- CC BY 4.0 → `free_attributed`
- CC BY-NC, CC BY-NC-ND, CC BY-NC-SA → `internal_only`
- Public reports from non-profits with no stated license → `premium` (flag for legal review)
- Behind paywall → `internal_only`
- Unknown license → `internal_only` (conservative default)

### What Makes a Source Worth Ingesting

A source clears the threshold for a Claude Code ingest session if it has:

1. **50+ incremental PWSIDs** — after dedup against existing data
2. **Structured data** — CSV, Excel, API, or clean HTML tables. NOT unstructured PDF prose.
3. **PWSIDs or matchable identifiers** — EPA PWSIDs ideal. City/utility names acceptable (fuzzy matching works at ~90% rate). State-specific IDs acceptable if a crosswalk to EPA PWSIDs exists.
4. **Accessible** — freely downloadable or queryable without authentication, paywall, or institutional access
5. **Rate data** — actual dollar amounts (bill totals, fixed charges, volumetric rates, tier breakpoints). NOT water use volumes, NOT financial aggregates (total revenue), NOT water quality metrics.

Sources below threshold go in the skip list with a reason. Sources at borderline (30-50 PWSIDs) get documented but deprioritized.

### Existing Sources (What We Already Have)

*Last updated: 2026-03-26. Total: ~7,000+ PWSIDs, 30+ states, ~45% US population. Numbers are approximate and grow between sessions — treat as directional, not exact.*

**Commercial tier (premium):**

| Source | Key | States | ~PWSIDs | Format |
|--------|-----|--------|---------|--------|
| EFC dashboards (19 states) | `efc_{state}_{year}` | AL, AR, AZ, CA, CT, DE, FL, GA, HI, IA, IL, MA, ME, MO, MS, NC, NH, OH, SC, WI | ~5,436 | JSON API (Topsail) |
| TX Municipal League | `tx_tml_2023` | TX | ~476 | XLSX download |
| CA SWRCB eAR | `ca_swrcb_ear` | CA | ~194 | Excel via HydroShare |
| CA OWRS | `ca_owrs` | CA | ~381 | CSV/YAML |
| WV PSC | `wv_psc_2026` | WV | ~241 | HTML scrape |
| LLM-scraped (various) | `scraped_llm` | 20+ states | ~330+ | Pipeline scraping (Duke URLs, domain guesser, SearXNG, curated) |

**Reference tier (internal_only):**

| Source | Key | States | ~PWSIDs | License |
|--------|-----|--------|---------|---------|
| Duke GitHub 10-state | `duke_nieps_10state` | CA, CT, KS, NC, NJ, NM, OR, PA, TX, WA | ~5,371 | CC BY-NC-ND 4.0 |
| Duke PLOS 787 national | N/A — not available as download | All 50 | ~787 | CC BY-NC-ND 4.0. PLOS SI is anonymized (no PWSIDs). Dashboard-only. |

**States with ZERO or near-zero commercial bulk coverage (highest value for new sources):**
NY, MI, IN, MN, KY, TN, LA, OK, MD, NE, NV, SD, ND, WY, MT, VT, RI, DC

(Some of these have a handful of PWSIDs from the domain guesser or Duke URL scraping but no bulk source. A new bulk source for any of these states is high value.)

### Confirmed Dead Ends (Cumulative Skip List)

Do NOT re-investigate these. They've been thoroughly checked. This list grows across sessions — new entries are added by each research session's output.

*Last updated: 2026-03-26*

- **Zenodo DOI 10.5281/zenodo.5156654** — Verified mirror of Duke GitHub repo. GitHub has 5,371 PWSIDs; Zenodo lags at 3,038. No additional data.
- **Duke PLOS 787 supplementary data** — Verified anonymized. S1 Data contains no PWSIDs, no utility names — only computed affordability metrics for reproducing paper figures. The un-anonymized 787-community data exists only as the dashboard backend, not as a bulk download. Contact Lauren Patterson (lauren.patterson@duke.edu) for access.
- **Internet of Water** — Coordination initiative, not a data repository.
- **USGS water use data** — Water USE volumes, not rates.
- **EPA SDWIS** — System metadata only, no rate data.
- **AWWA/Raftelis rate survey** — Paywalled. ~500 utilities. Not accessible.
- **UNC EFC published papers (Brown et al. 2023)** — Bill data for 1,720 systems in AZ, GA, NH, WI — all states already covered by EFC dashboards. No incremental data.
- **Teodoro affordability papers** — Methodology, not data.
- **Brookings water reports** — Policy analysis, no original rate data.
- **US Water Alliance** — Advocacy org, no original rate data.
- **Texas Comptroller** — Aggregate financial data, not rate schedules.
- **TCEQ Drinking Water Watch** — System metadata, no rates.
- **Bardot et al. 2025 (PLOS Water)** — Water quality disparities, not rates.

---

## Output Requirements (Immutable)

### Required Output Files

Every research session produces exactly these files. File names should be descriptive of the topic — the Claude Code session receiving them will handle placement in the repo.

#### 1. Research Report (markdown)

Narrative findings report. For each source investigated:
- What was found (or not found)
- Exact access method (URL, download link, API endpoint)
- What the data contains (fields, record count, states, PWSIDs)
- Overlap with existing data (reference the "Existing Sources" table above)
- License and tier assignment with rationale
- Incremental value assessment (number of new PWSIDs in states we don't cover)
- Recommendation: `ingest` / `reference_only` / `defer` / `skip`

#### 2. Ingest Specs (YAML)

For EVERY source that clears the 50+ incremental PWSID threshold, a structured spec that a Claude Code session can implement from directly:

```yaml
- source_key: "descriptive_key"
  name: "Full Name of Source"
  tier: "premium | free_attributed | internal_only"
  commercial_use: true | false
  license: "exact license name or 'none stated'"
  
  # Access
  url: "exact URL to data or download page"
  download_url: "exact direct download link (if different from url)"
  format: "csv | xlsx | json_api | html_table | pdf_table"
  authentication: "none | api_key | institutional"
  
  # Scope
  states_covered: [XX, YY, ZZ]
  estimated_total_records: NNN
  estimated_incremental_pwsids: NNN  # after dedup against existing sources
  has_pwsid: true | false
  pwsid_format: "SS1234567"  # if applicable
  matching_strategy: "direct_pwsid | city_name_fuzzy | utility_name_fuzzy"
  data_vintage: "YYYY or YYYY-YYYY"
  
  # Schema — exact column names or JSON keys
  schema:
    columns:
      - name: "exact_column_name"
        maps_to: "our_field_name"
        type: "string | float | integer | date"
        description: "What this field contains"
        sample_value: "example"
      # ... all relevant columns
  
  # At least one complete sample record
  sample_records:
    - field1: "value1"
      field2: "value2"
      # ... every field
  
  # Implementation guidance
  ingest_approach: |
    Step-by-step description of how to build the ingest module.
  similar_to: "name of closest existing ingest module in the codebase"
  key_differences:
    - "difference from the template module"
  estimated_effort: "N hours"
  data_quality_notes:
    - "known issues, missing values, outliers, dedup needs"
```

**The spec must be implementation-ready.** Test: can a Claude Code session read this file and produce a working ingest module in under 4 hours without searching the web? If the answer is no, the spec is incomplete.

#### 3. Updates to Skip List (YAML)

For every source investigated and found NOT worth ingesting. This list is **cumulative across sessions** — append new entries, never remove old ones:

```yaml
- name: "Source Name"
  url: "https://..."
  investigated: "YYYY-MM-DD"
  reason: "One-line reason for skipping"
  revisit: "never | if_{condition}_changes"
```

### What the Research Session Must NOT Do

1. **Do not fabricate data schemas.** If you cannot see the actual data (column names, sample values), say "schema not accessible — requires interactive investigation." Do not guess.
2. **Do not fabricate URLs.** If a download link is not confirmed working, say "download URL not confirmed." Do not construct plausible URLs.
3. **Do not fabricate record counts.** If a count is estimated, say "estimated ~NNN based on [methodology]." If unknown, say "unknown."
4. **Do not re-investigate skip list sources.** They are confirmed dead ends.
5. **Do not recommend ingesting sources below the 50-PWSID threshold** unless they cover a zero-coverage state with no other path to data.
6. **Do not recommend purchasing paywalled datasets** without explicitly flagging the cost and commercial viability.
7. **Do not produce ingest specs for sources that are `internal_only`** unless the reference/validation value justifies the Claude Code session time. If it does, explain why.
8. **Do not modify any framing about what the product is or how the pipeline works.** This context is accurate and current.
9. **Do not follow citation chains more than 2 levels deep.** The goal is finding datasets, not mapping academic literature.
10. **Do not spend more than 30 minutes investigating any single source that isn't panning out.** Note what you found and move on.

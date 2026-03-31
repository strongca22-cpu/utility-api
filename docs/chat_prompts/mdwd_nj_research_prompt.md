# Research Task: MDWD Harvard Dataverse + Jersey Water Works NJ Study

## What You're Doing

Download and assess two academic water rate data sources. For each: document the schema, assess the license, count PWSIDs, measure overlap with our existing data, and produce an ingest spec if warranted. Update the attached `academic_source_skip_list.yaml` with findings.

## Source 1: MDWD (Municipal Drinking Water Database)

**Harvard Dataverse:** `https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DFB6NG`

**What it is:** A database of 2,000+ municipally-owned community water systems by Sara Hughes (U Michigan). Contains PWSID, municipality name, population, financial metrics (revenue/capita, expenditures, debt ratios), SDWA violations, demographics, political data. Published April 2023.

**The base MDWD likely does NOT contain rate data.** It's institutional/financial metadata. But the PWSID-to-municipality matching alone is valuable for our name-matching work (e.g., matching TX TML city names to PWSIDs).

**A January 2025 extension exists.** Hughes et al. published "Understanding the Cost of Basic Drinking Water Services in the United States" in AWWA Water Science (DOI: 10.1002/aws2.70014). This added water cost data for 2,161 municipalities — monthly bill at 6,000 gal computed from fixed charges and volumetric rates collected Nov 2021–May 2022. The rate data may be in the paper's supplementary files or in an updated Dataverse deposit.

**Do this:**

1. Download all files from the Harvard Dataverse page. List every file with name, format, size.
2. Open the primary data file(s). Document the complete column schema (every column name, type, sample values).
3. Count unique PWSIDs and states represented.
4. Confirm whether the base has ANY rate/bill fields or is purely institutional.
5. Check the Dataverse license/terms of use — is it CC BY? CC0? Custom?
6. Search for the Hughes 2025 rate extension:
   - Check the Dataverse page for version history or updated files added in 2024/2025
   - Check the paper at `https://awwa.onlinelibrary.wiley.com/doi/full/10.1002/aws2.70014` for supplementary data links
   - Search: `Hughes "drinking water" cost 2161 municipalities dataset download`
   - Even if the paper text is paywalled, SI files on Wiley are sometimes open
7. If rate data is found, document: fields included, consumption level for bill computation, whether tier breakpoints are present or just final bills, geographic scope, exact license.

**Produce:**
- `config/mdwd_ingest_spec.yaml` with two sections:
  - Spec A: MDWD base (matching metadata) — likely `free_open` or check Dataverse terms
  - Spec B: MDWD + Hughes rates (if found) — likely `internal_only` (CC BY-NC-ND)
- Include: download URLs, file names, complete column schema with types and samples, PWSID format, state count, data vintage, license, tier assignment.

## Source 2: Jersey Water Works / Van Abs NJ Study

**What it is:** Daniel Van Abs (Rutgers) conducted a three-phase NJ water affordability study for Jersey Water Works (a non-profit collaborative). Phase 3 (2021) collected rate schedules for ~266 drinking water utilities covering >90% of NJ households with CWS. Rate data is in PDF appendix tables.

**Why it matters:** NJ has 800+ CWS. EFC covers 0. Duke GitHub covers 327. This study covers ~266 — most overlap Duke but some are incremental. The value is uncertain until we see the actual tables.

**Do this:**

1. Find and download the Phase 3 report PDF:
   - Try: `https://www.jerseywaterworks.org/resources/` and search for "affordability methodology"
   - Try: `https://cms.jerseywaterworks.org/wp-content/uploads/` with various path guesses
   - Search: `Van Abs 2021 "New Jersey Affordability Methodology" PDF`
   - The Phase 1 PDF URL pattern was: `cms.jerseywaterworks.org/wp-content/uploads/2018/09/Van-Abs-and-Evans-2018.09.26-Phase-1-...`
2. In the PDF, find Table 15 (Drinking Water Utility Rates and Estimated Household Costs, starts around p.63).
3. Document: how many utilities listed, what columns exist, whether tables are cleanly extractable or messy (merged cells, footnotes, irregular formatting), how many pages they span.
4. Extract 5-10 sample rows verbatim to demonstrate the table structure.
5. Check the AWWA Water Science companion paper (DOI: 10.1002/aws2.1287) for supplementary structured data — that would be far easier than PDF extraction.
6. Search for any structured (CSV/Excel) version:
   - `Van Abs New Jersey water rates dataset download`
   - Check Rutgers institutional repository: `rucore.libraries.rutgers.edu`
   - Check if NJDEP's 2022 reporting requirement produced a public dataset: search `NJDEP water utility cost reporting dataset`
7. Assess the license: look for any explicit license statement in the PDF, on the JWW website, or in the report's front matter. A public non-profit report with no stated license is likely usable with attribution.

**Produce:**
- `config/nj_jwe_ingest_spec.yaml` (if data is extractable) with: PDF URL, table page numbers, column structure, sample rows, extraction approach (pymupdf/tabula/camelot), name-matching strategy for NJ PWSIDs, license assessment, tier assignment, estimated effort.
- If the data is too messy or the incremental value too low (~50 PWSIDs over Duke), say so clearly and add to the skip list instead.

## Existing Data for Dedup Reference

States with commercial coverage: AL, AR, AZ, CA, CT, DE, FL, GA, HI, IA, IL, MA, ME, MO, MS, NC, NH, NJ (Duke only, 327 PWSIDs), OH, SC, WI, TX, WV, KS, NM, OR, PA, WA

States with ZERO coverage: NY, MI, IN, CO, MN, VA, KY, TN, LA, OK, MD, NE, NV, SD, ND, WY, MT, ID, VT, RI, DC

## Tier Assignment

| License | Tier |
|---------|------|
| Public domain / CC0 | `free_open` |
| CC BY 4.0 | `free_attributed` |
| CC BY-NC-ND 4.0 | `internal_only` |
| Public report, no license stated | `premium` (flag for legal review) |
| Harvard Dataverse default | Check per-dataset depositor terms |
| Paywalled | `internal_only` |

## Output Files

1. `docs/mdwd_nj_research_report.md` — narrative findings for both sources
2. `config/mdwd_ingest_spec.yaml` — MDWD base + Hughes rate extension specs
3. `config/nj_jwe_ingest_spec.yaml` — NJ study spec (if warranted, otherwise explain skip)
4. Updated `config/academic_source_skip_list.yaml` — move investigated items from `investigate_further` to either `skipped_sources` (with findings) or leave with updated status

## Time Budget

- MDWD: ~60 minutes (download, schema docs, Hughes extension search)
- NJ: ~45 minutes (find PDF, examine tables, assess extractability)
- Zenodo: skip entirely (already verified as duplicate of GitHub repo — see skip list)
- Citation chain: ~15 minutes while investigating the above (note any new dataset leads)
- Writing specs: ~30 minutes

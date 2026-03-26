# UAPI Research Report — Remaining Gap States (Tier 2 + Small States)
**Session date:** 2026-03-26
**States investigated:** NJ, OR, VA, NE, KS, WY, MT, SD, ND, NV, ID, VT, RI, DC, LA (LRWA revisit), TN (TAUD revisit)
**Bonus find:** NM (surfaced during NJ investigation — not on target list)

---

## Executive Summary

Three ingest-ready sources found:

| Source | State | ~Utilities | Format | Tier | Action |
|--------|-------|-----------|--------|------|--------|
| NM NMED Annual Water & Sewer Rate Survey | NM | ~200 publicly-owned CWS | PDF download (annual) | `premium` | **Ingest** |
| VT EFC Rates Dashboard | VT | 182 utilities | Topsail JSON API | `premium` | **Ingest** |
| VA EFC Rates Dashboard | VA | ~136 utilities | Topsail JSON API | `premium` | **Defer** (2019 data — verify if updated) |

One source requiring interactive verification before committing to ingest:

| Source | State | Issue |
|--------|-------|-------|
| NJ Jersey WaterCheck Excel export | NJ | Login requirement for bulk export unconfirmed; PWSID inclusion unconfirmed |

All remaining states: skip.

---

## State-by-State Findings

---

### NEW MEXICO (NM) — BONUS FIND — not on target list ⭐ HIGH VALUE

Surfaced while investigating NJDEP rate data links.

**Category C: NMED Drinking Water Bureau Annual Water and Sewer Rate Survey**

The New Mexico Environment Department (NMED) Drinking Water Bureau publishes a **free annual Water and Wastewater User Charge Survey** at:

`https://www.env.nm.gov/drinking_water/rates/`

PDFs for every year back to 2012 are directly downloadable. The 2025 edition (covering December 2024 rates) is confirmed accessible at a short-URL redirect that resolves to a cloud.env.nm.gov PDF.

**Confirmed schema (2025 edition, from PDF extraction):**

| Column | Maps To | Type | Sample |
|--------|---------|------|--------|
| System Name | utility_name | string | "Artesia Municipal Water System" |
| County | county | string | "Eddy" |
| Residential 6,000 Gal. Water Rate/Month Dec. 2024 | monthly_bill_water_6kgal | float | "$10.02" |
| Commercial 6,000 Gal. Water Rate/Month | monthly_bill_water_commercial | float | "$21.52" |
| Number of Residential Water Connections | residential_connections | integer | 4,781 |
| Number of Commercial Water Connections | commercial_connections | integer | 722 |
| Residential 6,000 Gal. Sewer Rate/Month | monthly_bill_sewer_6kgal | float | "$13.48" |
| Commercial 6,000 Gal. Sewer Rate/Month | monthly_bill_sewer_commercial | float | "$13.48" |
| Number of Residential Sewer Connections | — | integer | 4,198 |
| Number of Commercial Sewer Connections | — | integer | 619 |
| July [Year] Water Production (Gal/Month) | — | integer | N/A |
| [Year] Total Annual Water Production (Gal/Year) | — | integer | N/A |
| AWWA Water Audit fields (apparent losses, real losses, etc.) | — | float | N/A |

**Sample records (from 2025 PDF):**
```
Artesia Municipal Water System | Eddy | $10.02 | $21.52 | 4781 | 722 | $13.48 | $13.48 | 4198 | 619
Bloomfield Water Supply System | San Juan | $46.32 | $46.32 | 2781 | 323 | $46.32 | $46.32 | 2102 | 225
Canada De Los Alamos MDWCA | Santa Fe | $284.00 | $284.00 | 27 | 1 | N/A | N/A
Carlsbad Municipal Water System | Eddy | $17.45 | $20.66 | 11209 | 1813 | $32.41 | $36.41 | 9879 | 1298
```

**Coverage:** The 2025 PDF spans 15 pages and covers publicly-owned community water systems throughout New Mexico. Estimated ~200 utilities. The survey description states it covers "public water systems throughout the state." Commercial entries are also listed under a separate section (not extracted here). Note: "N/A = Not Applicable or Not Reported" appears frequently for smaller systems.

**PWSIDs:** Not included. Matching strategy: utility name + county fuzzy match against NM SDWIS CWS records.

**Format:** PDF table, text-selectable (not scanned). Annually updated. 2025 edition noted "Revised 7-22-25."

**Overlap:** New Mexico is currently covered only by Duke (internal_only, ~440 NM PWSIDs). This source would add ~150-180 incremental PWSIDs commercially.

**License/Tier:** NMED state government publication, no stated restrictions. Tier: `premium`.

**Vintage:** Annual. Current edition covers December 2024 rates. URL pattern: `https://www.env.nm.gov/drinking_water/rates/` links to each year's PDF via service.web.env.nm.gov short URLs.

**Recommendation: INGEST.**

---

### NEW JERSEY (NJ) — 558 CWS, Duke has ~327 (internal_only)

**Category A: NJ BPU**

NJBPU regulates approximately 30 private investor-owned water utilities. No bulk download, no bill comparison tool. Individual PDF tariffs only. The major IOUs (NJ American Water, Veolia/SUEZ, Middlesex Water) are addressable by scraping pipeline but are already partially covered by Duke (internal_only). Skip for BPU.

**Category C: Jersey WaterCheck / NJDEP WQAA**

Jersey WaterCheck (`njwatercheck.com`) is a public dashboard built by Jersey Water Works covering ~266 water utilities (representing ~90% of NJ households with drinking water service). The NJDEP collects annual water rate data from all public community water systems with >500 connections as part of the Water Quality Accountability Act (WQAA).

The Reports page (`njwatercheck.com/Reports`) offers an Excel export button with the following confirmed rate metrics at system level:
- `Total cost to a residential billed customer with a ⅝" meter for 50,000 gallons used per year (system-level)`
- `Cost/price for 1000 gallons to a residential billed customer for 50,000 gallons used per year (system-level)`
- `Cost/price for 1000 gallons to a residential billed customer for 80,000 gallons used per year (system-level)`
- `Annual residential cost/price of sewer services at 45,000 gallons of water consumption (system-level)`

**Critical unknowns requiring interactive verification:**
1. Does the Excel export require login? The page has a "Log In" link — unclear if export is public or member-gated.
2. Do system records include PWSIDs or only utility names?
3. What is the vintage of rate data? A 2024 article referenced "2022 rates" as the most recent NJDEP data.
4. How many systems have populated rate values (vs. N/A)?

**If export is public and PWSIDs are included:** This would be ~150-200 incremental NJ PWSIDs (after dedup against the 327 Duke URLs, which are internal_only). Non-trivial value.

**Recommendation: DEFER.** Requires a manual browser session to verify export accessibility and PWSID inclusion before committing to a Claude Code ingest spec.

---

### OREGON (OR) — 747 CWS, 9 Duke PWSIDs (internal_only)

**Category B: League of Oregon Cities Annual Water Rates Survey**

The League of Oregon Cities (LOC) publishes an annual Water Rates Survey Report (`orcities.org/application/files/...`). The 2023 report (published Feb 2024) is confirmed downloadable. However, it is **aggregate-only**: results are reported as regional/population-quintile averages, not per-utility. 70 cities responded out of 241 total Oregon cities. No per-utility rate table, no utility names, no PWSIDs. This is methodology documentation, not bulk rate data.

**Category A/C:** Oregon Health Authority (OHA) Drinking Water Services does not collect rate data. OHA's Drinking Water Data Online (`yourwater.oregon.gov`) covers compliance/quality data, not rates.

**Recommendation: SKIP** (LOC survey is aggregate; OHA has no rate data).

---

### VIRGINIA (VA) — 910 CWS, ~50 scraped PWSIDs

**EFC Dashboard — confirmed exists**

A UNC EFC Virginia Water and Wastewater Rates Dashboard exists at:
`https://dashboards.efc.sog.unc.edu/va`

Dashboard page confirms: "Rates as of July 1, 2019. Dashboard updated: February 28, 2020." Data source: Draper Aden 2019 Virginia Water & Wastewater Rate Survey.

The same Topsail JSON API that powers the existing 19-state EFC sources serves this dashboard. Coverage: estimated ~136 utilities based on the TRC 2022 report (which surveyed 136 of 259 Virginia water and sewer providers).

**Concerns:**
- Data vintage is 2019 — 6+ years stale. This is significantly older than typical EFC dashboard data (most are 2021-2024).
- TRC (formerly Draper Aden) publishes a new annual report — the 2023 report was confirmed available. It is unknown whether the EFC dashboard has been updated with 2023 data; the dashboard landing page still shows 2020 as the last update date.
- The TRC annual report is produced as a service to clients; the raw utility-level data may or may not be publicly available for ingest.

**Recommendation: DEFER.** Before scheduling a Claude Code session, verify: (1) whether the Topsail API for VA returns 2019 or more recent data; (2) whether the EFC/TRC partnership has produced a dashboard update using 2022 or 2023 survey data. If the data is still 2019, the vintage concern is significant for a commercial API product. VA already has ~50 scraped PWSIDs; a 2019 bulk source may not be worth the ingest effort given potential staleness conflicts.

---

### VERMONT (VT) — 400+ CWS ⭐ HIGH VALUE

**EFC Dashboard — confirmed, 182 utilities**

A UNC EFC Vermont Water and Wastewater Rates Dashboard exists at:
`https://dashboards.efc.sog.unc.edu/vt`

Confirmed details from EFC resource page: "The EFC has surveyed nearly all of the utilities in the state and visualized their rates and financial data on the dashboard in 2021. The current dashboard displays water and wastewater rates as of July 2021 for **182 utilities** in Vermont."

Data sources: UNC EFC + Vermont Department of Environmental Conservation Water and Wastewater Rates Survey. Funded by VT DEC.

This is the same Topsail JSON API, same format, same ingest pattern as the existing 19-state EFC sources. Vermont is not in the existing 19-state list. VT currently has near-zero commercial bulk coverage.

**Vintage:** July 2021. Reasonably current (4 years stale) — comparable to several existing EFC states.

**PWSIDs:** EFC dashboards include PWSIDs cross-referenced from EPA SDWIS.

**Incremental PWSIDs:** Estimated 150-165 (Vermont has ~400+ CWS; the survey covers 182; match rate ~85-90% → ~155-165 PWSIDs).

**License/Tier:** State/federal partnership data, publicly accessible. Tier: `premium`.

**Recommendation: INGEST.** Direct clone of existing EFC ingest pipeline, just swap in the VT dashboard URL.

---

### NEBRASKA (NE) — 500+ CWS

**Category A: Nebraska PSC**

Nebraska PSC regulates "private water company rates" (as stated on their homepage) but explicitly limits this to privately-owned entities — not cities, villages, or special districts. The number of regulated private water companies in NE is small (estimated <20). No bulk tool, no rate comparison database found.

**Category C:** Nebraska DHHS (now absorbed into other agencies) oversees drinking water quality but does not collect rate data. Nebraska DWEE (Department of Water, Energy, and Environment) manages water rights and use data, not utility rates.

**Recommendation: SKIP.**

---

### KANSAS (KS) — 848 CWS, Duke has ~438 PWSIDs (internal_only)

**Category A: Kansas Corporation Commission (KCC)**

KCC regulates electric and gas. Water utilities in Kansas are not regulated at the state level for rates — municipal systems set their own rates, and rural water districts have their own boards. No statewide water rate database found.

**Category C:** KDHE (Kansas Dept. of Health and Environment) maintains the Public Water Supply Data Collector for compliance monitoring, not rates.

**Recommendation: SKIP.** Kansas has significant Duke coverage (internal_only). No premium bulk source path identified.

---

### NEVADA (NV) — 250+ CWS

**Category A: Nevada PUC (PUCN)**

PUCN fully regulates 27 investor-owned water and wastewater utilities serving ~22,300 customers. Critically: PUCN explicitly does **not** regulate Las Vegas Valley Water District (LVVWD), Southern Nevada Water Authority (SNWA), or Truckee Meadows Water Authority — the three dominant utilities serving the vast majority of Nevada's population. Individual tariff PDFs exist for the 27 regulated IOUs, but this is insufficient scale for a bulk ingest (well below 50 PWSIDs, especially since LVVWD/SNWA/TMWA are unregulated).

**Recommendation: SKIP.** The dominant large utilities are unregulated by PUCN. The 27 regulated IOUs would be addressable individually via scraping, not bulk.

---

### VERMONT PUC (supplementary note)

Vermont PUC regulates private water companies (not municipal). The VT EFC dashboard captures both municipal and private systems via the VT DEC survey. The PUC's ePUC system has individual tariff filings but the EFC dashboard is the correct bulk path and is already recommended for ingest.

---

### RHODE ISLAND (RI) — 100+ CWS

**Category A: RI PUC / Division of Public Utilities**

The RIPUC publishes a single webpage listing all regulated water suppliers with current rates. Confirmed from the live page: **6 regulated water utilities** total (Kent County Water Authority, Newport Water Dept., Pawtucket Water Supply Board, Providence Water Supply Board, Veolia Water RI, Woonsocket Water Dept.). The page includes metered rates and service charges with tariff PDF links.

6 utilities is far below the 50 PWSID threshold. The dominant utilities in RI are covered by this page. No bulk source needed — manual ingest of these 6 is trivial.

**Recommendation: SKIP** (too small for pipeline; consider adding the 6 manually as curated entries).

---

### DC — 10+ CWS

DC Water (`dcwater.com`) is the primary water utility serving Washington DC. It is a quasi-governmental authority not regulated by a state PUC. DC Water publishes its own rate schedules. No statewide aggregation needed — single dominant utility. Handle manually via LLM scraping pipeline.

**Recommendation: SKIP** (single utility; address via existing scraping pipeline).

---

### WYOMING (WY), MONTANA (MT), SOUTH DAKOTA (SD), NORTH DAKOTA (ND), IDAHO (ID)

Quick checks confirm no state PUC for water in any of these states. WY DEQ, MT DEQ, SD DENR, ND DEQ, ID DEQ all focus on water quality compliance, not rate regulation. Rural water associations in these states (WY Rural Water, MT Rural Water, SD Rural Water, ND Rural Water, ID Rural Water) provide technical assistance but publish no public rate surveys. No municipal league surveys found.

**All five states: SKIP.**

---

### TENNESSEE (TN) — TAUD revisit

TAUD (Tennessee Association of Utility Districts) has a rate survey page (`taud.org/taudratesurvey/`) — confirmed by URL presence in search results — but the page is currently returning a server error and appears to be a form submission page for member utilities to submit rate data, not a published results page. TAUD's downloads page lists a "Water/Wastewater Use Inventory Survey" template but no published aggregate results. TAUD's member services page mentions providing rate analysis to individual members but not publishing aggregate results publicly.

**Recommendation: SKIP** (TAUD survey is member-submission input, not public output).

---

### LOUISIANA (LA) — LRWA revisit

Louisiana Rural Water Association (LRWA) provides technical assistance including rate analysis for individual member utilities, as confirmed by their Member Services page testimonials. LRWA does not publish an aggregate rate survey. Resources page shows a salary/benefits survey (confidential, members-only) but no water rate survey. LRWA's Annual Conference focuses on training, not rate data publication.

**Recommendation: SKIP.**

---

## New Mexico NMED Rate Survey — full detail

This was the session's most significant find and warrants a complete record.

**Access:** Annual PDFs available freely at `https://www.env.nm.gov/drinking_water/rates/`. Direct PDF links use short URLs via `service.web.env.nm.gov`. The 2025 edition URL (confirmed accessible): `https://service.web.env.nm.gov/urls/nxEGQnEO`

**Coverage:** Publicly-owned community water systems. ~200 entries across 15 pages of the 2025 PDF (the full count was not extracted but estimated from page count and row density). A separate section for investor-owned systems likely exists; confirm during ingest.

**What's NOT in this source:**
- Full tier structures (only bill-at-consumption for 6,000 gal/month)
- PWSIDs (utility name + county matching required)
- Private well systems or very small systems (<15 connections)

**Annual update cycle:** Data as of December of the prior year, published mid-year (2025 edition notes "Revised 7-22-25" for December 2024 rates). Contact: Michael Montoya at 505-570-7682.

**Overlap with existing sources:** NM was covered by Duke (internal_only, ~440 PWSIDs). The NMED survey covers publicly-owned systems only, so likely ~150-180 incremental commercial PWSIDs.

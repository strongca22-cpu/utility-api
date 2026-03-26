# Research Report: MDWD Harvard Dataverse + Jersey Water Works NJ Study

**Date:** 2026-03-26
**Scope:** Assess two academic water rate data sources for ingest potential

---

## Source 1: MDWD (Municipal Drinking Water Database)

### Dataset Identity

- **Harvard Dataverse:** doi:10.7910/DVN/DFB6NG
- **Authors:** Sara Hughes (U Michigan), Christine Kirchhoff (Penn State), et al.
- **License:** CC0 1.0 (public domain)
- **Version:** 3.0 (2023-08-02) — no updates since
- **Funding:** NSF Grant #2048505

### What It Contains

A panel dataset of 2,219 municipally-owned community water systems across 48 states (continental US), with 22 years of observations (1997–2018). 66 columns covering:

- **Identifiers:** PWSID, FIPS codes, Census place name, SDWIS name, Census of Governments ID
- **Municipal finance:** Revenue, expenditure, taxes, water utility enterprise-fund financials (CPI-adjusted)
- **Demographics:** Population, race/ethnicity, income, poverty rate, education
- **System characteristics:** Violation count, water source type, form of government
- **Political:** 2016 presidential vote share

### Critical Finding: ZERO Rate Data

The MDWD contains no rate, bill, tariff, or pricing columns of any kind. The "Water_Utility_Revenue" column is aggregate Census of Governments enterprise-fund reporting — what the utility takes in overall — NOT customer-facing rate schedules.

### Value for Our Project

**Primary value:** PWSID-to-municipality matching. The `Census.Name` and `Name` columns provide standardized city/place names linked to 2,219 PWSIDs. This is useful for fuzzy-matching city-based rate data (TX TML cities, WV PSC utility names) to SDWIS PWSIDs.

**Coverage context:** 1,552 of the 2,219 MDWD PWSIDs currently have no rate data in our system. These cluster in uncovered states: IL (177), MA (131), MI (125), FL (108), OH (105), NY (86), MN (77). These represent the largest municipally-owned systems in each state — exactly the utilities most likely to have discoverable rate schedules online.

**Existing ingest:** The MDWD is already partially loaded (225 records, VA + CA only) into `utility.mdwd_financials` from Sprint 1. A 50-state expansion is trivial but low priority.

### Tier Assignment

**`free_open`** — CC0 public domain, no restrictions.

---

## Hughes 2025 Rate Extension

### What It Is

Sara Hughes et al. published "Understanding the Cost of Basic Drinking Water Services in the United States" (AWWA Water Science, DOI: 10.1002/aws2.70014, January 2025). This paper added water cost data for 2,161 municipalities — monthly bill at 6,000 gallons computed from fixed charges and volumetric rates collected November 2021 – May 2022.

### Data Availability: NOT PUBLIC

The paper's Data Availability Statement (page 10):

> "The data that support the findings of this study are available from the corresponding author upon reasonable request."

The rate dataset was **never deposited** in any public repository. The Harvard Dataverse MDWD has not been updated since August 2023 — no rate columns were added. No supplementary data files exist on the Wiley page. The paper is available as a free PDF via NSF PAGES (https://par.nsf.gov/servlets/purl/10574381).

### What the Data Contains (from paper methodology)

- Monthly cost at 6,000 gal: range $5.00–$163.40, mean $38.60
- 2,161 municipalities (2,119 in final analysis)
- Computed from fixed charges + volumetric rates
- **Critically: only the computed bill is reported, NOT the underlying rate structure**
- No tier breakpoints, no per-unit rates, no rate schedule decomposition

### Contact

Sara Hughes has moved from University of Michigan to **RAND Corporation, Santa Monica, CA** (shughes@rand.org).

### Value Assessment

**Low priority.** Even if obtained:
- Only one consumption point (6,000 gal/month) — cannot calculate bills at other volumes
- No rate structure decomposition — strictly inferior to Duke's full tier breakpoints
- ~660 of the 2,161 PWSIDs overlap with our existing rate data
- ~1,500 would be new, but only as single bill estimates

**Action:** Contact Hughes only if a large-scale bill validation campaign is planned. Frame as validation data, not primary rate source.

### Tier Assignment

**`internal_only`** — Data not publicly available. License terms unknown until negotiated with author.

---

## Source 2: Jersey Water Works / Van Abs NJ Study

### Dataset Identity

- **Title:** "A New Jersey Affordability Methodology and Assessment for Drinking Water and Sewer Utility Costs"
- **Authors:** Daniel J. Van Abs PhD, with Tim Evans and Kimberley Irby (New Jersey Future)
- **Date:** August 2021 (Phase 3)
- **Commissioned by:** Jersey Water Works (via New Jersey Future)
- **PDF URL:** https://cms.jerseywaterworks.org/wp-content/uploads/2021/09/Van-Abs-2021.08-NJ-Affordability-Assessment-Smaller-file-size-2-compressed.pdf
- **Downloaded to:** data/raw/nj_jwe/Van-Abs-2021-NJ-Affordability-Assessment.pdf

### What It Contains

**Appendix B (pages 51–59):** Drinking water utility cost table with ~275 PWSIDs.

Columns:
| Column | Description |
|--------|-------------|
| PWSID | 7-digit NJ format (no state prefix) |
| Water System Name | Utility name |
| County | NJ county |
| System Size | Categorical: Very Large >100K through Tiny |
| Base Charge (quarterly) | Fixed charge per quarter, USD |
| Volume Charge (11.25K gal/qtr) | Volumetric charge at one consumption point |
| Total Annual Cost (45K gal/yr) | Pre-computed annual bill |

**Sample rows:**

| PWSID | System Name | Base Charge | Vol Charge | Annual Cost |
|-------|------------|-------------|------------|-------------|
| 0238001 | SUEZ WATER NJ HACKENSACK SA1 | $53.79 | $63.83 | $470.48 |
| 2004002 | NJ AMERICAN WATER - RARITAN A2 | $60.60 | $74.38 | $539.93 |
| 0714001 | NEWARK WATER DEPT | $0.00 | $27.93 | $111.73 |

### Critical Limitations

1. **No tier breakpoints** — only one volumetric charge at one consumption level
2. **No volumetric rate per unit** — cannot derive $/1000 gal or $/CCF
3. **Cannot reconstruct rate schedules** — only pre-computed bills
4. **Single consumption point** — 45K gal/year (11,250 gal/quarter)
5. **Data vintage:** 2020 rate schedules, now 5–6 years old
6. **No structured data** — PDF only. No CSV/Excel available anywhere.

### Overlap with Duke

Duke covers 327 NJ PWSIDs with **full tier structures** (breakpoints, per-unit rates, multiple meter sizes). Van Abs covers ~275 NJ PWSIDs with only pre-computed bills. The overlap is substantial. Incremental coverage: likely ~50 PWSIDs at most, and those would only have a single bill estimate.

### Structured Data Search — Negative

- No dataset on Rutgers RUcore institutional repository
- No supplementary data from AWWA companion paper (Van Abs 2022, doi:10.1002/aws2.1287)
- No bulk download from Jersey WaterCheck dashboard (njwatercheck.com)
- No public NJDEP utility cost dataset found
- NJDEP collects cost data under the Water Quality Accountability Act, but it feeds the WaterCheck dashboard and is not independently downloadable

### License

No explicit license statement in the PDF or on the JWW website. Standard copyright applies. The report was publicly funded (Dodge Foundation) and freely distributed. Likely usable with attribution, but no formal open license.

### Decision: SKIP

**Not worth ingesting.** Rationale:
- Duke already covers NJ with 327 PWSIDs + full tier structures
- Van Abs adds ~50 incremental PWSIDs at most, with strictly inferior data (one bill amount only)
- PDF extraction effort is non-trivial for minimal value
- Data is 5+ years old
- No structured version exists

The sewer utility cost table (Appendix C, ~500 municipality entries) has secondary value if sewer affordability becomes relevant — but that's not in scope.

---

## Summary of Findings

| Source | Rate Data? | Public? | PWSIDs | Incremental Value | Tier | Action |
|--------|-----------|---------|--------|--------------------|------|--------|
| MDWD base | No | Yes (CC0) | 2,219 | Matching metadata only | free_open | Low priority 50-state expansion |
| Hughes 2025 rates | Yes (bills only) | No | 2,161 | ~1,500 gap-fill (bills only) | internal_only | Contact if validation needed |
| Van Abs NJ | Yes (bills only) | PDF only | ~275 | ~50 over Duke (bills only) | skipped | Not worth ingesting |

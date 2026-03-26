# Duke/Nicholas Institute Water Affordability Dashboard: Research Report

**Date:** 2026-03-25
**Status:** FULLY ACCESSIBLE — data cloned and schema documented

---

## How the Data Was Found

The GitHub repository was the key discovery:
- **Search query that worked:** `duke nicholas institute water affordability dashboard tool`
- **Repository URL:** `https://github.com/NIEPS-Water-Program/water-affordability`
- **The README explicitly states:** "data folder contains the raw service area boundaries, census, and rates data needed to create the dashboard. The rates_data are provided in csv format for each state in the dashboard."
- Note: Despite the README saying "csv format," the actual files are `.xlsx` (Excel). This is likely a documentation error.

## Data Access Method

**Git clone → Excel files.** No API, no scraping, no authentication needed.

```bash
git clone https://github.com/NIEPS-Water-Program/water-affordability.git
# Data is in: data/rates_data/rates_{state}.xlsx
```

Each Excel file contains two sheets:
1. `ratesMetadata` — One row per (PWSID, service_type). Contains effective dates, source URLs, and analyst notes.
2. `rateTable` — Multiple rows per PWSID. Contains the full rate structure: fixed charges, volumetric tiers, meter sizes, surcharges.

## Coverage: 10 States, 3,297 PWSIDs

The repo contains MORE states than the dashboard advertises (10 vs 7):

| State | PWSIDs | Rate Rows | In EFC? | Incremental Value |
|-------|--------|-----------|---------|-------------------|
| TX | 751 | 8,509 | No | CRITICAL |
| CA | 673 | 5,335 | Partial | Moderate (overlap with eAR/OWRS) |
| NC | 516 | 6,389 | Yes | Low (already have EFC NC) |
| KS | 438 | 5,889 | No | HIGH |
| PA | 332 | 6,836 | No | HIGH |
| WA | 246 | 2,244 | No | HIGH |
| NJ | 214 | 1,478 | No | HIGH |
| CT | 65 | 518 | Yes | Low |
| NM | 53 | 4,917 | No | Moderate |
| OR | 9 | 480 | No | Low |
| **Total** | **3,297** | **43,595** | | |

**Gap-filling states (not in EFC):** TX (751), KS (438), PA (332), WA (246), NJ (214), NM (53) = **2,034 new PWSIDs** in states with no other bulk source.

## Data Quality Assessment

**Strengths:**
- Uses EPA PWSIDs — direct join to our SDWIS database
- Full rate structure detail: fixed charges, volumetric tiers with breakpoints, meter sizes
- Includes both water AND sewer rates
- Well-documented schema with metadata template
- Source URLs (utility websites) are recorded in the metadata — bonus for URL discovery
- Open source with clear citation requirements

**Weaknesses:**
- Data vintage is primarily 2019-2021 (rates effective dates). Now 4-5 years old.
- Manually collected by Duke research team — may have transcription errors
- CC BY-NC-ND 4.0 license restricts commercial use and derivatives
- "NA" used as string literal for missing values (not null)
- Some Excel formulas remain unevaluated in year/month/day columns
- Coverage is partial within each state (e.g., 751 of 4,584 TX CWS)

## License Implications

The data is licensed CC BY-NC-ND 4.0 (NonCommercial, NoDerivatives). This means:
- **OK:** Using for internal research and development, building ingest pipelines
- **OK:** Using PWSIDs as a target list for direct scraping of primary sources
- **NOT OK:** Directly incorporating rate values into a commercial product
- **NOT OK:** Creating derivative datasets for sale

**Recommended approach for commercial use:** Use the Duke data to identify which PWSIDs have rate data and what their rate structures look like, then scrape the primary sources (the `website` URLs in the metadata) to get current rate data that isn't encumbered by the Duke license. Alternatively, contact Duke OTC for commercial licensing (otcquestions@duke.edu, ref OTC File No. 7797).

## Bonus Discovery: Utility Website URLs

The `ratesMetadata.website` column contains the URL where each utility's rate data was found. This is independently valuable for the URL discovery pipeline — it provides ~3,297 confirmed utility rate page URLs, many of which may still be valid.

## Related Datasets

1. **National Water Affordability Dashboard (787 communities):** A separate, more recent Duke dashboard at `nicholasinstitute.duke.edu/water-affordability/water-affordability-united-states/` covers 787 communities representing half the US population. Published in PLOS Water. May contain additional states or more recent data. Worth investigating separately.

2. **Zenodo archive:** The dataset is also archived at DOI `10.5281/zenodo.5156654`. This may have version-specific snapshots.

3. **Jersey Water Works NJ Affordability Study:** A separate NJ-specific study by Rutgers/Van Abs with detailed NJ water and sewer rates. Published 2021. Contains rate tables for NJ utilities in PDF appendices. Could supplement the Duke NJ data.

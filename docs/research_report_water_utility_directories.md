# Water Utility Website URL Database — Directory Research Report

**Date:** 2026-03-25  
**Scope:** Federal and state drinking water program directories  
**Goal:** Discover utility website URLs mapped to PWSIDs for automated rate scraping  

---

## Executive Summary

**Critical finding: No federal or state drinking water directory includes utility website URLs.**

After systematically researching EPA ECHO/SDWIS, Envirofacts, EWG Tap Water Database, and state drinking water program directories for all Priority 1 and Priority 2 states, the conclusion is clear: these directories track regulatory compliance data (violations, water quality, contact addresses, phone numbers) but do not store utility website URLs. The SDWIS data model has no website/URL field.

This means the original strategy of "bulk-discover utility website URLs from authoritative directories" cannot work as envisioned — no such bulk source exists. However, the research did identify high-value **supplementary data** (system names, counties, populations, mailing addresses, admin contacts) available via bulk download from several sources. This data can significantly improve the existing SearXNG-based URL discovery by providing better search queries and enabling batch processing by state/county.

### What IS Available (Bulk)

| Source | Coverage | Format | Key Fields | Website URLs? |
|--------|----------|--------|------------|---------------|
| EPA ECHO SDWA Download | ~50,000 CWS nationwide | CSV (bulk ZIP) | PWSID, name, address, county, population, admin contact, phone | **No** |
| Envirofacts WATER_SYSTEM API | ~150,000 PWS nationwide | JSON/CSV/XML API | PWSID, name, address, county, population, source type | **No** |
| CA Drinking Water Repository | ~8,000 PWS in CA | CSV downloads | System number, name, contact, location, source type | **No** |
| OR Drinking Water Data Online | ~3,600 PWS in OR | HTML searchable + downloadable inventory | PWSID, name, contact person, phone, county | **No** |
| WA Sentry Internet | ~4,000+ PWS in WA | HTML searchable + data download | System ID, name, address, phone, county | **No** |
| VA Waterworks Owner Listing | ~2,800 PWS in VA | PDF (342 pages) | PWSID, name, county, owner, address, email | **No** |
| TX Water District Database | ~7,000+ in TX | HTML searchable + Open Data Portal | Name, address, county, contacts | **No** |
| EWG Tap Water Database | ~48,500 PWS | HTML (by PWSID) | PWSID, name, contaminants, quality data | **No** |

---

## Tier 1: Federal Sources — Detailed Findings

### EPA ECHO / SDWIS (Safe Drinking Water Information System)

**Status:** Thoroughly investigated. **No website URL field exists.**

The SDWIS database is the federal register of all ~150,000 public water systems. It is the most comprehensive data source, with every system identified by PWSID. The data model includes:

- `PWSID` — unique identifier (2-letter state + 7 digits)
- `PWS_NAME` — system name
- `ADDRESS_LINE1`, `ADDRESS_LINE2` — mailing address
- `CITY_NAME`, `STATE_CODE`, `ZIP_CODE`
- `COUNTY_SERVED`
- `POPULATION_SERVED_COUNT`
- `ADMIN_NAME` — administrative contact name
- `PHONE_NUMBER`, `ALT_PHONE_NUMBER`, `FAX_NUMBER`
- `PWS_TYPE_CODE` — CWS, NTNC, NC
- `GW_SW_CODE` — groundwater vs surface water
- `SERVICE_CONNECTIONS_COUNT`

**No `WEBSITE`, `URL`, `WEB_ADDRESS`, or similar field exists in the SDWIS data model.** This was confirmed by reviewing:

1. The ECHO SDWA Data Download Summary and Data Element Dictionary (all fields listed alphabetically)
2. The ECHO SDW REST Services API documentation (`get_systems` endpoint returns `DfrUrl` for the EPA facility report URL — not the utility's own website)
3. The NPM package documentation for `@datafire/epa_gov_sdw` listing all response fields
4. The Envirofacts WATER_SYSTEM table metadata

**Bulk downloads available:**
- ECHO SDWA Data Downloads: https://echo.epa.gov/tools/data-downloads (weekly-refreshed ZIP with CSV files including `SDWA_PUB_WATER_SYSTEMS.csv`)
- Envirofacts REST API: `https://data.epa.gov/efservice/sdwis.water_system/` (queryable, supports CSV/JSON/XML output, paginatable)
- ECHO SDW REST API: `https://echodata.epa.gov/echo/sdw_rest_services.get_systems` (queryable, supports CSV download via `get_download`)

**Value for this project:** The ECHO SDWA bulk download provides the authoritative PWSID → name + county + population mapping needed for URL matching. Even though it lacks URLs, it is essential reference data. Download and use it as the master list for search-based URL discovery.

### Envirofacts SDWIS Tables

**Status:** Available via REST API. **No website URL field.**

The Envirofacts API allows querying SDWIS tables directly. The `WATER_SYSTEM` table contains inventory information. The API is at:
```
https://data.epa.gov/efservice/sdwis.water_system/STATE_CODE/VA/rows/0:100/JSON
```

This can be used to bulk-extract system names, addresses, and counties by state — useful for constructing targeted search queries.

### EWG Tap Water Database

**URL:** https://www.ewg.org/tapwater/  
**Status:** Profiles keyed by PWSID. **No utility website URLs.**

EWG profiles (~48,500 systems) are accessible via `https://www.ewg.org/tapwater/system.php?pws=XX1234567` and contain water quality/contaminant data only. No API or bulk download is available. EWG does not link to utility websites. The data is sourced from SDWIS/state agencies and focuses on health/contaminant information.

### EPA Public Water System Service Areas (GIS)

**URL:** https://www.epa.gov/ground-water-and-drinking-water/public-water-system-service-areas  
**Status:** Geospatial boundaries dataset. **No website URLs.**

This dataset maps service area boundaries for community water systems nationwide. Each polygon is attributed with PWSID and basic system info (name, population, connections). Useful for spatial analysis but contains no website URLs.

---

## Tier 2: State Drinking Water Program Directories

### Summary Table

| State | Directory | Format | Bulk DL | Has PWSID | Has Website URLs | Has Contact Info |
|-------|-----------|--------|---------|-----------|-----------------|-----------------|
| VA | VDH Drinking Water Viewer + Waterworks Listing | HTML search + PDF | PDF only | Yes | **No** | Yes (name, address, phone, email) |
| TX | TCEQ Drinking Water Watch + Water District Database | HTML search + Open Data Portal | Partial | Yes (7-digit state IDs) | **No** | Yes (name, address) |
| CA | SWRCB Drinking Water Watch + DDW Program Repository | HTML search + CSV downloads | **Yes (CSV)** | Yes | **No** | Yes (name, location, contact) |
| OR | OHA Drinking Water Data Online | HTML search + downloadable inventory | **Yes (HTML table export)** | Yes | **No** | Yes (contact person, phone) |
| WA | DOH Sentry Internet + Data Download | HTML search + data files | **Yes (data files)** | Yes | **No** | Yes (address, phone) |
| GA | GA EPD Drinking Water Program | HTML search | No | Yes | **No** | Limited |
| NC | NC DEQ Public Water Supply | HTML search | No | Yes | **No** | Limited |
| OH | OH EPA Division of Drinking Water | HTML search | No | Yes | **No** | Limited |
| IL | IL EPA Bureau of Water | HTML search | No | Yes | **No** | Limited |
| AZ | ADEQ Water Quality Division | HTML search | No | Yes | **No** | Limited |
| NV | NV DEP | HTML search | No | Yes | **No** | Limited |
| IA | IA DNR Water Supply | HTML search | No | Yes | **No** | Limited |
| SC | SC DHEC Bureau of Water | HTML search | No | Yes | **No** | Limited |
| PA | PA DEP | HTML search | No | Yes | **No** | Limited |
| NY | NY DOH | HTML search | No | Yes | **No** | Limited |
| FL | FL DEP | HTML search | No | Yes | **No** | Limited |
| NJ | NJ DEP Drinking WaterWatch | HTML search | No | Yes | **No** | Limited |

### Detailed State Findings

#### Virginia (VA) — Priority 1

**Directory:** VDH Office of Drinking Water — Drinking Water Viewer (DWV)  
**URL:** https://www.vdh.virginia.gov/drinking-water/drinking-water-data/  
**Waterworks Listing PDF:** https://www.vdh.virginia.gov/drinking-water/information-for-consumers/listing-of-waterworks-and-owners/

- DWV is a searchable web application for viewing water system data
- The Waterworks Owner Listing (PDF, 342 pages as of 2020 edition, updated ~annually) contains: PWSID, system name, operator category, city/county, system type, service connections, population, source, owner type, owner name/address, admin contact name/address/email
- **No website URLs in either source**
- Some admin contact emails are listed (e.g., `jlinkous@pulaskicounty.org`) which could help identify government domains
- Data is keyed by PWSID (format: 7-digit number, e.g., 1035475)
- Virginia PWSIDs in federal format: VA + 7 digits (e.g., VA1035475)

#### Texas (TX) — Priority 1

**Directory:** TCEQ Drinking Water Watch + Water District Database  
**DWW:** Referenced at https://www.tceq.texas.gov/assistance/water/pdws  
**WDD:** https://www.tceq.texas.gov/waterdistricts/iwdd.html  
**GIS Data:** https://gis-tceq.opendata.arcgis.com/ (ArcGIS feature service with PWS locations)  
**Open Data Portal:** Texas Open Data Portal has water district contact info  

- Texas uses 7-digit PWS IDs where first 3 digits = county number (254 counties, alphabetical)
- Federal PWSID format: TX + 7 digits
- WDD is searchable by name, county, or application status
- Contact information available includes office addresses
- **No website URLs in any TCEQ directory**
- The ArcGIS feature service could be queried programmatically for location data

#### California (CA) — Priority 1

**Directory:** CA SWRCB Division of Drinking Water  
**Drinking Water Watch:** https://sdwis.waterboards.ca.gov/PDWW/  
**Program Repository:** https://www.waterboards.ca.gov/resources/data_databases/compliance_enforcement.html  
**Data Catalog:** https://catalog.data.gov/dataset/drinking-water-public-water-system-information  

- **Best state data availability:** CSV downloads of all active public water systems including contact, location, water source, and type
- Program Repository provides flat files (CSV/Excel) for multiple data categories
- Data is keyed by Water System Number (PWSID)
- **No website URLs** in any available download or search interface
- Contact info includes name, phone, address for system contacts

#### Oregon (OR) — Priority 1

**Directory:** OHA Drinking Water Services — Data Online  
**URL:** https://yourwater.oregon.gov/  
**Inventory:** https://yourwater.oregon.gov/inventorylist.php  
**Search:** https://yourwater.oregon.gov/wssearch.php  

- Well-structured searchable database with map integration
- Water System Inventory is downloadable/filterable
- Individual system pages include: contact person name, phone number, county served, connections, sources
- **No website URLs**
- Data is keyed by PWSID (format: OR + 7 digits in federal)

#### Washington (WA) — Priority 1

**Directory:** WA DOH Sentry Internet  
**URL:** https://doh.wa.gov/data-statistical-reports/environmental-health/drinking-water-system-data/sentry-internet  
**Data Download:** https://doh.wa.gov/data-statistical-reports/environmental-health/drinking-water-system-data/data-download/data-terms  

- Sentry Internet is the main searchable database for WA water systems
- Data download files available with system general data and source data
- Fields include water system ID, name, address, phone numbers, county
- **No website URLs**
- Useful for consultants, lending institutions, and local health jurisdictions

#### New Jersey (NJ) — Priority 2

**Directory:** NJ DEP Drinking WaterWatch  
**URL:** https://www-dep.nj.gov/DEP_WaterWatch_public/  

- Searchable by PWSID or system name (supports wildcards with `%`)
- **No website URLs** — compliance-focused data only

---

## Tier 3: Aggregator and Association Sources

### AWWA (American Water Works Association)

**URL:** https://www.awwa.org/  
**Member Directory:** Exists but is **members-only/login-required**  
**MarketBASE Sourcebook:** https://sourcebook.awwa.org/ — This is a supplier/vendor directory, NOT a utility directory  

- AWWA has 4,300+ utility members covering ~80% of North America's drinking water
- No public utility directory with website URLs is available
- Not usable for this project without membership

### NAWC (National Association of Water Companies)

**URL:** https://nawc.org/  
**Status:** No public member directory  

- Represents investor-owned water utilities (American Water, Aquarion, California Water Service, etc.)
- Member companies are named in press releases and board announcements but no structured directory
- The major investor-owned utilities (American Water Works, Essential Utilities/Aqua, California Water Service, SJW Group, Middlesex Water, Artesian Resources, etc.) have known corporate websites — these could be manually mapped

### State Rural Water Associations

- These associations (e.g., VRWA - Virginia Rural Water Association) sometimes maintain member directories
- Most are behind login walls or are simple listing pages without website URLs
- Low value for bulk URL discovery

### State Municipal Leagues

- Municipal league directories (e.g., Virginia Municipal League) list member cities with links to city government websites
- These are NOT mapped to PWSIDs and would require matching
- Could be useful for identifying city government homepages that host water utility departments

### TapWaterData.com (Commercial)

**URL:** https://www.tapwaterdata.com/data  
- Commercial dataset offering verified contact data for 4,385+ US water utilities
- Includes names, titles, phone numbers, emails, mailing addresses
- Available by state or nationally in CSV/Excel/API
- **Likely does NOT include website URLs** (focused on B2B contact data)
- Paid service — requires quote

---

## Recommended Strategy Pivot

Since no directory contains website URLs, the project should pivot to a **search-enhanced approach** using directory data as seed information:

### Phase 1: Build the Master Reference (Immediate)

1. **Download ECHO SDWA bulk data** from https://echo.epa.gov/tools/data-downloads
   - Extract `SDWA_PUB_WATER_SYSTEMS.csv`
   - Filter to `PWS_TYPE_CODE = 'CWS'` and `PWS_ACTIVITY_CODE = 'A'` (active community water systems)
   - This gives ~50,000 systems with PWSID, name, city, county, state, population

2. **Download CA Program Repository CSVs** for California-specific enrichment

3. **Construct optimized search queries** using system names:
   - `"{system_name}" water rates {city} {state}` 
   - `"{system_name}" utility rates schedule`
   - Use population to prioritize (larger systems first = more public web presence)

### Phase 2: Improve Search Hit Rate (Short-term)

1. **Use admin contact email domains** where available (from VA PDF, state directories):
   - If admin email is `jsmith@staffordcountyva.gov`, the utility website is likely `https://www.staffordcountyva.gov`
   - Parse email domains to extract government website URLs
   - Then search within those sites for water rate pages

2. **Use city/county government homepage patterns:**
   - Most municipal water systems are departments of city/county governments
   - Pattern: `https://www.{city}{state}.gov` or `https://www.{city}.{state}.us`
   - Search for `/utilities`, `/water`, `/public-works`, `/rates` paths within these sites

3. **For investor-owned utilities** (American Water, Essential Utilities, etc.):
   - Map parent company → subsidiary → PWSID using SDWIS owner data
   - Major companies have well-structured websites with rate pages per service area

### Phase 3: Consumer Confidence Reports (Medium-term)

- The EPA requires all CWS to publish an annual Consumer Confidence Report (CCR)
- The EPA CCR search at https://ofmpub.epa.gov/apex/safewater/f?p=CCR_WISER may contain links to utility websites where CCRs are hosted
- Many utilities post CCRs on their own websites, so a CCR link often leads directly to a utility website
- This could be a productive source of website URLs paired with PWSIDs

---

## Files Produced

1. `research_report_water_utility_directories.md` — This report
2. `config/source_catalog_directories.yaml` — Catalog of all discovered directory sources
3. `config/strategy_search_queries.yaml` — Recommended search query templates

---

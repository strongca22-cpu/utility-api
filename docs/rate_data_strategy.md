# Water Rate Data Strategy

## Current State (Session 5, 2026-03-24)

- VA: 22/31 high/medium (90% pop coverage)
- CA: 16/194 high/medium via web scraping
- Total: 38 utilities, 9.9M population, $0.97 API cost

## Strategic Approach

### Layer 1: State-Level Bulk Data (highest ROI)

#### California SWRCB eAR (Electronic Annual Report)
- **~7,400 CA water systems** file annual rate data
- HydroShare has cleaned 2020-2022 data: https://www.hydroshare.org/resource/8108599db4934252a5d0e6e83b5d3551/
- Raw data: https://data.ca.gov/dataset/groups/drinking-water-public-water-systems-rates-information
- eAR portal: https://ear.waterboards.ca.gov/
- **Action**: Download HydroShare processed data, map to our water_rates schema. Could cover all 194 CA MDWD utilities in one ingest.

#### California Data Collaborative — OWRS
- Machine-readable YAML rate structures: https://github.com/California-Data-Collaborative/Open-Water-Rate-Specification
- Bill calculator: https://github.com/California-Data-Collaborative/BillCalculator
- Data on OpenEI: https://data.openei.org/submissions/674
- **Action**: Check coverage count and data vintage. If recent enough, ingest as validation layer.

#### CPUC Tariff Filings (Investor-Owned Utilities)
- ~10 Class A IOUs: Cal Water, American Water, Liberty, Golden State, San Jose Water
- PDFs at files.cpuc.ca.gov/WaterAdviceLetters/
- **Action**: Parse tariff PDFs for IOUs — covers many MDWD service areas

### Layer 2: Platform-Based Crawling (medium ROI)

#### CivicPlus DocumentCenter
- 14% of utilities use this platform
- Consistent pattern: `{domain}/DocumentCenter/View/{id}`
- **100% PDF extraction success rate** when we find the right document
- **Action**: For each CivicPlus city, crawl /DocumentCenter index page to find rate-related PDFs by filename

#### Revize CMS
- Pattern: `cms9files.revize.com/{city}/...`
- Example: Manassas, Stockton
- **Action**: Similar crawl approach

### Layer 3: Web Scraping (lowest ROI per utility, but necessary for gaps)

Current pipeline: SearXNG discovery → HTTP/Playwright scrape → Claude API parse

**Optimization opportunities:**
- PDF-first search queries (`filetype:pdf site:{domain}`)
- Authority-specific naming in queries
- VPS proxy to double SearXNG rate limit capacity
- Standalone tmux discovery script (no API calls needed)

### Layer 4: Manual Curation (last resort)

For utilities where all automated approaches fail:
- CivicPlus 403 sites
- No web presence
- Complex rate structures (LADWP seasonal/budget-based)

## CMS Platform Distribution (from log analysis)

| Platform | Count | % | PDF Success |
|----------|-------|---|-------------|
| CivicPlus (DocumentCenter) | 9 | 14% | 100% |
| Revize | 1 | 2% | 100% |
| WordPress | 1 | 2% | N/A |
| Custom/Other | 53 | 83% | varies |

## Spot-Check Issues

- **Colonial Heights VA**: Tier limits wrong (unit conversion error)
- **Manassas Park VA**: Combined water+sewer, not water-only
- **Anaheim CA**: May be missing volumetric charge ($15.75 seems low)
- **LADWP**: 2016 base ordinance only, missing pass-through adjustments
- **Escondido CA**: Duplicated San Diego URL

## Recommended Next Steps (Priority Order)

1. **Ingest SWRCB eAR data** — single bulk load could cover all 194 CA utilities
2. **Ingest OWRS data** — machine-readable, maps directly to our schema
3. **CivicPlus DocumentCenter crawler** — platform-specific, high success rate
4. **Run standalone discovery** in tmux when SearXNG recovers
5. **VPS proxy routing** for doubled rate limit capacity
6. **Manual curation** for remaining gaps

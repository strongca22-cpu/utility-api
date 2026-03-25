# Utility Enrichment Layer — Product Vision & Strategy

## The One-Liner

A geospatial database of US water utility service boundaries, rates, rate structures, and management context — queryable by location, served as an API, and maintained by LLM-agent scraping of public records.

---

## The Problem

There are ~50,000 community water systems in the United States. Nobody has built a unified, geospatially-indexed dataset that answers the question: **"Given a location, who provides water, what does it cost, and how is it structured?"**

The pieces exist separately:

- **EPA CWS Service Area Boundaries** give you the polygons (who serves where) — public domain, 44,000 systems, 99% population coverage
- **SDWIS** gives you system metadata (source water, population, violations) — public domain, linked by PWSID
- **Raftelis/AWWA** have rate data for 500 utilities — paywalled, dashboard-only, no geospatial index
- **Bluefield Research** tracks 61 utilities deeply — enterprise subscription, no API
- **Black & Veatch / Circle of Blue** cover 30–50 large cities — annual snapshots, no programmatic access
- **UNC EFC** has state-level dashboards for ~13 states — high coverage within those states, but no VA or CA

Nobody has wired these layers together. Nobody serves them through an API. Nobody covers more than ~5% of the 50,000 systems with actual rate data. And nobody has built a method to scale rate collection beyond manual surveys.

## The Product

### Product A: The Dataset

A geospatial database where each record is a US community water system (keyed by PWSID) with three layers of information:

**Layer 1 — Geographic Scope (where)**
- Service area boundary polygon (from EPA CWS dataset)
- Centroid coordinates
- Counties and jurisdictions served
- Population served, service connections
- Source: EPA public domain. Complete for ~44,000 systems.

**Layer 2 — Rates (how much)**
- Current residential rate schedule: fixed charges, volumetric tiers, surcharges
- Cost benchmarks at standard consumption levels (5 CCF, 10 CCF, 20 CCF monthly)
- Effective date, next scheduled change
- Source: LLM-agent parsed from utility websites. Coverage grows over time.

**Layer 3 — Rate Structure & Type (how)**
- Classification: flat, uniform volumetric, increasing block, decreasing block, seasonal, budget-based, drought-adjusted, demand-based
- Number of tiers
- Conservation signal strength (ratio of highest to lowest tier)
- Billing frequency
- Source: Derived deterministically from Layer 2 parsed data.

Each record also carries:
- SDWIS metadata (system type, source water, owner type, SDWA violation history)
- Provenance chain (source URL, scrape timestamp, confidence score)
- Last verified date

### Product B: The API

A wrapper on Product A that resolves queries by location. Three endpoints:

**`/resolve`** — Given a lat/long or address, return the PWSID(s) that serve that location, with Layer 1 metadata. This is the spatial join against EPA CWS polygons.

**`/utility/{pwsid}`** — Given a PWSID, return the full record: geographic scope, rate schedule, rate structure classification, SDWIS metadata, provenance.

**`/site-report/{lat}/{lng}`** — Composite endpoint. Resolves location → utility, then returns the full utility record plus any applicable regulatory permits (state DEQ overlay). This is the premium call.

Product B is not a separate product from Product A. It is Product A made accessible by use case. The dataset is the asset; the API is the delivery mechanism.

---

## Competitive Position

### What the incumbents have that we don't
- **Historical depth.** Raftelis has tracked rates since 2002. Bluefield has 9 years of annual city-level data. Circle of Blue has 2010–2019 trends for 30 cities. We start at today. Backscraping for historical rates is possible but low priority — cross-temporal analysis is where incumbents have a structural advantage and we should not compete on that axis initially.
- **Relationships.** Raftelis conducts rate studies *for* utilities. Bluefield's clients include Amazon, Xylem, and major engineering firms. These relationships took decades to build and generate proprietary insights.
- **Analyst interpretation.** Bluefield and Raftelis don't just collect data — they publish analysis, forecasts, and strategic commentary. We produce structured data, not narrative intelligence.

### What we have that the incumbents don't
- **Geographic resolution.** Nobody has wired rate data to EPA service area polygons. You can't query by lat/long in any existing product. We can.
- **Coverage breadth.** Raftelis: 500. Bluefield: 61. Circle of Blue: 30. We target 10,000+ through LLM-agent scraping. The bottom 90% of US water systems are invisible to every existing product.
- **Programmatic access.** No existing rate data product exposes an API. Every competitor serves dashboards, PDFs, and subscription portals designed for human analysts. We serve structured JSON for both humans and machines.
- **Refresh velocity.** Competitors update annually or semi-annually. Our change-detection pipeline re-parses within weeks of a utility posting new rates.
- **Agent-native delivery.** An MCP server means any LLM agent can use our data as a tool. This distribution channel doesn't exist for any competitor and has zero customer acquisition cost.

### Where we coexist
The incumbents serve Fortune 500 water strategy teams, utility boards, and engineering firms who want analyst-curated intelligence with historical context. We serve developers, consultants doing site-specific due diligence, AI/LLM platforms needing structured utility context, and researchers who need breadth over depth. These buyer segments barely overlap. We complement rather than displace the incumbents — and our dataset could eventually be *licensed to* them to extend their coverage.

---

## Database Strategy

### Schema Core

```
water_systems
├── pwsid (PK, from EPA/SDWIS)
├── utility_name
├── state
├── geometry (PostGIS polygon, from EPA CWS boundaries)
├── centroid_lat, centroid_lng
├── population_served (from SDWIS)
├── source_water_type (from SDWIS — groundwater/surface/purchased)
├── owner_type (from SDWIS — municipal/private/federal/etc)
├── sdwa_violations_5yr (from SDWIS)
└── last_updated

rate_schedules
├── pwsid (FK → water_systems)
├── effective_date
├── next_scheduled_change
├── billing_frequency
├── customer_class (residential/commercial/industrial)
├── rate_structure_type (classification)
├── fixed_charges (JSONB — array of {name, amount, frequency})
├── volumetric_tiers (JSONB — array of {tier, min_gal, max_gal, rate_per_1000})
├── surcharges (JSONB — array of {name, rate, condition})
├── cost_at_5ccf (derived — monthly cost at 3,740 gal)
├── cost_at_10ccf (derived — monthly cost at 7,480 gal)
├── cost_at_20ccf (derived — monthly cost at 14,960 gal)
├── conservation_signal (derived — ratio of highest to lowest tier rate)
├── source_url
├── scrape_timestamp
├── confidence_score
└── needs_review (boolean)
```

### Key design decisions:

**JSONB for rate components.** Rate structures vary wildly (1 tier to 7 tiers, 0 to 5 surcharges, seasonal vs. flat vs. budget-based). JSONB in Postgres lets us store heterogeneous structures without schema explosions while still supporting indexed queries.

**Derived cost benchmarks stored, not just computed.** The `cost_at_5ccf`, `cost_at_10ccf`, `cost_at_20ccf` columns are computed from the tier data and stored. This enables fast comparison queries ("rank all VA utilities by cost at 10 CCF") without tier math at query time. Recomputed on any rate schedule update.

**Provenance is non-negotiable.** Every rate record traces to a source URL and scrape timestamp. Consultant and law firm buyers need this for citations. Agent consumers need this for reliability assessment. The confidence score (0–1) is set by the LLM parser based on extraction clarity.

**Customer class as a dimension, not a filter.** A utility may have different rate schedules for residential, commercial, and industrial customers. Each is a separate row in `rate_schedules` with the same PWSID. Industrial rates (especially for cooling water users) are the highest-value data for data center siting but the hardest to find — many utilities don't publish them online.

### Spatial indexing

PostGIS GIST index on `water_systems.geometry` enables sub-second `ST_Contains(geometry, ST_MakePoint(lng, lat))` queries. This is the core of `/resolve` — a spatial point-in-polygon lookup against 44,000 service area boundaries.

For `/site-report` with regulatory overlay, a second spatial query finds permits within a configurable radius using `ST_DWithin`. This hits state DEQ permit layers loaded as separate tables.

---

## What This Is Not

This is not a water governance assessment tool. The WRI/Kölbel/Strong MPWM framework identifies five pillars (information access, infrastructure, allocations/caps, pricing, crisis response). We cover pricing. The other four pillars require qualitative assessment that can't be reliably extracted from public websites by LLM agents — they need survey data, expert judgment, or field validation. The MPWM framework is a useful north star for what a *complete* water management dataset would contain, and Layers 1–3 are the foundation that the remaining pillars could eventually be built on.

This is not a competitor to Bluefield or Raftelis. They sell analyst-curated intelligence to enterprise water strategy teams. We sell structured data at scale to developers, consultants, and machines. The products address different buyer needs at different price points through different channels.

This is not a historical rates database. The incumbents own temporal depth. We own spatial breadth and programmatic access. Over time we accumulate history as a byproduct of ongoing collection, but we don't backfill aggressively.

---

## Guiding Principles

1. **Geography first, intelligence second.** The spatial index (Layer 1) is the foundation everything else attaches to. It's free, it's complete, and it's the integration layer nobody else has built. Every enrichment — rates, permits, risk scores, governance — joins to PWSIDs resolved through geography.

2. **Breadth over depth.** 10,000 utilities at 80% data completeness beats 500 utilities at 100%. The long tail of small and medium systems is where existing products have zero coverage and where marginal value per additional system is highest.

3. **Structured data, not narrative.** We output JSON with provenance, not analyst reports with opinions. Consumers (human or machine) interpret the data in their own context. This keeps the product simple, the liability low, and the automation high.

4. **Freshness through automation, not labor.** Change detection + LLM re-parsing means ongoing maintenance scales logarithmically with coverage, not linearly. Adding the 10,000th utility shouldn't cost more to maintain than the 1,000th.

5. **Product B is Product A with an address bar.** The API and the dataset are the same asset. The API is how real-time consumers access it. The dataset export is how batch consumers access it. Neither is primary — the underlying database is.

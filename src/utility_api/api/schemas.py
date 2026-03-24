#!/usr/bin/env python3
"""
API Response Schemas

Purpose:
    Pydantic models for API request validation and response serialization.
    Used by the /resolve endpoint and future endpoints.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - pydantic
"""

from datetime import date

from pydantic import BaseModel, Field


class ResolveResponse(BaseModel):
    """Response from the /resolve endpoint."""

    # Query echo
    lat: float
    lng: float

    # Match indicators
    cws_match: bool = Field(description="Whether a CWS boundary was found for this location")
    aqueduct_match: bool = Field(description="Whether an Aqueduct polygon was found")

    # CWS identity
    pwsid: str | None = Field(None, description="EPA Public Water System ID")
    pws_name: str | None = Field(None, description="Water system name")
    state_code: str | None = Field(None, description="State code (2-letter)")
    county_served: str | None = None
    population_served: int | None = Field(None, description="Population served by this system")

    # SDWIS enrichment
    system_type: str | None = Field(None, description="CWS/TNCWS/NTNCWS")
    water_source: str | None = Field(None, description="GW (groundwater), SW (surface water)")
    owner_type: str | None = Field(None, description="F/S/L/P/N (Federal/State/Local/Private/NA)")
    service_connections: int | None = None
    is_wholesaler: str | None = None
    activity_status: str | None = None
    violation_count_5yr: int | None = Field(None, description="Total SDWA violations in last 5 years")
    health_violation_count_5yr: int | None = Field(None, description="Health-based violations in last 5 years")
    last_violation_date: date | None = None

    # MDWD financials (nullable — only ~2,200 systems nationally)
    mdwd_available: bool = Field(False, description="Whether MDWD financial data exists")
    mdwd_year: int | None = Field(None, description="Year of MDWD data")
    # Rate data
    has_rate_data: bool = Field(False, description="Whether parsed rate data exists for this utility")
    rate_source: str | None = Field(None, description="Source of best estimate: swrcb_ear_2022, owrs, scraped_llm")
    best_estimate_bill_10ccf: float | None = Field(None, description="Best-estimate monthly bill at 10 CCF ($/month)")
    best_estimate_fixed_charge: float | None = Field(None, description="Best-estimate monthly fixed/service charge ($/month)")
    best_estimate_rate_structure: str | None = Field(None, description="Rate structure: uniform, increasing_block, etc.")
    rate_effective_date: date | None = Field(None, description="Effective date of the selected rate schedule")
    rate_confidence: str | None = Field(None, description="Confidence in best estimate: high, medium, low")
    rate_n_sources: int | None = Field(None, description="Number of independent data sources for this utility")
    median_household_income: float | None = Field(None, description="Median household income for service area")
    pct_below_poverty: float | None = Field(None, description="Percent of population below poverty line")
    water_utility_revenue: float | None = Field(None, description="Water utility revenue (CPI-adjusted, from Census of Governments)")
    water_utility_expenditure: float | None = Field(None, description="Water utility total expenditure (CPI-adjusted)")
    water_utility_debt: float | None = Field(None, description="Total debt outstanding (CPI-adjusted)")
    mdwd_population: int | None = Field(None, description="MDWD census population for service area")

    # Aqueduct water risk
    aqueduct_id: str | None = Field(None, description="Aqueduct polygon composite key")
    water_stress_score: float | None = Field(None, description="Baseline water stress (0-5)")
    water_stress_label: str | None = Field(None, description="Low / Medium-High / High / Extremely High")
    water_depletion_score: float | None = None
    drought_risk_score: float | None = None
    interannual_variability: float | None = None
    seasonal_variability: float | None = None
    overall_water_risk: float | None = None

    model_config = {"json_schema_extra": {"example": {
        "lat": 38.8951,
        "lng": -77.0364,
        "cws_match": True,
        "aqueduct_match": True,
        "pwsid": "VA0001234",
        "pws_name": "Example Water Authority",
        "state_code": "VA",
        "population_served": 125000,
        "system_type": "CWS",
        "water_source": "SW",
        "water_stress_score": 2.3,
        "water_stress_label": "Medium-High",
    }}}


class PermitRecord(BaseModel):
    """Single permit in the /permits response."""

    source: str = Field(description="Data source: va_deq_vwp, va_deq_vpdes, ca_swrcb_ewrims")
    permit_number: str = Field(description="State-assigned permit/application ID")
    facility_name: str | None = Field(None, description="Facility or owner name")
    source_category: str | None = Field(None, description="Category as delivered by data provider")
    category_group: str | None = Field(None, description="Normalized bucket: industrial, energy, municipal, etc.")
    use_codes: list[str] | None = Field(None, description="List of use codes (CA multi-use rights)")
    status: str | None = Field(None, description="Permit status")
    state_code: str | None = None
    county: str | None = None
    issued_date: date | None = None
    expiration_date: date | None = None
    face_value_amount: float | None = Field(None, description="Permitted volume")
    face_value_units: str | None = Field(None, description="Units for face_value_amount")
    max_diversion_rate: float | None = Field(None, description="Max direct diversion rate")
    max_diversion_units: str | None = Field(None, description="Units for max_diversion_rate")
    max_diversion_rate_gpd: float | None = Field(None, description="Max diversion rate normalized to gallons per day")
    lat: float | None = None
    lng: float | None = None
    distance_km: float = Field(description="Distance from query point in km")


class PermitsResponse(BaseModel):
    """Response from the /permits endpoint."""

    query_lat: float
    query_lng: float
    radius_km: float
    total_results: int = Field(description="Number of permits found within radius")
    permits: list[PermitRecord]

    model_config = {"json_schema_extra": {"example": {
        "query_lat": 38.8951,
        "query_lng": -77.0364,
        "radius_km": 10.0,
        "total_results": 3,
        "permits": [{
            "source": "va_deq_vwp",
            "permit_number": "21-0533",
            "facility_name": "Microsoft Corporation - Timber Data Center",
            "source_category": "Data Center",
            "category_group": "industrial",
            "status": "active",
            "state_code": "VA",
            "county": "Mecklenburg County",
            "distance_km": 2.3,
        }],
    }}}


class PermitXrefRecord(BaseModel):
    """Permit linked to a facility via cross-reference, with match metadata."""

    source: str
    permit_number: str
    facility_name: str | None = None
    source_category: str | None = None
    category_group: str | None = None
    use_codes: list[str] | None = None
    status: str | None = None
    state_code: str | None = None
    county: str | None = None
    issued_date: date | None = None
    expiration_date: date | None = None
    face_value_amount: float | None = None
    face_value_units: str | None = None
    max_diversion_rate: float | None = None
    max_diversion_units: str | None = None
    max_diversion_rate_gpd: float | None = None
    lat: float | None = None
    lng: float | None = None
    match_type: str = Field(description="How the link was established: spatial_match, manual")
    match_distance_km: float | None = Field(None, description="Distance between permit and facility centroids")
    match_confidence: str | None = Field(None, description="high (<1km), medium (1-5km), low (>5km)")


class FacilityPermitsResponse(BaseModel):
    """Response from the /facility/{id}/permits endpoint."""

    facility_id: str
    facility_name: str | None = None
    operator: str | None = None
    state_code: str | None = None
    linked_permits: list[PermitXrefRecord] = Field(description="Permits directly cross-referenced to this facility")
    nearby_permits: list[PermitRecord] = Field(description="Other permits within search radius")
    radius_km: float

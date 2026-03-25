# City Data Assessment
## EPA Envirofacts / ECHO SDWIS Field Verification

**Date:** 2026-03-25
**Author:** Research session (automated)
**Purpose:** Confirm availability of city name field for SDWIS ingest module update

---

### Envirofacts API: ✅ YES

**Field name:** `city_name`

**Verification method:** Direct API query to
`https://data.epa.gov/efservice/WATER_SYSTEM/STATE_CODE/VA/rows/0:5/JSON`

The `WATER_SYSTEM` table returns `city_name` as a standard field in every record.
Note: this is the **mailing address city**, not necessarily the city served.

**Sample values from VA query:**

| PWSID | PWS Name | city_name |
|-------|----------|-----------|
| AR0000872 | ROYAL OAKS MOBILE HOME PARK | RICHMOND |
| AZ0408166 | POWELL LOADING STATION | FAIRFAX |
| AZ0420116 | LUKEVILLE BORDER STATION | LEESBURG |
| CA3301717 | VALLEY VIEW TRAILER PARK | HOPEWELL |
| CA5800803 | LOMA RICA WATER COMPANY | BEDFORD |
| CT0189853 | 1112 FEDERAL ROAD | DRAPER |

**Important caveat:** The `STATE_CODE/VA` filter returns systems whose
**mailing address** is in VA, not necessarily systems whose PWSID starts
with `VA`. The sample above includes PWSIDs from AR, AZ, CA, and CT that
happen to have VA mailing addresses. The `state_code` field in the response
reflects the mailing address state. For PWSID-based state filtering, use the
first two characters of the `pwsid` field.

### ECHO Bulk Download: ✅ YES (expected)

The ECHO SDWA bulk download (`SDWA_PUB_WATER_SYSTEMS.csv`) at
https://echo.epa.gov/tools/data-downloads includes address fields.
Based on the Envirofacts schema (which shares the same underlying SDWIS
data model), the column name is expected to be `CITY_NAME` or equivalent.

**Note:** The ECHO bulk download was not directly verified in this session
due to the large file size. The Envirofacts API result is sufficient to
confirm the field exists in the SDWIS data model.

### Other Address Fields Available

The Envirofacts API response also includes these address fields that may
be useful for geocoding or domain guessing:

- `address_line1` — Street address
- `address_line2` — Secondary address line
- `city_name` — City
- `state_code` — State (2-letter, mailing address state)
- `zip_code` — ZIP code
- `country_code` — Country (US)

### Recommended Action

Add `city_name` to the `sdwis.py` ingest module as column `city`.

```python
# In sdwis.py, add to the column mapping:
COLUMN_MAP = {
    # ... existing columns ...
    'city_name': 'city',
}
```

This will make city available to the DomainGuesser agent for pattern
generation (e.g., `{city}{state}.gov`, `cityof{city}.org`).

### Additional Consideration

The `city_name` field represents the **utility mailing address**, which
for small private systems may be the owner's home address rather than
the community served. For municipal systems, the mailing address city
typically matches the service area. For the domain guessing use case,
the mailing city is a reasonable proxy — municipal utilities almost
always have a mailing address in the city they serve.

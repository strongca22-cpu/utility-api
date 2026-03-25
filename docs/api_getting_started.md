# Utility Intelligence API — Getting Started

## Get an API Key

API keys are created via the CLI (no self-service registration yet):

```bash
ua-ops create-api-key --name "your-name" --tier basic
```

Tiers:
- **free**: 100 requests/day
- **basic**: 1,000 requests/day
- **premium**: 10,000 requests/day

Save the key — it's shown once and cannot be recovered.

## Authentication

Pass your key via the `X-API-Key` header on every request:

```bash
curl -H "X-API-Key: ua-key-YOUR-KEY-HERE" http://localhost:8000/resolve?lat=38.85&lng=-77.35
```

The `/health` endpoint does not require authentication.

## Endpoints

### `GET /resolve` — Geographic Utility Lookup

Given lat/lng coordinates, returns the water utility serving that location with SDWIS metadata, financial data, rate estimates, and water stress risk.

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/resolve?lat=38.8951&lng=-77.0364"
```

### `GET /rates/{pwsid}` — Rate Details

Full rate schedule for a specific utility, including tier structure, fixed charges, and bill benchmarks.

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/rates/VA4760100"
```

### `GET /rates/best-estimate` — Rate Comparison

List best-estimate rates for all utilities. Filterable by state and confidence.

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/rates/best-estimate?state=VA"
```

### `GET /bulk-download` — Dataset Export

Download the full rate dataset as CSV or GeoJSON. One row per PWSID with utility identity, rate data, and centroid coordinates.

```bash
# CSV
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/bulk-download?state=VA&format=csv" > va_rates.csv

# GeoJSON
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/bulk-download?format=geojson" > rates.geojson
```

### `GET /health` — Health Check (no auth)

```bash
curl http://localhost:8000/health
```

## Interactive Documentation

When the API is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI spec**: http://localhost:8000/openapi.json

## Rate Limit Headers

When you exceed your daily limit, the API returns HTTP 429 with a `Retry-After` header.

## MCP Server

For MCP-compatible agents (Claude Desktop, etc.), a Model Context Protocol server is available:

```bash
ua-mcp  # Runs as stdio MCP server
```

Tools exposed:
- `resolve_water_utility(latitude, longitude)` — same as `/resolve`
- `get_utility_details(pwsid)` — same as `/rates/{pwsid}` + SDWIS details

Claude Desktop config (`~/.claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "utility-api": {
      "command": "ua-mcp"
    }
  }
}
```

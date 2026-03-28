# Desktop SearXNG Recovery Runbook

**Created:** 2026-03-28
**Sprint:** 22
**Purpose:** Procedure for re-enabling the desktop SearXNG instance after IP cooldown.

---

## Current State (2026-03-28)

- Desktop SearXNG (`localhost:8888`) is running but **not used by the pipeline**
- Pipeline config (`agent_config.yaml`) points to VPS (`localhost:8889`)
- Desktop IP was burned from earlier heavy usage — currently in cooldown
- DDG intermittently CAPTCHA-gating on desktop

## Before Re-Enabling

- [ ] Confirm cooldown period elapsed (minimum 7 days from last block signal)
- [ ] Do NOT use desktop for general browsing or other search traffic during recovery
- [ ] Set desktop SearXNG to ONLY serve the pipeline — no other consumers

## Desktop Engine Config (Post-Recovery)

Enable:
  - `google` (weight: 3) — PRIMARY reason to use desktop. VPS cannot run Google.
  - `duckduckgo` (weight: 1)
  - `startpage` (weight: 1) — was working on desktop before cooldown

Disable:
  - `brave` — traffic already served by VPS
  - `bing` — traffic already served by VPS
  - `mojeek` — traffic already served by VPS
  - `aol` — traffic already served by VPS

## Desktop Throttle (STRICT — Do Not Relax)

```yaml
# In agent_config.yaml — desktop-specific section (not yet implemented)
desktop_discovery:
  searxng_url: "http://localhost:8888/search"
  delay_between_queries: 25      # seconds
  delay_between_utilities: 60    # seconds
  max_utilities_per_session: 8
  total_query_budget: 25
  session_cooldown: 4 hours      # minimum between sessions
```

## Desktop Usage Pattern

- Only process PWSIDs where VPS returned 0 URLs above threshold
- Run 1 session per 4 hours maximum
- Monitor Google error log after every session
- If any CAPTCHA or block signal → stop immediately, extend cooldown 7 days

## Expected Output

~30-40 PWSIDs/day on Google alone, targeting the hard cases VPS missed.

## Switching Pipeline to Desktop

When ready, update `config/agent_config.yaml`:

```yaml
discovery:
  searxng_url: "http://localhost:8888/search"  # was 8889 (VPS)
```

Or implement dual-instance support: try VPS first, fall back to desktop for zero-result PWSIDs.

## Monitoring

After each desktop session, check:
```bash
# Local SearXNG logs
docker logs searxng-searxng-1 2>&1 | grep -i 'captcha\|block\|403\|error' | tail -10

# Google-specific errors
docker logs searxng-searxng-1 2>&1 | grep -i 'google' | tail -10
```

If any CAPTCHA or block → disable Google, extend cooldown.

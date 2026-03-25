# UAPI — Implementation Guide (v2 Architecture Companion)

**Purpose**: This document annotates the v2 architecture with implementation-level precision. It exists to prevent overengineering. Read this before building any agent.

**Rule**: If a task doesn't require natural language understanding, don't use an LLM. "Agent" is an architectural role, not an implementation prescription.

---

## The Critical Distinction

The v2 architecture defines 5 agent roles. Three are **deterministic automation** (Python + SQL). Two use **LLM inference**. The BaseAgent interface is shared, but the implementations are fundamentally different:

| Agent | Type | LLM? | Implementation |
|-------|------|------|----------------|
| Orchestrator | **Deterministic** | No | Python function + SQL queries + config file |
| Discovery | **LLM-assisted** | Yes (Haiku, low volume) | SearXNG search + LLM relevance scoring |
| Scrape | **Deterministic** | No | requests + Playwright + pymupdf |
| Parse | **LLM-powered** | Yes (Haiku/Sonnet, routed) | Claude API structured extraction |
| Best Estimate | **Deterministic** | No | Python function + SQL + config-driven priority |

---

## Agent Implementation Specs

### Orchestrator Agent

**What to build**: A Python class with a `run()` method that executes SQL queries against `source_catalog`, `pwsid_coverage`, and `scrape_registry`, applies priority logic from `config/source_priority.yaml`, and returns a ranked list of tasks.

**What NOT to build**: No LLM calls. No prompt templates. No token tracking. No retry logic for API calls. No `anthropic` import.

**Core logic** (pseudocode):
```python
class OrchestratorAgent:
    def run(self) -> list[Task]:
        tasks = []
        
        # 1. Check bulk sources for new vintages
        stale_sources = db.query("""
            SELECT * FROM source_catalog 
            WHERE next_check_date <= CURRENT_DATE
        """)
        for source in stale_sources:
            tasks.append(Task(type='check_bulk_source', source_key=source.source_key))
        
        # 2. Find high-priority PWSIDs without rate data
        gaps = db.query("""
            SELECT pwsid, state, population_served, priority_tier
            FROM pwsid_coverage
            WHERE has_rate_data = FALSE
            ORDER BY priority_tier ASC, population_served DESC
            LIMIT :batch_size
        """, batch_size=config.orchestrator.batch_size)
        for gap in gaps:
            tasks.append(Task(type='discover_and_scrape', pwsid=gap.pwsid))
        
        # 3. Find stale URLs due for re-check
        stale_urls = db.query("""
            SELECT * FROM scrape_registry
            WHERE status = 'active' 
            AND last_fetch_at < NOW() - INTERVAL ':freshness_days days'
        """, freshness_days=config.orchestrator.freshness_days)
        for url in stale_urls:
            tasks.append(Task(type='change_detection', registry_id=url.id))
        
        # 4. Find retriable failures
        retriable = db.query("""
            SELECT * FROM scrape_registry
            WHERE status = 'pending_retry'
            AND retry_after <= NOW()
            ORDER BY retry_count ASC
            LIMIT :retry_batch
        """, retry_batch=config.orchestrator.retry_batch)
        for url in retriable:
            tasks.append(Task(type='retry_scrape', registry_id=url.id))
        
        return sorted(tasks, key=lambda t: t.priority)
```

**Lines of code**: ~100–150. One file. No external dependencies beyond SQLAlchemy and PyYAML.

**There is one narrow exception** where an LLM might help: checking whether a bulk source webpage has a new vintage available (e.g., "has HydroShare published eAR 2023 yet?"). This is a nice-to-have. For Sprint 13, implement it as a simple HTTP GET + regex check for year strings. If that proves brittle later, swap in a Haiku call. Don't pre-build the LLM path.

---

### Discovery Agent

**What to build**: A Python class that takes a PWSID + utility name, runs a SearXNG search, and scores the returned URLs for relevance. The scoring step uses Haiku.

**What NOT to build**: No Sonnet. No Opus. No extended thinking. No complex prompt chains. The LLM call is a single-turn classification: "Is this URL a water rate page? Score 0–100."

**The LLM call** (the only one in this agent):
```python
# Input: ~200 tokens (URL + title + snippet)
# Output: ~50 tokens (score + one-line rationale)
# Model: Haiku 4.5
# Cost: ~$0.0003 per call

prompt = f"""Score this URL's relevance as a water utility rate page (0-100).
Utility: {utility_name}, {state}
URL: {url}
Title: {title}
Snippet: {snippet}

Respond with only: SCORE: [number] REASON: [one line]"""
```

**Volume**: ~5 URLs scored per PWSID. At 50 PWSIDs/batch = 250 calls = $0.02.

**Self-host path**: This exact prompt works with Llama 3.1 8B. When volume exceeds ~2,000 calls/week, swap the `anthropic` client for a local inference call. The agent interface doesn't change.

**Alternative (no LLM at all)**: A keyword scoring function handles ~70% of cases correctly:
```python
def score_url_relevance(url: str, title: str, snippet: str) -> int:
    score = 0
    text = f"{url} {title} {snippet}".lower()
    for keyword in ["rate", "schedule", "tariff", "water bill", "fee schedule", 
                     "rate structure", "charges", "pricing"]:
        if keyword in text: score += 15
    for negative in ["meeting", "agenda", "minutes", "news", "press", 
                      "election", "job", "career"]:
        if negative in text: score -= 20
    return max(0, min(100, score))
```

**Recommendation**: Start with the keyword scorer. Add Haiku only for PWSIDs where the keyword scorer returns ambiguous results (score 30–60). This eliminates ~80% of LLM calls.

---

### Scrape Agent

**What to build**: A Python class that fetches a URL, extracts text content, computes a content hash, and updates the scrape registry.

**What NOT to build**: No LLM. No AI. This is an HTTP client with PDF handling.

**Core logic**:
```python
class ScrapeAgent:
    def run(self, registry_entry: ScrapeRegistryRow) -> ScrapeResult:
        url = registry_entry.url
        
        # Fetch
        if url.endswith('.pdf'):
            response = requests.get(url, timeout=30)
            text = extract_pdf_text(response.content)  # pymupdf
            content_type = 'pdf'
        else:
            try:
                response = requests.get(url, timeout=30)
                text = extract_html_text(response.text)  # BeautifulSoup
                content_type = 'html'
            except:
                # Fallback to Playwright for JS-rendered pages
                text = playwright_fetch(url)
                content_type = 'html_js'
        
        # Record
        content_hash = hashlib.sha256(text.encode()).hexdigest()
        content_changed = (content_hash != registry_entry.last_content_hash)
        
        # Update registry
        db.update(scrape_registry, id=registry_entry.id,
            last_fetch_at=now(),
            last_http_status=response.status_code,
            last_content_hash=content_hash,
            last_content_length=len(text),
            content_changed=content_changed,
            status='active' if response.ok else 'failed',
            failure_reason=None if response.ok else f'HTTP {response.status_code}')
        
        return ScrapeResult(text=text, content_type=content_type, changed=content_changed)
```

**Lines of code**: ~80–120. Standard library + requests + playwright + pymupdf. No AI dependencies.

**Retry logic**: Simple exponential backoff on HTTP errors. Implemented in the agent, not in an LLM.
```python
if response.status_code == 403:
    registry.retry_after = now() + timedelta(days=2 ** registry.retry_count)
    registry.retry_count += 1
    if registry.retry_count > 5:
        registry.status = 'dead'
```

---

### Parse Agent

**What to build**: A Python class that takes raw text, routes to the appropriate model based on a complexity heuristic, calls the Claude API with the rate extraction prompt, validates the response, and writes structured data to `rate_schedules`.

**This is the one agent that genuinely needs LLM infrastructure.** Build it properly.

**Key components**:
1. **Complexity router** (no LLM — pure text analysis):
```python
def route_model(text: str) -> str:
    length = len(text)
    tier_keywords = count_keywords(text, ["tier", "block", "step", "level"])
    complex_keywords = any(k in text.lower() for k in 
        ["budget-based", "drought", "seasonal", "surcharge", "cpuc"])
    
    if length > 10000 or tier_keywords > 6 or complex_keywords:
        return "sonnet-4-6"
    elif length > 3000 or tier_keywords > 3:
        return "haiku-4-5"  # could add extended thinking here
    else:
        return "haiku-4-5"
```

2. **System prompt** (cached — identical for every call, ~800 tokens). Use prompt caching. This saves 90% on the system prompt after the first call.

3. **Batch vs live routing**: All scheduled parsing uses Batch API (50% off). Only on-demand customer-triggered parses use live API.

4. **Response validation** (no LLM — structural checks):
```python
def validate_parse(result: dict) -> tuple[bool, list[str]]:
    errors = []
    if not result.get('volumetric_tiers'):
        errors.append('No tiers found')
    for tier in result.get('volumetric_tiers', []):
        rate = tier.get('rate_per_1000_gal', 0)
        if rate < 0.5 or rate > 30:
            errors.append(f'Tier rate {rate} outside sanity bounds')
    if result.get('confidence', 0) < 0.6:
        errors.append(f'Low confidence: {result["confidence"]}')
    return (len(errors) == 0, errors)
```

5. **Cost tracking**: Log input tokens, output tokens, model, and cost per call. Store on `scrape_registry.last_parse_cost_usd`.

**This agent has two dependencies**: `anthropic` (for API calls) and the system prompt text. Everything else is standard Python.

---

### Best Estimate Agent

**What to build**: A Python function that reads `source_priority.yaml`, queries `rate_schedules` for a set of PWSIDs, applies the merge hierarchy, and writes to `rate_best_estimate`.

**What NOT to build**: No LLM. No AI. This is config-driven business logic.

**Core logic**:
```python
class BestEstimateAgent:
    def run(self, pwsids: list[str] = None):
        config = load_yaml('config/source_priority.yaml')
        
        if pwsids is None:
            # Run for all PWSIDs with rate data
            pwsids = db.query("SELECT DISTINCT pwsid FROM rate_schedules")
        
        for pwsid in pwsids:
            state = get_state(pwsid)
            priority_order = config.get(state, config['default'])['priority_order']
            
            # Get all rate records for this PWSID
            records = db.query("""
                SELECT * FROM rate_schedules 
                WHERE pwsid = :pwsid 
                ORDER BY vintage_date DESC
            """, pwsid=pwsid)
            
            # Select best by priority
            best = None
            for source_key in priority_order:
                candidates = [r for r in records if r.source_key == source_key]
                if candidates:
                    best = candidates[0]  # newest vintage from this source
                    break
            
            if best is None and records:
                best = records[0]  # fallback: newest from any source
            
            if best:
                # Margin check against existing estimate
                existing = db.query("""
                    SELECT * FROM rate_best_estimate WHERE pwsid = :pwsid
                """, pwsid=pwsid)
                
                if existing and best.source_key != existing.source_key:
                    margin = abs(best.bill_10ccf - existing.bill_10ccf) / existing.bill_10ccf
                    if margin > config[state].get('margin_threshold', 0.20):
                        flag_for_review(pwsid, best, existing, margin)
                        continue
                
                upsert_best_estimate(pwsid, best)
```

**Lines of code**: ~80–100. SQLAlchemy + PyYAML. No AI dependencies.

**Trigger**: Runs after any `rate_schedules` insert/update. Can be called directly or wired to a database trigger / post-commit hook.

---

## BaseAgent Interface

The shared interface is minimal. Don't overengineer it.

```python
from abc import ABC, abstractmethod
from datetime import datetime

class BaseAgent(ABC):
    """All agents share this interface. Implementation varies radically."""
    
    agent_name: str  # e.g., 'orchestrator', 'parse'
    
    @abstractmethod
    def run(self, **kwargs) -> dict:
        """Execute the agent's task. Returns a result summary."""
        pass
    
    def log_run(self, status: str, detail: dict):
        """Write to ingest_log after every run."""
        db.insert(ingest_log, 
            source_key=self.agent_name,
            started_at=self._start_time,
            completed_at=datetime.utcnow(),
            status=status,
            **detail)
```

That's it. No task queues, no message buses, no celery, no async frameworks. Each agent is a Python class with a `run()` method. The CLI calls them. Cron calls the CLI. The orchestrator calls the others. Keep it simple.

---

## What Claude Code Should NOT Build

This list exists because the v2 architecture uses language that could be misinterpreted:

1. **Do not build an LLM chain or prompt template system for the orchestrator.** It's SQL queries and Python logic. The word "agent" in "OrchestratorAgent" means "autonomous software component," not "LLM agent."

2. **Do not build a message queue or task broker.** The orchestrator returns a Python list. The CLI iterates it. If you need async later, add it later. For Sprint 13 volumes (~100 tasks/day), a for-loop is correct.

3. **Do not build a vector store or RAG pipeline.** Nothing in this system requires semantic search. All lookups are by PWSID, by state, or by spatial query. These are SQL operations.

4. **Do not abstract the LLM client into a provider-agnostic wrapper** (yet). The parse agent calls the Anthropic API. The discovery agent calls the Anthropic API. Both use `anthropic.Anthropic()`. When the self-hosting transition happens, swap the client in those two agents. Don't pre-build an abstraction layer for a transition that's months away.

5. **Do not build a web dashboard** for coverage or pipeline health. A CLI command that prints a formatted table is correct for now. `ua-coverage-report` prints to stdout. `ua-pipeline-health` prints to stdout. No Flask, no React, no Grafana.

6. **Do not use LangChain, CrewAI, or any agent framework.** The agents are plain Python classes. The orchestration is a for-loop. The state is in PostgreSQL. Adding a framework adds dependency weight and abstraction overhead for zero benefit at this scale.

7. **Do not build the cron infrastructure in Sprint 12.** Sprint 12 builds agent interfaces and the first two agents. Sprint 14 adds cron. Don't pre-wire scheduling into the agent interface — a cron job calling `ua-run-orchestrator` is the correct first implementation.

---

## Build Order Reminder

From v2 architecture, with implementation annotations:

| Sprint | Build | LLM involved? |
|--------|-------|---------------|
| 10 | `source_catalog`, `pwsid_coverage`, SDWIS expansion, `ua-coverage-report` CLI | No |
| 11 | `rate_schedules` table, migration transform, best estimate generalization, API updates | No |
| 12 | `scrape_registry`, BaseAgent ABC, BulkIngestAgent (wrapper on existing modules), BestEstimateAgent | No |
| 13 | OrchestratorAgent (Python+SQL), DiscoveryAgent (SearXNG + optional Haiku), ScrapeAgent (HTTP), ParseAgent (Claude API) | Yes — Discovery + Parse only |
| 14 | Cron, change detection, Batch API routing, health monitoring | Minimal — re-parse only |
| 15 | API productization (auth, docs, MCP, caching, billing) | No |
| 16 | Coverage expansion, new bulk sources, pilot customers | Yes — parse agent on new states |

Sprints 10–12 involve zero LLM calls. Sprint 13 is the first sprint where `anthropic` gets imported. This is intentional. Build the machine first, then connect the AI.

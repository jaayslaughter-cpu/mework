```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches you how to contribute to the `mework` Python codebase, which is organized around a set of core logic, enrichment, orchestration, and integration layers. The repository is structured for rapid iteration on new features ("phases"), robust bugfixing in the core pipeline, and seamless integration with external APIs and database layers. You'll learn the project's coding conventions, how to implement new features or phases, fix bugs, update database schemas, and handle API proxy fallbacks, all following established workflows.

## Coding Conventions

- **File Naming:**  
  Use `snake_case` for all Python files.  
  *Example:*  
  ```plaintext
  tasklets.py, fangraphs_layer.py, nightly_recap.py
  ```

- **Import Style:**  
  Prefer **relative imports** within the package.  
  *Example:*  
  ```python
  from .prop_enrichment_layer import enrich_props
  from .fangraphs_layer import FangraphsCache
  ```

- **Export Style:**  
  Mixed: both explicit `__all__` and implicit exports are used.  
  *Example:*  
  ```python
  # Explicit
  __all__ = ["enrich_props", "FangraphsCache"]

  # Implicit (no __all__ defined)
  def enrich_props(...):
      ...
  ```

- **Commit Messages:**  
  - Freeform, but often start with `fix` or `refactor`.
  - Average length: ~82 characters.
  *Example:*  
  ```
  fix: correct grading logic for late games in tasklets.py
  refactor: move enrichment logic to prop_enrichment_layer.py
  ```

## Workflows

### Feature/Phase Release
**Trigger:** When developing or releasing a new feature or "Phase XX".  
**Command:** `/new-phase-feature`

1. Update or add logic in `tasklets.py` for the new feature or phase.
2. Update or add logic in `prop_enrichment_layer.py` and/or `fangraphs_layer.py` for data enrichment or feature vectors.
3. Update `orchestrator.py` to wire up the new feature or endpoint.
4. Optionally, update `calibration_layer.py` or `sportsbook_reference_layer.py` if odds or calibration logic is involved.

*Example: Adding a new phase to tasklets.py*
```python
def run_phase_17():
    # New feature logic here
    ...
```
*Example: Wiring up in orchestrator.py*
```python
from .tasklets import run_phase_17

def orchestrate():
    ...
    if phase == 17:
        run_phase_17()
```

---

### Bugfix or Hotfix: Core Pipeline
**Trigger:** When a bug is discovered in grading, recap, or alerting pipeline.  
**Command:** `/fix-pipeline-bug`

1. Identify and fix the bug in `tasklets.py`, `nightly_recap.py`, or `DiscordAlertService.py`.
2. If orchestration is affected, update `orchestrator.py`.
3. If related to season records or recap, update `season_record.py`.
4. Commit and merge the fix with a descriptive message.

*Example: Fixing a bug in nightly_recap.py*
```python
def generate_recap():
    # Fixed: handle empty game list
    if not games:
        return "No games today."
    ...
```

---

### Database Schema or Cache Update
**Trigger:** When a new database table or cache layer is needed, or schema needs to be updated.  
**Command:** `/new-db-table`

1. Create or update a migration SQL file in `migrations/` (e.g., `V28__fg_cache.sql`).
2. Update the corresponding Python layer (e.g., `fangraphs_layer.py`) to use the new or modified table.
3. Update `orchestrator.py` or other integration points to wire up the new schema.
4. Update `.env.example` if new environment variables are needed.

*Example: Migration file (migrations/V28__fg_cache.sql)*
```sql
CREATE TABLE fg_cache (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```
*Example: Using new table in fangraphs_layer.py*
```python
def cache_fangraphs_data(data):
    # Insert data into fg_cache table
    ...
```

---

### API Proxy Fallback Integration
**Trigger:** When a new proxy fallback or tiered API fetch logic is needed.  
**Command:** `/add-api-fallback`

1. Update `live_dispatcher.py` to add or modify proxy fallback logic.
2. Update `.env.example` to include new API keys or environment variables.
3. Update `orchestrator.py` if orchestration logic changes.
4. Update `fangraphs_layer.py` if caching or API fetch logic changes.

*Example: Adding fallback in live_dispatcher.py*
```python
def fetch_prizepicks_data():
    try:
        return fetch_from_primary_api()
    except Exception:
        return fetch_from_proxy_api()
```
*Example: Updating .env.example*
```env
PRIZEPICKS_PROXY_API_KEY=your-key-here
```

## Testing Patterns

- **Framework:** Unknown (not detected in codebase).
- **Test File Pattern:** Files named `*.test.ts` (TypeScript).
- **Note:** Python code may not have direct tests or may rely on external test runners or integration tests. If adding tests, follow the existing pattern or propose a new one (e.g., `test_*.py` for Python).

## Commands

| Command             | Purpose                                                      |
|---------------------|-------------------------------------------------------------|
| /new-phase-feature  | Start a new feature or phase release workflow               |
| /fix-pipeline-bug   | Initiate a bugfix or hotfix in the core pipeline            |
| /new-db-table       | Begin a database schema or cache update workflow            |
| /add-api-fallback   | Implement or update API proxy fallback integration          |
```
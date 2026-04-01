```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches you the core development patterns, coding conventions, and common workflows for contributing to the `mework` Python codebase. The repository is focused on data pipeline enhancements, bugfixes, database migrations, and robust API integrations, with a strong emphasis on modularity and maintainability. While no specific framework is used, the project is organized around clear layers and tasklets for extensibility.

## Coding Conventions

- **File Naming:**  
  Use `snake_case` for all Python files and modules.  
  _Example:_  
  ```
  fangraphs_layer.py
  prop_enrichment_layer.py
  tasklets.py
  ```

- **Import Style:**  
  Prefer **relative imports** within the package.  
  _Example:_  
  ```python
  from .fangraphs_layer import FangraphsLayer
  from . import tasklets
  ```

- **Export Style:**  
  Both explicit and implicit exports are used.  
  _Example:_  
  ```python
  # Explicit
  def some_function():
      pass

  __all__ = ['some_function']

  # Implicit (just defining functions/classes)
  class Tasklet:
      ...
  ```

- **Commit Message Patterns:**  
  - Use freeform messages, often prefixed with `fix` or `refactor`.
  - Reference the phase or area affected.
  - Keep messages concise but descriptive (average ~82 characters).
  _Example:_  
  ```
  fix: correct grading logic in tasklets.py for edge-case recaps
  refactor: phase 7 enrichment and add new data source to fangraphs_layer.py
  ```

## Workflows

### Feature Phase Development
**Trigger:** When a new feature phase is planned or a major enhancement is required  
**Command:** `/new-phase-feature`

1. Edit or enhance `fangraphs_layer.py` and/or `prop_enrichment_layer.py` to add new features or data sources.
2. Update `tasklets.py` to wire in new logic or processing steps.
3. Optionally modify related files (e.g., `calibration_layer.py`, `line_comparator.py`) if the phase requires.
4. Commit with a message referencing the phase number and a summary of changes.

_Example:_
```python
# In fangraphs_layer.py
def enrich_with_new_metric(data):
    # Add new metric calculation
    ...

# In tasklets.py
from .fangraphs_layer import enrich_with_new_metric

def run_phase_8():
    data = fetch_data()
    enriched = enrich_with_new_metric(data)
    ...
```
_Commit message:_  
`refactor: phase 8 - add new metric enrichment to fangraphs_layer and tasklets`

---

### Bugfix or Pipeline Fix
**Trigger:** When a bug is reported or an issue is detected in the pipeline's operation or output  
**Command:** `/bugfix-pipeline`

1. Identify the bug or issue and locate the relevant logic in `tasklets.py` or related files.
2. Edit `tasklets.py` to fix the bug (e.g., grading, recap, math, query filters).
3. If the bug affects other modules (e.g., `DiscordAlertService.py`, `calibration_layer.py`), update those as well.
4. Commit with a message referencing the fix and affected area.

_Example:_
```python
# In tasklets.py
def calculate_grade(score):
    if score < 0:
        return 0  # Fix: Prevent negative grades
    return score
```
_Commit message:_  
`fix: prevent negative grades in tasklets.calculate_grade`

---

### Database Schema or Migration Update
**Trigger:** When a new database table or cache is needed, or schema changes are required for a new feature  
**Command:** `/new-db-migration`

1. Create or edit migration SQL files (e.g., `migrations/V27__add_discord_sent.sql`, `migrations/V28__fg_cache.sql`).
2. Update `fangraphs_layer.py` or other relevant modules to use the new/updated tables.
3. Optionally update `.env.example` if new environment variables are required.
4. Commit with a message referencing the migration and affected features.

_Example:_
```sql
-- migrations/V28__fg_cache.sql
CREATE TABLE fg_cache (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```
```python
# In fangraphs_layer.py
def cache_fg_data(data):
    # Insert into fg_cache table
    ...
```
_Commit message:_  
`refactor: add fg_cache table and update fangraphs_layer to use caching`

---

### Proxy or API Fallback Implementation
**Trigger:** When a new fallback tier is needed for API reliability or to add a new proxy layer  
**Command:** `/add-api-fallback`

1. Edit `live_dispatcher.py` to add or update proxy/fallback logic.
2. Update `.env.example` to include new API keys or environment variables.
3. If caching is involved, update `fangraphs_layer.py` and add relevant migrations.
4. Commit with a message referencing the fallback/proxy and affected APIs.

_Example:_
```python
# In live_dispatcher.py
def fetch_data_with_fallback():
    try:
        return fetch_from_primary_api()
    except Exception:
        return fetch_from_secondary_api()
```
```env
# In .env.example
PRIZEPICKS_API_KEY=your-key-here
```
_Commit message:_  
`fix: add fallback to secondary API in live_dispatcher.py and update .env.example`

---

## Testing Patterns

- **Framework:** Unknown (not detected)
- **Test File Pattern:** Files are named with `.test.ts` extension, suggesting some TypeScript-based testing (possibly for a frontend or API contract).
- **Python Testing:** No explicit Python test framework detected. If adding tests, follow the convention of naming test files as `test_*.py` and use standard Python testing tools like `pytest` or `unittest`.

_Example:_
```python
# test_tasklets.py
def test_calculate_grade():
    assert calculate_grade(-5) == 0
    assert calculate_grade(10) == 10
```

## Commands

| Command            | Purpose                                                      |
|--------------------|--------------------------------------------------------------|
| /new-phase-feature | Start a new feature phase with enhancements or new data      |
| /bugfix-pipeline   | Fix bugs or issues in the pipeline or grading logic          |
| /new-db-migration  | Add or update database schema/migrations for new features    |
| /add-api-fallback  | Implement or update proxy/fallback logic for external APIs   |
```

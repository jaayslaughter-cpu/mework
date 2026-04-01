```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches you how to contribute effectively to the `mework` Python codebase. You'll learn the project's coding conventions, commit patterns, and the main workflows for adding features, extending the database, fixing bugs, wiring pipelines, and merging branches. The guide includes practical code and workflow examples, plus suggested `/commands` for common tasks.

## Coding Conventions

- **File Naming:**  
  Use `snake_case` for all Python files.  
  *Example:*  
  ```python
  # Good
  prop_enrichment_layer.py

  # Bad
  PropEnrichmentLayer.py
  ```

- **Import Style:**  
  Use **relative imports** within the package.  
  *Example:*  
  ```python
  from .fangraphs_layer import FangraphsLayer
  ```

- **Export Style:**  
  Mixed; both explicit `__all__` and implicit exports are used.  
  *Example:*  
  ```python
  # Explicit
  __all__ = ["FangraphsLayer", "PropEnrichmentLayer"]

  # Implicit (no __all__, all top-level symbols exported)
  ```

- **Commit Messages:**  
  - Freeform, but often start with `fix`, `refactor`, or `Phase XX:`
  - Average length: ~82 characters  
  *Example:*  
  ```
  fix: correct cache invalidation logic in fangraphs_layer.py
  Phase 105: xbh_per_game + SLG as TB/power prop features
  ```

## Workflows

### Feature Phase Release
**Trigger:** When introducing a new feature or major enhancement as part of a named "Phase" (e.g., "Phase 105: xbh_per_game + SLG as TB/power prop features").  
**Command:** `/new-phase-feature`

1. Edit or add logic in one or more of the following core pipeline files:
   - `fangraphs_layer.py`
   - `prop_enrichment_layer.py`
   - `calibration_layer.py`
   - `sportsbook_reference_layer.py`
   - `line_comparator.py`
   - `base_rate_model.py`
2. Update orchestration logic in `tasklets.py`.
3. Sometimes update `orchestrator.py` for API or pipeline wiring.
4. Commit with a message like:
   ```
   Phase 106: Add new player prop enrichment for walk rate
   ```
5. Test the new feature end-to-end.

---

### Database Schema and Cache Extension
**Trigger:** When adding/modifying database tables, migrations, or cache layers to support new features or performance improvements.  
**Command:** `/new-db-migration`

1. Create or edit migration SQL files in `migrations/` (e.g., `V28__fg_cache.sql`).
2. Update related Python data access layers (e.g., `fangraphs_layer.py` for cache logic).
3. Update `tasklets.py` if logic is affected (e.g., Discord sent flag).
4. Update `.env.example` if new environment variables are required.
5. Update `orchestrator.py` if pipeline wiring is needed.
6. Apply the migration and test.

*Example migration file:*
```sql
-- migrations/V28__fg_cache.sql
ALTER TABLE fangraphs_cache ADD COLUMN last_updated TIMESTAMP;
```

---

### Bugfix or Hotfix Pipeline
**Trigger:** When a bug is discovered in production or QA, especially in core pipeline logic.  
**Command:** `/bugfix`

1. Identify the bug in the relevant core file (e.g., `tasklets.py`, `calibration_layer.py`).
2. Fix the bug and update related files if necessary.
3. Commit with a message starting with `Fix:` or `Hotfix:`.
   ```
   Fix: handle NoneType in DiscordAlertService.py notifications
   ```
4. Test to confirm the fix.

---

### Multi-file Pipeline Wiring
**Trigger:** When integrating a new data source, enrichment, or agent into the main pipeline.  
**Command:** `/wire-pipeline`

1. Edit or add logic in `prop_enrichment_layer.py`, `fangraphs_layer.py`, or other data layers.
2. Update `orchestrator.py` and/or `tasklets.py` to wire the new logic into the pipeline.
3. Commit with a message referencing the pipeline change or phase.
   ```
   Wire new Statcast enrichment into main pipeline
   ```
4. Test the full pipeline for correct integration.

---

### Merge Pull Request Main Into Feature
**Trigger:** When synchronizing feature branches with main or completing a feature via pull request.  
**Command:** `/merge-main`

1. Run `git merge main` into the feature branch (or vice versa).
2. Resolve any conflicts, which may touch many files.
3. Commit with a message like:
   ```
   Merge branch 'main' into feature/phase-106
   ```
4. Run tests to ensure stability.

---

## Testing Patterns

- **Framework:** Unknown (not detected in Python codebase).
- **Test File Pattern:** Some tests may be written in TypeScript (`*.test.ts`), suggesting cross-language or integration tests.
- **Best Practice:** If adding tests, follow the pattern of placing them in files ending with `.test.ts` or in a dedicated test directory.

*Example test file name:*
```
player_stats_enrichment.test.ts
```

## Commands

| Command            | Purpose                                                      |
|--------------------|--------------------------------------------------------------|
| /new-phase-feature | Start a new feature or major enhancement phase               |
| /new-db-migration  | Add or modify a database schema or cache layer               |
| /bugfix            | Apply a bugfix or urgent hotfix to the pipeline              |
| /wire-pipeline     | Wire up new data sources, enrichments, or agent logic        |
| /merge-main        | Merge main into a feature branch or complete a pull request  |
```

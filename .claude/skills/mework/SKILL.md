```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill teaches the core development and maintenance patterns for the `mework` Python codebase. The repository implements a data pipeline for statistical enrichment, feature engineering, and recap/settlement logic, with a focus on modularity and maintainability. You'll learn the project's coding conventions, commit practices, and the main workflows for adding features, fixing bugs, and enhancing the pipeline.

## Coding Conventions

- **File Naming:**  
  All Python files use `snake_case`.  
  *Example:*  
  ```
  prop_enrichment_layer.py
  nightly_recap.py
  ```

- **Import Style:**  
  Relative imports are preferred within modules.  
  *Example:*  
  ```python
  from .fangraphs_layer import fetch_fangraphs_data
  ```

- **Export Style:**  
  Both explicit (`__all__`) and implicit exports are used depending on the file.

- **Commit Messages:**  
  - Prefixes: `feat`, `fix`, `refactor`
  - Freeform descriptive messages, average length ~81 characters  
  *Example:*  
  ```
  feat: add new wOBA enrichment to prop_enrichment_layer and update tasklets
  fix: correct math error in calibration_layer.py affecting win probability
  ```

## Workflows

### Feature Phase Development
**Trigger:** When adding a new feature, data signal, or phase to the pipeline  
**Command:** `/feature-phase-development`

1. Modify or add logic in `fangraphs_layer.py`, `prop_enrichment_layer.py`, and/or `tasklets.py` to implement the new feature.
2. Commit changes with a message referencing the phase or feature.
3. Optionally, merge the branch or pull request into `main`.

*Example:*
```python
# prop_enrichment_layer.py
def enrich_with_new_stat(df):
    df['new_stat'] = df['hits'] / df['at_bats']
    return df
```
```
feat: add new_stat enrichment to prop pipeline
```

---

### Bugfix or Pipeline Fix
**Trigger:** When fixing a bug or correcting pipeline behavior  
**Command:** `/bugfix-or-pipeline-fix`

1. Identify and fix the bug in the relevant file(s), commonly `tasklets.py`, `calibration_layer.py`, `DiscordAlertService.py`, or `orchestrator.py`.
2. Commit the fix, referencing the bug or issue in the commit message.
3. Optionally, merge the branch or pull request into `main`.

*Example:*
```python
# calibration_layer.py
def calibrate_probabilities(probs):
    # Fixed division by zero bug
    return [p / sum(probs) if sum(probs) != 0 else 0 for p in probs]
```
```
fix: handle division by zero in calibrate_probabilities
```

---

### Add New Prop Feature
**Trigger:** When adding a new statistical feature or enrichment to the prop pipeline  
**Command:** `/add-prop-feature`

1. Update `fangraphs_layer.py` and/or `prop_enrichment_layer.py` to calculate or fetch the new feature.
2. Update `tasklets.py` to ensure the new feature is integrated into the pipeline.
3. Commit changes with a message referencing the new feature.

*Example:*
```python
# fangraphs_layer.py
def fetch_new_metric():
    # logic to fetch new metric
    pass
```
```
feat: integrate new_metric into prop enrichment pipeline
```

---

### Recap or Settlement Fix
**Trigger:** When correcting or enhancing the recap/settlement process for bets  
**Command:** `/fix-recap-or-settlement`

1. Modify `tasklets.py`, `nightly_recap.py`, `season_record.py`, and/or `DiscordAlertService.py` to fix or enhance recap/settlement logic.
2. Commit changes with a message referencing recap, settlement, or grading.
3. Optionally, merge the branch or pull request into `main`.

*Example:*
```python
# nightly_recap.py
def generate_recap(results):
    # Improved grading logic
    pass
```
```
fix: improve grading logic in nightly recap
```

## Testing Patterns

- **Framework:** Unknown (no explicit framework detected)
- **File Pattern:** Test files use the `*.test.ts` pattern, suggesting some TypeScript-based tests may exist, possibly for frontend or integration layers.
- **Python Testing:** No explicit Python test framework detected. If adding tests, follow the snake_case naming and place them in appropriately named files (e.g., `test_tasklets.py`).

## Commands

| Command                     | Purpose                                                        |
|-----------------------------|----------------------------------------------------------------|
| /feature-phase-development  | Start a new feature or phase in the pipeline                   |
| /bugfix-or-pipeline-fix     | Fix a bug or pipeline issue                                    |
| /add-prop-feature           | Add a new statistical or enrichment feature to the prop pipeline|
| /fix-recap-or-settlement    | Fix or enhance recap/settlement logic                          |
```

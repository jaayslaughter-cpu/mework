```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches the core development patterns and workflows for contributing to the **mework** Python codebase. The repository implements a data pipeline with layered logic for enrichment, calibration, and alerting, and features regular enhancements, bugfixes, and documentation updates. The codebase follows clear conventions for file organization, commit structure, and workflow execution, ensuring maintainability and collaboration.

## Coding Conventions

### File Naming

- **Style:** `snake_case`
- **Examples:**
  - `fangraphs_layer.py`
  - `prop_enrichment_layer.py`
  - `tasklets.py`

### Import Style

- **Relative imports** are preferred.
- **Example:**
  ```python
  from .fangraphs_layer import compute_signals
  from . import calibration_layer
  ```

### Export Style

- **Mixed:** Some modules use explicit `__all__`, others rely on implicit exports.
- **Example:**
  ```python
  __all__ = ["compute_signals", "enrich_props"]
  ```

### Commit Patterns

- **Prefixes:** `feat`, `fix`, `refactor` (not strictly enforced)
- **Average message length:** ~81 characters
- **Examples:**
  - `feat(Phase 110): Add new statcast signals to enrichment layer`
  - `fix: Correct payout calculation in calibration_layer.py`
  - `refactor: Move alert logic to DiscordAlertService.py`

## Workflows

### Feature Phase Development
**Trigger:** When adding a new feature, signal, or enhancement to the pipeline  
**Command:** `/feature-phase-development`

1. Edit or add logic in `fangraphs_layer.py` and/or `prop_enrichment_layer.py` if the feature relates to enrichment or stats.
2. Update `tasklets.py` to wire up the new feature or support new pipeline steps.
3. Commit all related files together. Include the Phase number in the commit message for traceability.

**Example:**
```python
# In fangraphs_layer.py
def compute_new_signal(data):
    # Add new computation logic here
    return data["stat"] * 1.05

# In tasklets.py
from .fangraphs_layer import compute_new_signal

def run_pipeline():
    ...
    data["new_signal"] = compute_new_signal(data)
    ...
```
**Commit message:**  
`feat(Phase 112): Add compute_new_signal to fangraphs_layer and pipeline`

---

### Bugfix or Pipeline Fix
**Trigger:** When fixing a bug or correcting a calculation/logic error in the pipeline  
**Command:** `/bugfix-or-pipeline-fix`

1. Identify and fix the bug in the relevant Python file(s) (commonly `tasklets.py`, `calibration_layer.py`, `DiscordAlertService.py`).
2. If needed, update related files to ensure consistency (e.g., update both `calibration_layer.py` and `tasklets.py` for payout logic).
3. Commit the fix with a descriptive message (often includes `Fix`, `Hotfix`, or `bug`).

**Example:**
```python
# In calibration_layer.py
def calculate_payout(amount, odds):
    # Fixed division by zero error
    if odds == 0:
        return 0
    return amount * odds
```
**Commit message:**  
`fix: Prevent division by zero in payout calculation`

---

### Documentation and ECC Bundle Update
**Trigger:** When adding or updating ECC tool bundles, skills, or command documentation  
**Command:** `/ecc-bundle-update`

1. Edit or add markdown files in `.claude/commands/` (e.g., `bugfix-or-pipeline-fix.md`, `feature-phase-development.md`, `database-migration.md`).
2. Update or add `SKILL.md` in `.agents/skills/mework/` and/or `.claude/skills/mework/`.
3. Update configuration files like `.claude/ecc-tools.json` and `.claude/identity.json`.
4. Commit all related documentation/configuration files together.

**Example:**
- Edit `.claude/commands/feature-phase-development.md` to document a new command.
- Update `.claude/ecc-tools.json` to register the new command.

**Commit message:**  
`docs: Update ECC bundle and add new skill documentation for feature-phase-development`

---

### Merge Main into Feature Branch
**Trigger:** When updating a feature/fix branch with the latest changes from main  
**Command:** `/merge-main`

1. Run a merge from `main` into the current feature or fix branch.
2. Resolve any merge conflicts, especially in shared files (e.g., `fangraphs_layer.py`, `prop_enrichment_layer.py`, `tasklets.py`).
3. Commit the merge.

**Example:**
```bash
git checkout feature/phase-112
git merge main
# Resolve conflicts in fangraphs_layer.py if prompted
git add fangraphs_layer.py
git commit -m "Merge main into feature/phase-112"
```

---

## Testing Patterns

- **Framework:** Unknown (no standard Python test framework detected)
- **Test File Pattern:** `*.test.ts` (TypeScript-style test files, possibly for frontend or integration)
- **Note:** Python code may not have automated tests in this repo; consider adding `pytest` or similar for future coverage.

## Commands

| Command                       | Purpose                                                      |
|-------------------------------|--------------------------------------------------------------|
| /feature-phase-development    | Start a new feature or enhancement phase                     |
| /bugfix-or-pipeline-fix       | Fix bugs or logic errors in the pipeline                     |
| /ecc-bundle-update            | Update documentation, ECC tools, or skill bundles            |
| /merge-main                   | Merge latest main branch changes into your feature/fix branch |

```
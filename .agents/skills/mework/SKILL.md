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
This skill teaches the core development and maintenance patterns for the `mework` Python codebase. The repository implements a data pipeline for statistical enrichment, feature engineering, and recap/settlement logic, with a focus on modularity and maintainability. You'll learn the project's coding conventions, commit practices, and the main workflows for adding features, fixing bugs, and enhancing the pipeline.

## Coding Conventions

- **File Naming:**  
  Use `snake_case` for Python files and modules.  
  *Example:*  
  ```
  prop_enrichment_layer.py
  sportsbook_reference_layer.py
  ```

- **Import Style:**  
  Prefer relative imports within the package.  
  *Example:*  
  ```python
  from .fangraphs_layer import FangraphsLayer
  ```

- **Export Style:**  
  Mixed; both explicit and implicit exports are used.  
  *Example:*  
  ```python
  # Explicit
  __all__ = ["FangraphsLayer", "PropEnrichmentLayer"]

  # Implicit (default)
  def some_function():
      ...
  ```

- **Commit Messages:**  
  - Use prefixes: `feat`, `fix`, `refactor`
  - Messages are freeform but descriptive, averaging ~81 characters.  
  *Example:*  
  ```
  feat: add support for new sportsbook data source in enrichment layer
  fix: resolve bug in line comparator edge case handling
  refactor: streamline calibration layer for improved performance
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

### mework-ecc-bundle-addition
**Trigger:** When adding or updating the mework ECC bundle, agent, or skill definitions  
**Command:** `/add-ecc-bundle`

1. Add or update `.claude/commands/*.md` files (e.g., `bugfix-or-pipeline-fix.md`, `feature-development.md`)
2. Add or update `.claude/identity.json`
3. Add or update `.claude/ecc-tools.json`
4. Add or update skill documentation:  
   - `.claude/skills/mework/SKILL.md`  
   - `.agents/skills/mework/SKILL.md`
5. Optionally, update agent configuration files:  
   - `.codex/agents/*.toml`  
   - `.agents/skills/mework/agents/openai.yaml`
6. Commit changes with a descriptive message.
7. Open a pull request for review and merge.

*Example file addition:*
```shell
git add .claude/skills/mework/SKILL.md
git commit -m "feat: update ECC bundle and skill docs for new agent"
```

---

### feature-phase-development-or-bugfix
**Trigger:** When implementing a new feature, bugfix, or pipeline fix in the core codebase  
**Command:** `/feature-phase`

1. Modify relevant Python files, such as:
   - `fangraphs_layer.py`
   - `prop_enrichment_layer.py`
   - `tasklets.py`
   - `sportsbook_reference_layer.py`
   - `line_comparator.py`
   - `calibration_layer.py`
   - `DiscordAlertService.py`
2. Write clear, descriptive commit messages indicating the change type.
3. Optionally, merge from `main` to stay up to date.
4. Push your branch and open a pull request.
5. Merge into `main` after review.

*Example bugfix:*
```python
# line_comparator.py
def compare_lines(line1, line2):
    # Fixed edge case for negative lines
    if line1 is None or line2 is None:
        return None
    return abs(line1 - line2)
```
```shell
git commit -am "fix: handle NoneType in line comparator"
```
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

- **Framework:** Unknown (no standard Python test framework detected)
- **Test File Pattern:** `*.test.ts` (TypeScript-style test files, possibly for frontend or integration)
- **Note:** Python code may not have automated tests in this repo; consider adding `pytest` or similar for future coverage.

## Testing Patterns

- **Framework:** Unknown (no explicit framework detected)
- **File Pattern:** Test files use the `*.test.ts` pattern, suggesting some TypeScript-based tests may exist, possibly for frontend or integration layers.
- **Python Testing:** No explicit Python test framework detected. If adding tests, follow the snake_case naming and place them in appropriately named files (e.g., `test_tasklets.py`).

## Commands

| Command                       | Purpose                                                      |
|-------------------------------|--------------------------------------------------------------|
| /feature-phase-development    | Start a new feature or enhancement phase                     |
| /bugfix-or-pipeline-fix       | Fix bugs or logic errors in the pipeline                     |
| /ecc-bundle-update            | Update documentation, ECC tools, or skill bundles            |
| /merge-main                   | Merge latest main branch changes into your feature/fix branch |

```

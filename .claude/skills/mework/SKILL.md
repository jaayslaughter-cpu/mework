```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches you how to develop, extend, and maintain the `mework` Python codebase. It covers conventions for file structure, code style, commit messaging, and the main workflows for adding features, fixing bugs, and managing ECC bundles for agent orchestration. The repository is Python-based with no detected framework, and features a modular, layered pipeline architecture.
The `mework` repository is a Python codebase focused on data processing, enrichment, and agent-based automation. It features modular Python scripts, agent/skill definitions, and configuration files for integrating with the mework agent ecosystem. This skill teaches you the coding conventions, commit patterns, and standard workflows for contributing effectively to the project.

## Coding Conventions

- **File Naming:**  
  Use `snake_case` for all Python files.  
  _Example:_  
  ```
  sportsbook_reference_layer.py
  prop_enrichment_layer.py
  ```

- **Import Style:**  
  Use **relative imports** within modules.  
  _Example:_  
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
  Mixed: some modules use explicit `__all__`, others rely on implicit exports.

- **Commit Messages:**  
  - Use prefixes: `feat`, `fix`, `refactor`  
  - Messages are freeform, average ~81 characters  
  _Example:_  
  ```
  feat: add support for new sportsbook data source to enrichment layer
  fix: correct vector calculation in calibration layer
  refactor: move alert logic to DiscordAlertService
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
  ```

## Workflows

### Feature Development Phase Update
**Trigger:** When adding a new feature, phase, or significant logic update to the pipeline  
**Command:** `/feature-phase-update`

1. Edit or add logic in one or more of the following files:
    - `fangraphs_layer.py`
    - `prop_enrichment_layer.py`
    - `sportsbook_reference_layer.py`
    - `calibration_layer.py`
    - `DiscordAlertService.py`
2. Update `tasklets.py` to wire in new logic, features, or bugfixes.
3. Commit changes with a message referencing the phase/feature.

_Example:_
```python
# In prop_enrichment_layer.py
def enrich_props(props):
    # New feature logic here
    ...

# In tasklets.py
from .prop_enrichment_layer import enrich_props
# Integrate new feature into pipeline
```
_Commit message:_
```
feat: integrate new prop enrichment logic into pipeline
```

---

### ECC Bundle Addition
**Trigger:** When adding or updating the mework ECC bundle for agent/skill orchestration  
**Command:** `/ecc-bundle-add`

1. Add or update `.claude/commands/*.md` files for new commands (e.g., feature development, bugfix, migration).
2. Add or update `.claude/skills/mework/SKILL.md` and/or `.agents/skills/mework/SKILL.md`.
3. Add or update `.claude/ecc-tools.json` and `.claude/identity.json`.
4. Add or update `.codex/agents/*.toml` and/or `.agents/skills/mework/agents/openai.yaml`.
5. Commit all related files together.

_Example:_
```
# Add new command documentation
touch .claude/commands/feature-phase-update.md

# Update skill manifest
vim .claude/skills/mework/SKILL.md

# Commit
git add .claude/commands/feature-phase-update.md .claude/skills/mework/SKILL.md
git commit -m "feat: add ECC bundle for new feature-phase-update command"
```

---

### Bugfix or Pipeline Fix
**Trigger:** When fixing a bug or correcting a pipeline issue  
**Command:** `/bugfix`

1. Edit one or more of:
    - `tasklets.py`
    - `DiscordAlertService.py`
    - `sportsbook_reference_layer.py`
    - `calibration_layer.py`
    - etc.
2. Update `.claude/commands/bugfix-or-pipeline-fix.md` if relevant.
3. Commit changes with a message referencing the fix.

_Example:_
```python
# In calibration_layer.py
def calibrate_vector(vector):
    # Fix bug in calibration logic
    ...

# Update command documentation if needed
vim .claude/commands/bugfix-or-pipeline-fix.md
```
_Commit message:_
```
fix: correct calibration bug affecting feature vectors
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

### Recap or Settlement Fix
**Trigger:** When correcting or enhancing the recap/settlement process for bets  
**Command:** `/fix-recap-or-settlement`

- **Framework:** Unknown (not detected)
- **File Pattern:** Test files use the `*.test.ts` pattern, suggesting some JavaScript/TypeScript tests may exist alongside Python code.
- **Best Practice:** Place tests in files matching `*.test.ts` and ensure they cover new or changed functionality.

*Example test file name:*
```
line_comparator.test.ts
```
fix: improve grading logic in nightly recap
```

## Testing Patterns

- **Framework:** Unknown (not detected)
- **File Pattern:** `*.test.ts` (suggests some TypeScript tests, possibly for integrations or front-end)
- **Python Testing:** Not explicitly detected; if adding tests, follow Python conventions (e.g., `test_*.py` with `unittest` or `pytest`).

## Commands

| Command                | Purpose                                                        |
|------------------------|----------------------------------------------------------------|
| /feature-phase-update  | Add or update a feature, phase, or core logic in the pipeline  |
| /ecc-bundle-add        | Add or update ECC bundle, commands, and agent configs          |
| /bugfix                | Fix bugs or issues in the pipeline                             |
- **Framework:** Unknown (no explicit framework detected)
- **File Pattern:** Test files use the `*.test.ts` pattern, suggesting some TypeScript-based tests may exist, possibly for frontend or integration layers.
- **Python Testing:** No explicit Python test framework detected. If adding tests, follow the snake_case naming and place them in appropriately named files (e.g., `test_tasklets.py`).

## Commands

| Command            | Purpose                                                         |
|--------------------|-----------------------------------------------------------------|
| /add-ecc-bundle    | Add or update ECC bundle, agent, or skill definitions           |
| /feature-phase     | Start a new feature, bugfix, or pipeline development workflow   |

```

```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

This skill teaches you how to develop, extend, and maintain the `mework` Python codebase. It covers conventions for file structure, code style, commit messaging, and the main workflows for adding features, fixing bugs, and managing ECC bundles for agent orchestration. The repository is Python-based with no detected framework, and features a modular, layered pipeline architecture.

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
```

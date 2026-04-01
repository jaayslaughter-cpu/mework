```markdown
# mework Development Patterns

> Auto-generated skill from repository analysis

## Overview

The `mework` repository is a Python codebase focused on data processing, enrichment, and agent-based automation. It features modular Python scripts, agent/skill definitions, and configuration files for integrating with the mework agent ecosystem. This skill teaches you the coding conventions, commit patterns, and standard workflows for contributing effectively to the project.

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

## Testing Patterns

- **Framework:** Unknown (not detected)
- **File Pattern:** Test files use the `*.test.ts` pattern, suggesting some JavaScript/TypeScript tests may exist alongside Python code.
- **Best Practice:** Place tests in files matching `*.test.ts` and ensure they cover new or changed functionality.

*Example test file name:*
```
line_comparator.test.ts
```

## Commands

| Command            | Purpose                                                         |
|--------------------|-----------------------------------------------------------------|
| /add-ecc-bundle    | Add or update ECC bundle, agent, or skill definitions           |
| /feature-phase     | Start a new feature, bugfix, or pipeline development workflow   |

```
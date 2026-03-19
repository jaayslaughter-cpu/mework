---
name: mework-conventions
description: Development conventions and patterns for mework. Python project with conventional commits.
---

# Mework Conventions

> Generated from [jaayslaughter-cpu/mework](https://github.com/jaayslaughter-cpu/mework) on 2026-03-19

## Overview

This skill teaches Claude the development patterns and conventions used in mework.

## Tech Stack

- **Primary Language**: Python
- **Architecture**: hybrid module organization
- **Test Location**: separate

## When to Use This Skill

Activate this skill when:
- Making changes to this repository
- Adding new features following established patterns
- Writing tests that match project conventions
- Creating commits with proper message format

## Commit Conventions

Follow these commit message conventions based on 80 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`

### Message Guidelines

- Average message length: ~60 characters
- Keep first line concise and descriptive
- Use imperative mood ("Add feature" not "Added feature")


*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/add-command-or-workflow-documentation.md)
```

*Commit message example*

```text
chore: Remove Streamlit dashboard
```

*Commit message example*

```text
fix: 3 architecture fixes - market_id width, Redis fail-fast, timestamp format
```

*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/feature-development.md)
```

*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/database-migration.md)
```

*Commit message example*

```text
feat: add mework ECC bundle (.codex/agents/docs-researcher.toml)
```

*Commit message example*

```text
feat: add mework ECC bundle (.codex/agents/reviewer.toml)
```

*Commit message example*

```text
feat: add mework ECC bundle (.codex/agents/explorer.toml)
```

## Architecture

### Project Structure: Single Package

This project uses **hybrid** module organization.

### Configuration Files

- `api/Dockerfile`
- `docker-compose.yml`
- `hub/Dockerfile`
- `hub/package.json`

### Guidelines

- This project uses a hybrid organization
- Follow existing patterns when adding new code

## Code Style

### Language: Python

### Naming Conventions

| Element | Convention |
|---------|------------|
| Files | camelCase |
| Functions | camelCase |
| Classes | PascalCase |
| Constants | SCREAMING_SNAKE_CASE |

### Import Style: Mixed Style

### Export Style: Mixed Style


## Error Handling

### Error Handling Style: Try-Catch Blocks


*Standard error handling pattern*

```typescript
try {
  const result = await riskyOperation()
  return result
} catch (error) {
  console.error('Operation failed:', error)
  throw new Error('User-friendly message')
}
```

## Common Workflows

These workflows were detected from analyzing commit patterns.

### Feature Development

Standard feature implementation workflow

**Frequency**: ~29 times per month

**Steps**:
1. Add feature implementation
2. Add tests for feature
3. Update documentation

**Files typically involved**:
- `**/api/**`

**Example commit sequence**:
```
feat(api): Defensive Contrast Engine for batted-ball profile mismatches
feat(scripts): Enhanced training pipeline with multi-source data
chore: Remove Streamlit dashboard
```

### Add Command Or Workflow Documentation

Adds documentation for a new command or workflow to the system.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ describing the command or workflow.

**Files typically involved**:
- `.claude/commands/*.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ describing the command or workflow.
```

### Add Agent Configuration

Adds or updates agent configuration files for the Codex system.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update a .toml file in .codex/agents/ for the agent.

**Files typically involved**:
- `.codex/agents/*.toml`

**Example commit sequence**:
```
Create or update a .toml file in .codex/agents/ for the agent.
```

### Add Skill Documentation

Adds or updates SKILL.md documentation for a skill in both .agents and .claude directories.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update SKILL.md in .agents/skills/mework/
2. Create or update SKILL.md in .claude/skills/mework/

**Files typically involved**:
- `.agents/skills/mework/SKILL.md`
- `.claude/skills/mework/SKILL.md`

**Example commit sequence**:
```
Create or update SKILL.md in .agents/skills/mework/
Create or update SKILL.md in .claude/skills/mework/
```

### Add Ecc Tools Json

Adds or updates the ECC tools configuration file.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update .claude/ecc-tools.json with new tool definitions.

**Files typically involved**:
- `.claude/ecc-tools.json`

**Example commit sequence**:
```
Create or update .claude/ecc-tools.json with new tool definitions.
```

### Add Identity Json

Adds or updates the identity configuration for Claude.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update .claude/identity.json.

**Files typically involved**:
- `.claude/identity.json`

**Example commit sequence**:
```
Create or update .claude/identity.json.
```


## Best Practices

Based on analysis of the codebase, follow these practices:

### Do

- Use conventional commit format (feat:, fix:, etc.)
- Use camelCase for file names
- Prefer mixed exports

### Don't

- Don't write vague commit messages
- Don't deviate from established patterns without discussion

---

*This skill was auto-generated by [ECC Tools](https://ecc.tools). Review and customize as needed for your team.*

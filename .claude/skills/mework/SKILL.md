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

Follow these commit message conventions based on 69 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`

### Message Guidelines

- Average message length: ~59 characters
- Keep first line concise and descriptive
- Use imperative mood ("Add feature" not "Added feature")


*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/add-api-service-module.md)
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

### Database Migration

Database schema changes with migration files

**Frequency**: ~3 times per month

**Steps**:
1. Create migration file
2. Update schema definitions
3. Generate/update types

**Files typically involved**:
- `migrations/*`

**Example commit sequence**:
```
fix: 5 architecture refinements from CI bot review
feat(api): FastAPI bootstrap with async SQLAlchemy
fix: 3 architecture fixes - market_id width, Redis fail-fast, timestamp format
```

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
feat(api): FastAPI bootstrap with async SQLAlchemy
fix: 3 architecture fixes - market_id width, Redis fail-fast, timestamp format
feat(api): PyBaseball/Statcast integration
```

### Add Command Or Workflow Documentation

Adds or updates workflow documentation or command specs for mework ECC, typically as markdown files describing automated or manual workflows.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ describing the workflow or command.
2. Commit the file with a message referencing the workflow or command name.

**Files typically involved**:
- `.claude/commands/add-api-service-module.md`
- `.claude/commands/feature-development.md`
- `.claude/commands/database-migration.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ describing the workflow or command.
Commit the file with a message referencing the workflow or command name.
```

### Add Or Update Agent Skill Or Config

Adds or updates agent skill definitions, agent configuration files, or skill documentation for mework ECC and Codex agents.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update agent config files (.toml, .yaml) or skill documentation (SKILL.md) in the appropriate directory.
2. Commit the changes with a message referencing the agent or skill.

**Files typically involved**:
- `.codex/agents/docs-researcher.toml`
- `.codex/agents/reviewer.toml`
- `.codex/agents/explorer.toml`
- `.agents/skills/mework/agents/openai.yaml`
- `.agents/skills/mework/SKILL.md`
- `.claude/skills/mework/SKILL.md`

**Example commit sequence**:
```
Create or update agent config files (.toml, .yaml) or skill documentation (SKILL.md) in the appropriate directory.
Commit the changes with a message referencing the agent or skill.
```

### Add Or Update Ecc Core Config

Adds or updates core configuration or identity files for the mework ECC system.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update identity or tool config files in .claude/ (identity.json, ecc-tools.json).
2. Commit the changes.

**Files typically involved**:
- `.claude/identity.json`
- `.claude/ecc-tools.json`

**Example commit sequence**:
```
Create or update identity or tool config files in .claude/ (identity.json, ecc-tools.json).
Commit the changes.
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

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

Follow these commit message conventions based on 91 analyzed commits.

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
feat: add mework ECC bundle (.claude/commands/add-agent-configuration.md)
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
feat: add mework ECC bundle (.claude/commands/add-command-or-workflow-documentation.md)
```

*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/feature-development.md)
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

**Frequency**: ~30 times per month

**Steps**:
1. Add feature implementation
2. Add tests for feature
3. Update documentation

**Files typically involved**:
- `**/api/**`

**Example commit sequence**:
```
feat: add mework ECC bundle (.codex/agents/docs-researcher.toml)
feat: add mework ECC bundle (.codex/agents/reviewer.toml)
feat: add mework ECC bundle (.claude/homunculus/instincts/inherited/mework-instincts.yaml)
```

### Add Command Or Workflow Documentation

Adds documentation for a new command or workflow to the project.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ with the command or workflow documentation.

**Files typically involved**:
- `.claude/commands/add-command-or-workflow-documentation.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ with the command or workflow documentation.
```

### Add Api Service Module Documentation

Adds documentation for a new API service module.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ with the API service module documentation.

**Files typically involved**:
- `.claude/commands/add-api-service-module.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ with the API service module documentation.
```

### Add Feature Development Documentation

Adds documentation for feature development workflows.

**Frequency**: ~5 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ with feature development documentation.

**Files typically involved**:
- `.claude/commands/feature-development.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ with feature development documentation.
```

### Add Database Migration Documentation

Adds documentation for database migration workflows.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ with database migration documentation.

**Files typically involved**:
- `.claude/commands/database-migration.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ with database migration documentation.
```

### Add Agent Configuration

Adds or updates agent configuration files for various agents.

**Frequency**: ~5 times per month

**Steps**:
1. Create or update a .toml file in .codex/agents/ for the specific agent.

**Files typically involved**:
- `.codex/agents/docs-researcher.toml`
- `.codex/agents/reviewer.toml`
- `.codex/agents/explorer.toml`

**Example commit sequence**:
```
Create or update a .toml file in .codex/agents/ for the specific agent.
```

### Add Skill Documentation

Adds or updates documentation for a skill in the agents or Claude skills directories.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update SKILL.md in the appropriate skills directory.

**Files typically involved**:
- `.agents/skills/mework/SKILL.md`
- `.claude/skills/mework/SKILL.md`

**Example commit sequence**:
```
Create or update SKILL.md in the appropriate skills directory.
```

### Update Identity Or Tools Json

Updates identity or ECC tools configuration for Claude.

**Frequency**: ~4 times per month

**Steps**:
1. Edit .claude/identity.json or .claude/ecc-tools.json with new configuration.

**Files typically involved**:
- `.claude/identity.json`
- `.claude/ecc-tools.json`

**Example commit sequence**:
```
Edit .claude/identity.json or .claude/ecc-tools.json with new configuration.
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

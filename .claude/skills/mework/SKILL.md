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

Follow these commit message conventions based on 102 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`

### Message Guidelines

- Average message length: ~61 characters
- Keep first line concise and descriptive
- Use imperative mood ("Add feature" not "Added feature")


*Commit message example*

```text
feat: add mework ECC bundle (.claude/commands/add-api-service-module-documentation.md)
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
feat: add mework ECC bundle (.codex/agents/explorer.toml)
feat: add mework ECC bundle (.codex/agents/reviewer.toml)
feat: add mework ECC bundle (.codex/agents/docs-researcher.toml)
```

### Add Command Or Workflow Documentation

Adds documentation for a new command or workflow to the system.

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

**Frequency**: ~2 times per month

**Steps**:
1. Create or update a markdown file in .claude/commands/ with the API service module documentation.

**Files typically involved**:
- `.claude/commands/add-api-service-module-documentation.md`

**Example commit sequence**:
```
Create or update a markdown file in .claude/commands/ with the API service module documentation.
```

### Add Api Service Module

Adds a new API service module to the project.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update the .claude/commands/add-api-service-module.md file to document the new module.

**Files typically involved**:
- `.claude/commands/add-api-service-module.md`

**Example commit sequence**:
```
Create or update the .claude/commands/add-api-service-module.md file to document the new module.
```

### Add Feature Development Workflow

Documents or initiates a feature development workflow.

**Frequency**: ~5 times per month

**Steps**:
1. Create or update the .claude/commands/feature-development.md file.

**Files typically involved**:
- `.claude/commands/feature-development.md`

**Example commit sequence**:
```
Create or update the .claude/commands/feature-development.md file.
```

### Add Database Migration Workflow

Documents or initiates a database migration workflow.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update the .claude/commands/database-migration.md file.

**Files typically involved**:
- `.claude/commands/database-migration.md`

**Example commit sequence**:
```
Create or update the .claude/commands/database-migration.md file.
```

### Add Agent Configuration

Adds documentation for a new agent configuration.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update the .claude/commands/add-agent-configuration.md file.

**Files typically involved**:
- `.claude/commands/add-agent-configuration.md`

**Example commit sequence**:
```
Create or update the .claude/commands/add-agent-configuration.md file.
```

### Add Skill Documentation

Adds documentation for a new skill in the agents or Claude system.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update the SKILL.md file in either .agents/skills/mework/ or .claude/skills/mework/.

**Files typically involved**:
- `.agents/skills/mework/SKILL.md`
- `.claude/skills/mework/SKILL.md`

**Example commit sequence**:
```
Create or update the SKILL.md file in either .agents/skills/mework/ or .claude/skills/mework/.
```

### Add Codex Agent Configuration

Adds or updates agent configuration TOML files for Codex agents.

**Frequency**: ~5 times per month

**Steps**:
1. Create or update the .codex/agents/{agent}.toml file.

**Files typically involved**:
- `.codex/agents/docs-researcher.toml`
- `.codex/agents/reviewer.toml`
- `.codex/agents/explorer.toml`

**Example commit sequence**:
```
Create or update the .codex/agents/{agent}.toml file.
```

### Update Identity Or Tools Metadata

Updates project identity or ECC tools metadata.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update .claude/identity.json or .claude/ecc-tools.json.

**Files typically involved**:
- `.claude/identity.json`
- `.claude/ecc-tools.json`

**Example commit sequence**:
```
Create or update .claude/identity.json or .claude/ecc-tools.json.
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

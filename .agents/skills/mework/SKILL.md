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

Follow these commit message conventions based on 58 analyzed commits.

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

**Frequency**: ~5 times per month

**Steps**:
1. Create migration file
2. Update schema definitions
3. Generate/update types

**Files typically involved**:
- `migrations/*`

**Example commit sequence**:
```
fix: market ID split, betting_markets table, worker gating, fail-fast
fix: COALESCE for over/under odds, graceful degradation for missing API key
feat(hub): add ESPN public scoreboard fetcher
```

### Feature Development

Standard feature implementation workflow

**Frequency**: ~28 times per month

**Steps**:
1. Add feature implementation
2. Add tests for feature
3. Update documentation

**Files typically involved**:
- `**/api/**`

**Example commit sequence**:
```
feat(hub): add The Odds API fetcher module
feat(hub): add aggregator endpoint /api/slates/today
fix: rolling window rate limiter, sportsdata fetcher, Redis password encoding
```

### Add Api Service Module

Adds a new API service module or feature to the backend, often including analytics engines or new endpoints.

**Frequency**: ~4 times per month

**Steps**:
1. Create new module in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
2. Update api/services/predictor.py to integrate new logic (import, call, add response fields)
3. If needed, update or create corresponding router in api/routers/
4. Document or update requirements in api/requirements.txt if new dependencies are added

**Files typically involved**:
- `api/services/*.py`
- `api/services/predictor.py`

**Example commit sequence**:
```
Create new module in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
Update api/services/predictor.py to integrate new logic (import, call, add response fields)
If needed, update or create corresponding router in api/routers/
Document or update requirements in api/requirements.txt if new dependencies are added
```

### Database Migration And Indexing

Adds or modifies database tables, views, or indexes to support new features or improve performance.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update SQL migration file in db/init/ (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
2. Apply ALTER TABLE statements for schema changes (e.g., column type, NOT NULL constraints)
3. Add or update indexes for performance
4. Seed data if required (e.g., teams before games for FK constraints)

**Files typically involved**:
- `db/init/*.sql`

**Example commit sequence**:
```
Create or update SQL migration file in db/init/ (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
Apply ALTER TABLE statements for schema changes (e.g., column type, NOT NULL constraints)
Add or update indexes for performance
Seed data if required (e.g., teams before games for FK constraints)
```

### Add Fetcher Or Data Aggregator Module

Adds a new data fetcher or aggregator module to the hub for external APIs (e.g., ESPN, OddsAPI, SportsData), and integrates it into the data aggregation pipeline.

**Frequency**: ~3 times per month

**Steps**:
1. Create new fetcher in hub/src/fetchers/ (e.g., espn.js, oddsapi.js, sportsdata.js)
2. Update hub/src/routes/slates.js to include new fetcher in Promise.allSettled or aggregation logic
3. Update hub/src/server.js to register new routes or endpoints
4. If needed, update caching logic in hub/src/cache.js

**Files typically involved**:
- `hub/src/fetchers/*.js`
- `hub/src/routes/slates.js`
- `hub/src/server.js`

**Example commit sequence**:
```
Create new fetcher in hub/src/fetchers/ (e.g., espn.js, oddsapi.js, sportsdata.js)
Update hub/src/routes/slates.js to include new fetcher in Promise.allSettled or aggregation logic
Update hub/src/server.js to register new routes or endpoints
If needed, update caching logic in hub/src/cache.js
```

### Ml Training Pipeline Update

Implements or updates machine learning model training scripts and saves new model artifacts for prediction.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update scripts/train_model.py with new data sources, feature engineering, or model logic
2. Save trained model artifacts to api/models/ (e.g., prop_model_v1.json, hr_model_v1.json, xbh_model_v1.json)
3. If needed, update dependencies in api/requirements.txt

**Files typically involved**:
- `scripts/train_model.py`
- `api/models/*.json`

**Example commit sequence**:
```
Create or update scripts/train_model.py with new data sources, feature engineering, or model logic
Save trained model artifacts to api/models/ (e.g., prop_model_v1.json, hr_model_v1.json, xbh_model_v1.json)
If needed, update dependencies in api/requirements.txt
```

### Ecc Command Or Skill Bundle Addition

Adds or updates ECC (external cognitive component) command or skill bundles, typically for agent or automation frameworks.

**Frequency**: ~3 times per month

**Steps**:
1. Add or update .claude/commands/*.md for new commands
2. Add or update .claude/skills/mework/SKILL.md or .agents/skills/mework/SKILL.md for new skills
3. Add or update .codex/agents/*.toml for agent configuration
4. Update .claude/ecc-tools.json or .claude/identity.json if needed

**Files typically involved**:
- `.claude/commands/*.md`
- `.claude/skills/mework/SKILL.md`
- `.agents/skills/mework/SKILL.md`
- `.codex/agents/*.toml`
- `.claude/ecc-tools.json`
- `.claude/identity.json`

**Example commit sequence**:
```
Add or update .claude/commands/*.md for new commands
Add or update .claude/skills/mework/SKILL.md or .agents/skills/mework/SKILL.md for new skills
Add or update .codex/agents/*.toml for agent configuration
Update .claude/ecc-tools.json or .claude/identity.json if needed
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

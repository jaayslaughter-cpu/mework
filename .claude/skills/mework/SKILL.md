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

Follow these commit message conventions based on 47 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`

### Message Guidelines

- Average message length: ~58 characters
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
feat: add mework ECC bundle (.claude/homunculus/instincts/inherited/mework-instincts.yaml)
```

*Commit message example*

```text
feat: add mework ECC bundle (.codex/agents/docs-researcher.toml)
```

*Commit message example*

```text
feat: add mework ECC bundle (.codex/agents/reviewer.toml)
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

**Frequency**: ~6 times per month

**Steps**:
1. Create migration file
2. Update schema definitions
3. Generate/update types

**Files typically involved**:
- `migrations/*`

**Example commit sequence**:
```
fix: CI bot feedback - placeholder files, security, and schema fixes
fix: change npm ci to npm install, reorder USER before EXPOSE
fix: pin dependencies, fix Streamlit placeholder, add hub package.json
```

### Feature Development

Standard feature implementation workflow

**Frequency**: ~26 times per month

**Steps**:
1. Add feature implementation
2. Add tests for feature
3. Update documentation

**Files typically involved**:
- `**/api/**`

**Example commit sequence**:
```
feat(infra): scaffold Docker Compose v3.8 full stack orchestration
Initial commit
fix: CI bot feedback - placeholder files, security, and schema fixes
```

### Add Api Service Module

Adds a new analytics or logic module to the API service and integrates it with the main predictor logic.

**Frequency**: ~3 times per month

**Steps**:
1. Create new module file in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
2. Update api/services/predictor.py to import and integrate the new module
3. Add new parameters, context, or flags to the predictor response

**Files typically involved**:
- `api/services/predictor.py`
- `api/services/*.py`

**Example commit sequence**:
```
Create new module file in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
Update api/services/predictor.py to import and integrate the new module
Add new parameters, context, or flags to the predictor response
```

### Database Schema Migration

Adds or alters database tables, views, or indexes to support new features or improve performance.

**Frequency**: ~6 times per month

**Steps**:
1. Create or update SQL migration file in db/init/ (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
2. Add ALTER TABLE statements for idempotency and backward compatibility
3. Add or update indexes and constraints as needed

**Files typically involved**:
- `db/init/*.sql`

**Example commit sequence**:
```
Create or update SQL migration file in db/init/ (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
Add ALTER TABLE statements for idempotency and backward compatibility
Add or update indexes and constraints as needed
```

### Add Api Endpoint

Implements a new FastAPI router endpoint and integrates it into the main API application.

**Frequency**: ~2 times per month

**Steps**:
1. Create new router file in api/routers/ (e.g., mlb_data.py, predictions.py)
2. Update api/main.py to include the new router
3. Update api/requirements.txt if new dependencies are needed

**Files typically involved**:
- `api/routers/*.py`
- `api/main.py`
- `api/requirements.txt`

**Example commit sequence**:
```
Create new router file in api/routers/ (e.g., mlb_data.py, predictions.py)
Update api/main.py to include the new router
Update api/requirements.txt if new dependencies are needed
```

### Add Hub Fetcher Module

Adds a new data fetcher for external APIs to the Node.js hub, with caching and rate limiting.

**Frequency**: ~3 times per month

**Steps**:
1. Create new fetcher file in hub/src/fetchers/ (e.g., espn.js, oddsapi.js, sportsdata.js)
2. Update hub/src/routes/slates.js to include new fetcher in aggregation
3. Update hub/src/server.js if new routes are exposed

**Files typically involved**:
- `hub/src/fetchers/*.js`
- `hub/src/routes/slates.js`
- `hub/src/server.js`

**Example commit sequence**:
```
Create new fetcher file in hub/src/fetchers/ (e.g., espn.js, oddsapi.js, sportsdata.js)
Update hub/src/routes/slates.js to include new fetcher in aggregation
Update hub/src/server.js if new routes are exposed
```

### Architecture Fix And Refinement

Applies architecture-level fixes, constraint updates, or performance/security improvements across multiple files.

**Frequency**: ~5 times per month

**Steps**:
1. Update SQL migration files for constraints or indexes
2. Update Node.js hub files for caching, rate limiting, or fetcher logic
3. Update API files for response format or dependency pinning

**Files typically involved**:
- `db/init/*.sql`
- `hub/src/*.js`
- `api/*.py`

**Example commit sequence**:
```
Update SQL migration files for constraints or indexes
Update Node.js hub files for caching, rate limiting, or fetcher logic
Update API files for response format or dependency pinning
```

### Ml Training Pipeline Update

Implements or improves the model training pipeline and saves new model artifacts.

**Frequency**: ~2 times per month

**Steps**:
1. Update or create scripts/train_model.py with new data sources or features
2. Save new model artifacts to api/models/
3. Document or log feature importances and model performance

**Files typically involved**:
- `scripts/train_model.py`
- `api/models/*.json`

**Example commit sequence**:
```
Update or create scripts/train_model.py with new data sources or features
Save new model artifacts to api/models/
Document or log feature importances and model performance
```

### Docker Compose Orchestration Update

Updates Docker Compose orchestration and service configuration for the full stack.

**Frequency**: ~2 times per month

**Steps**:
1. Update docker-compose.yml with new or changed services
2. Update service Dockerfiles as needed
3. Update .env.example for new environment variables

**Files typically involved**:
- `docker-compose.yml`
- `api/Dockerfile`
- `dashboard/Dockerfile`
- `hub/Dockerfile`
- `.env.example`

**Example commit sequence**:
```
Update docker-compose.yml with new or changed services
Update service Dockerfiles as needed
Update .env.example for new environment variables
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

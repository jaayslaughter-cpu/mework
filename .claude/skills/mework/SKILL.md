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

Follow these commit message conventions based on 33 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`
- `chore`

### Message Guidelines

- Average message length: ~57 characters
- Keep first line concise and descriptive
- Use imperative mood ("Add feature" not "Added feature")


*Commit message example*

```text
chore: Remove Streamlit dashboard
```

*Commit message example*

```text
feat(scripts): Enhanced training pipeline with multi-source data
```

*Commit message example*

```text
fix: 3 architecture fixes - market_id width, Redis fail-fast, timestamp format
```

*Commit message example*

```text
feat(api): Defensive Contrast Engine for batted-ball profile mismatches
```

*Commit message example*

```text
feat(api): Usage Vacuum Engine for lineup opportunity detection
```

*Commit message example*

```text
feat(scripts): XGBoost training pipeline for prop prediction
```

*Commit message example*

```text
feat(api): Fatigue Logic Engine for projection adjustments
```

*Commit message example*

```text
feat(db): Bets log table, evaluation views, and performance indexes
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
| Files | snake_case |
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

**Frequency**: ~8 times per month

**Steps**:
1. Create migration file
2. Update schema definitions
3. Generate/update types

**Example commit sequence**:
```
fix: CI bot feedback - placeholder files, security, and schema fixes
fix: change npm ci to npm install, reorder USER before EXPOSE
fix: pin dependencies, fix Streamlit placeholder, add hub package.json
```

### Feature Development

Standard feature implementation workflow

**Frequency**: ~25 times per month

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

Adds a new analytics or ML engine module to the Python FastAPI backend, integrating it into the main prediction pipeline.

**Frequency**: ~3 times per month

**Steps**:
1. Create new service module in api/services (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
2. Implement core analytics logic in the new service file
3. Update api/services/predictor.py to import and integrate the new module
4. Add new parameters/context to evaluate_edge or related functions
5. Update API response to include new analytics outputs

**Files typically involved**:
- `api/services/predictor.py`
- `api/services/*.py`

**Example commit sequence**:
```
Create new service module in api/services (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
Implement core analytics logic in the new service file
Update api/services/predictor.py to import and integrate the new module
Add new parameters/context to evaluate_edge or related functions
Update API response to include new analytics outputs
```

### Add Api Endpoint

Adds a new FastAPI route and handler, integrating with service modules and updating requirements if needed.

**Frequency**: ~2 times per month

**Steps**:
1. Create new router file in api/routers (e.g., predictions.py, mlb_data.py)
2. Implement endpoint logic using FastAPI and Pydantic models
3. Update api/main.py to include the new router
4. Update api/requirements.txt if new dependencies are needed

**Files typically involved**:
- `api/main.py`
- `api/routers/*.py`
- `api/requirements.txt`

**Example commit sequence**:
```
Create new router file in api/routers (e.g., predictions.py, mlb_data.py)
Implement endpoint logic using FastAPI and Pydantic models
Update api/main.py to include the new router
Update api/requirements.txt if new dependencies are needed
```

### Add Db Table Or Migration

Adds a new SQL table, view, index, or alters schema via migration scripts for the analytics platform.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update SQL migration file in db/init (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
2. Add CREATE TABLE, CREATE VIEW, CREATE INDEX, or ALTER TABLE statements as needed
3. Ensure idempotency with DROP CONSTRAINT IF EXISTS or ALTER COLUMN
4. Reference new/changed tables in application code if required

**Files typically involved**:
- `db/init/*.sql`

**Example commit sequence**:
```
Create or update SQL migration file in db/init (e.g., 01_core_reference.sql, 02_projection_market_layer.sql, 03_bets_log_views_indexes.sql)
Add CREATE TABLE, CREATE VIEW, CREATE INDEX, or ALTER TABLE statements as needed
Ensure idempotency with DROP CONSTRAINT IF EXISTS or ALTER COLUMN
Reference new/changed tables in application code if required
```

### Add Hub Fetcher And Aggregator

Adds a new data fetcher module to the Node.js Hub, integrates it into the aggregator endpoint, and updates the polling worker if needed.

**Frequency**: ~3 times per month

**Steps**:
1. Create new fetcher in hub/src/fetchers (e.g., espn.js, oddsapi.js, sportsdata.js)
2. Implement caching and rate limiting in the fetcher
3. Update hub/src/routes/slates.js to include new fetcher in aggregation
4. Update hub/src/sync.js if background polling is required
5. Update hub/src/server.js to register new routes or workers

**Files typically involved**:
- `hub/src/fetchers/*.js`
- `hub/src/routes/slates.js`
- `hub/src/sync.js`
- `hub/src/server.js`

**Example commit sequence**:
```
Create new fetcher in hub/src/fetchers (e.g., espn.js, oddsapi.js, sportsdata.js)
Implement caching and rate limiting in the fetcher
Update hub/src/routes/slates.js to include new fetcher in aggregation
Update hub/src/sync.js if background polling is required
Update hub/src/server.js to register new routes or workers
```

### Add Ml Training Pipeline

Adds or enhances a machine learning training script, saves new model artifacts, and updates the API to use new models.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update scripts/train_model.py with new data sources, features, or model logic
2. Save trained model artifacts to api/models/*.json
3. Update api/services/predictor.py or related code to use new models if necessary

**Files typically involved**:
- `scripts/train_model.py`
- `api/models/*.json`

**Example commit sequence**:
```
Create or update scripts/train_model.py with new data sources, features, or model logic
Save trained model artifacts to api/models/*.json
Update api/services/predictor.py or related code to use new models if necessary
```

### Infrastructure Orchestration Update

Updates Docker Compose, Dockerfiles, and environment configuration for orchestration, service renaming, or security improvements.

**Frequency**: ~2 times per month

**Steps**:
1. Update docker-compose.yml with new/removed services or environment variables
2. Update Dockerfiles for services (hub, api, dashboard) as needed
3. Update .env.example and .gitignore if new env vars are required
4. Update service ports or network configuration

**Files typically involved**:
- `docker-compose.yml`
- `hub/Dockerfile`
- `api/Dockerfile`
- `dashboard/Dockerfile`
- `.env.example`
- `.gitignore`

**Example commit sequence**:
```
Update docker-compose.yml with new/removed services or environment variables
Update Dockerfiles for services (hub, api, dashboard) as needed
Update .env.example and .gitignore if new env vars are required
Update service ports or network configuration
```

### Architecture Fix And Refinement

Applies a batch of architecture, bug, or CI-driven fixes across multiple files, especially after bot/code review.

**Frequency**: ~3 times per month

**Steps**:
1. Update SQL schema files for constraint or column fixes
2. Patch caching, rate limiting, or polling logic in hub/src
3. Fix API response formats or timestamp handling
4. Apply security or environment variable handling improvements

**Files typically involved**:
- `db/init/*.sql`
- `hub/src/*.js`
- `api/main.py`

**Example commit sequence**:
```
Update SQL schema files for constraint or column fixes
Patch caching, rate limiting, or polling logic in hub/src
Fix API response formats or timestamp handling
Apply security or environment variable handling improvements
```


## Best Practices

Based on analysis of the codebase, follow these practices:

### Do

- Use conventional commit format (feat:, fix:, etc.)
- Use snake_case for file names
- Prefer mixed exports

### Don't

- Don't write vague commit messages
- Don't deviate from established patterns without discussion

---

*This skill was auto-generated by [ECC Tools](https://ecc.tools). Review and customize as needed for your team.*

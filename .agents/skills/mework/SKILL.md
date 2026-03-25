---
name: mework-conventions
description: Development conventions and patterns for mework. Python project with conventional commits.
---

# Mework Conventions

> Generated from [jaayslaughter-cpu/mework](https://github.com/jaayslaughter-cpu/mework) on 2026-03-20

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

Follow these commit message conventions based on 55 analyzed commits.

### Commit Style: Conventional Commits

### Prefixes Used

- `feat`
- `fix`
- `chore`

### Message Guidelines

- Average message length: ~58 characters
- Keep first line concise and descriptive
- Use imperative mood ("Add feature" not "Added feature")


*Commit message example*

```text
feat: PropIQ complete implementation [fix/sync-worker-bugs]
```

*Commit message example*

```text
fix(docker): Add REDIS_PASSWORD env var to hub service
```

*Commit message example*

```text
chore: Tighten CORS origins, remove wildcard
```

*Commit message example*

```text
Merge pull request #25 from jaayslaughter-cpu/ticket-6.4-defensive-contrast
```

*Commit message example*

```text
Merge branch 'main' into ticket-6.4-defensive-contrast
```

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
feat(api): Defensive Contrast Engine for batted-ball profile mismatches
```

## Architecture

### Project Structure: Single Package

This project uses **hybrid** module organization.

### Configuration Files

- `.github/workflows/npm-publish-github-packages.yml`
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

**Frequency**: ~10 times per month

**Steps**:
1. Create migration file
2. Update schema definitions
3. Generate/update types

**Example commit sequence**:
```
fix: CI bot feedback - placeholder files, security, and schema fixes
Merge pull request #4 from jaayslaughter-cpu/ticket-1.4-bets-log-views-indexes
fix: change npm ci to npm install, reorder USER before EXPOSE
```

### Feature Development

Standard feature implementation workflow

**Frequency**: ~17 times per month

**Steps**:
1. Add feature implementation
2. Add tests for feature
3. Update documentation

**Files typically involved**:
- `**/api/**`

**Example commit sequence**:
```
fix: CI bot feedback - placeholder files, security, and schema fixes
Merge pull request #4 from jaayslaughter-cpu/ticket-1.4-bets-log-views-indexes
fix: change npm ci to npm install, reorder USER before EXPOSE
```

### Refactoring

Code refactoring and cleanup workflow

**Frequency**: ~2 times per month

**Steps**:
1. Ensure tests pass before refactor
2. Refactor code structure
3. Verify tests still pass

**Files typically involved**:
- `src/**/*`

**Example commit sequence**:
```
fix: 8 architecture improvements from CI bot review
fix: 5 architecture refinements from CI bot review
feat(api): FastAPI bootstrap with async SQLAlchemy
```

### Add Or Update Database Table Or Schema

Adds or updates a database table, view, or index, often for new features or analytics. Includes SQL migration files and sometimes updates to related backend code.

**Frequency**: ~4 times per month

**Steps**:
1. Create or update SQL migration file in db/init/*.sql
2. Sometimes update related backend code (e.g., api/database.py, hub/src/sync.js) to use new/changed tables
3. Commit migration and related code

**Files typically involved**:
- `db/init/01_core_reference.sql`
- `db/init/02_projection_market_layer.sql`
- `db/init/03_bets_log_views_indexes.sql`

**Example commit sequence**:
```
Create or update SQL migration file in db/init/*.sql
Sometimes update related backend code (e.g., api/database.py, hub/src/sync.js) to use new/changed tables
Commit migration and related code
```

### Add Or Enhance Api Endpoint

Adds or updates FastAPI endpoints, including new routers, services, and sometimes model or requirements updates.

**Frequency**: ~3 times per month

**Steps**:
1. Create or update api/routers/*.py for endpoint logic
2. Create or update api/services/*.py for business logic
3. Update api/main.py to register new routers
4. Update api/requirements.txt if new dependencies are needed
5. Commit all related files

**Files typically involved**:
- `api/routers/*.py`
- `api/services/*.py`
- `api/main.py`
- `api/requirements.txt`

**Example commit sequence**:
```
Create or update api/routers/*.py for endpoint logic
Create or update api/services/*.py for business logic
Update api/main.py to register new routers
Update api/requirements.txt if new dependencies are needed
Commit all related files
```

### Add Or Train Ml Model

Adds new ML models, training scripts, and updates model artifacts for predictions.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update scripts/train_model.py for training logic
2. Generate or update model artifact files in api/models/*.json
3. Update api/services/predictor.py to use new models if needed
4. Update api/requirements.txt if new ML dependencies are needed
5. Commit all related files

**Files typically involved**:
- `scripts/train_model.py`
- `api/models/*.json`
- `api/services/predictor.py`
- `api/requirements.txt`

**Example commit sequence**:
```
Create or update scripts/train_model.py for training logic
Generate or update model artifact files in api/models/*.json
Update api/services/predictor.py to use new models if needed
Update api/requirements.txt if new ML dependencies are needed
Commit all related files
```

### Add Or Enhance Hub Sync Worker

Implements or updates the Node.js hub's background sync worker for polling APIs and syncing betting markets.

**Frequency**: ~2 times per month

**Steps**:
1. Update or create hub/src/sync.js for polling logic
2. Update related fetchers (hub/src/fetchers/*.js) for new data sources
3. Update hub/src/server.js to integrate sync worker
4. Update docker-compose.yml or .env.example if new env vars are needed
5. Commit all related files

**Files typically involved**:
- `hub/src/sync.js`
- `hub/src/fetchers/*.js`
- `hub/src/server.js`
- `docker-compose.yml`
- `.env.example`

**Example commit sequence**:
```
Update or create hub/src/sync.js for polling logic
Update related fetchers (hub/src/fetchers/*.js) for new data sources
Update hub/src/server.js to integrate sync worker
Update docker-compose.yml or .env.example if new env vars are needed
Commit all related files
```

### Add Or Enhance Dashboard Feature

Adds or updates the Streamlit dashboard, including app logic, requirements, and Dockerfile.

**Frequency**: ~2 times per month

**Steps**:
1. Create or update dashboard/app.py for Streamlit UI
2. Update dashboard/requirements.txt for new dependencies
3. Update dashboard/Dockerfile if needed
4. Update docker-compose.yml if dashboard service changes
5. Commit all related files

**Files typically involved**:
- `dashboard/app.py`
- `dashboard/requirements.txt`
- `dashboard/Dockerfile`
- `docker-compose.yml`

**Example commit sequence**:
```
Create or update dashboard/app.py for Streamlit UI
Update dashboard/requirements.txt for new dependencies
Update dashboard/Dockerfile if needed
Update docker-compose.yml if dashboard service changes
Commit all related files
```

### Add Or Update Docker Orchestration

Updates Docker Compose and service Dockerfiles to orchestrate multi-service deployments, often when adding new services or changing environment variables.

**Frequency**: ~2 times per month

**Steps**:
1. Update docker-compose.yml with new/changed services or env vars
2. Update service Dockerfiles as needed (api/Dockerfile, dashboard/Dockerfile, hub/Dockerfile)
3. Update .env.example for new environment variables
4. Commit all related files

**Files typically involved**:
- `docker-compose.yml`
- `api/Dockerfile`
- `dashboard/Dockerfile`
- `hub/Dockerfile`
- `.env.example`

**Example commit sequence**:
```
Update docker-compose.yml with new/changed services or env vars
Update service Dockerfiles as needed (api/Dockerfile, dashboard/Dockerfile, hub/Dockerfile)
Update .env.example for new environment variables
Commit all related files
```

### Add Or Update Backend Service Logic

Implements or enhances backend service logic, especially in api/services/*.py, often for analytics engines (fatigue, usage vacuums, defensive contrast, etc).

**Frequency**: ~3 times per month

**Steps**:
1. Create or update api/services/*.py with new logic
2. Update api/services/predictor.py to integrate new logic
3. Update or create tests if needed
4. Commit all related files

**Files typically involved**:
- `api/services/*.py`

**Example commit sequence**:
```
Create or update api/services/*.py with new logic
Update api/services/predictor.py to integrate new logic
Update or create tests if needed
Commit all related files
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

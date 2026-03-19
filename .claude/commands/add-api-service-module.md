---
name: add-api-service-module
description: Workflow command scaffold for add-api-service-module in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /add-api-service-module

Use this workflow when working on **add-api-service-module** in `mework`.

## Goal

Adds a new API service module or feature to the backend, often including analytics engines or new endpoints.

## Common Files

- `api/services/*.py`
- `api/services/predictor.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Create new module in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
- Update api/services/predictor.py to integrate new logic (import, call, add response fields)
- If needed, update or create corresponding router in api/routers/
- Document or update requirements in api/requirements.txt if new dependencies are added

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
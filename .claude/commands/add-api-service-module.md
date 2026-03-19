---
name: add-api-service-module
description: Workflow command scaffold for add-api-service-module in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /add-api-service-module

Use this workflow when working on **add-api-service-module** in `mework`.

## Goal

Adds a new analytics or logic module to the API service and integrates it with the main predictor logic.

## Common Files

- `api/services/predictor.py`
- `api/services/*.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Create new module file in api/services/ (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
- Update api/services/predictor.py to import and integrate the new module
- Add new parameters, context, or flags to the predictor response

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
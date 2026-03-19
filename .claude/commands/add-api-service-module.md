---
name: add-api-service-module
description: Workflow command scaffold for add-api-service-module in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /add-api-service-module

Use this workflow when working on **add-api-service-module** in `mework`.

## Goal

Adds a new analytics or ML engine module to the Python FastAPI backend, integrating it into the main prediction pipeline.

## Common Files

- `api/services/predictor.py`
- `api/services/*.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Create new service module in api/services (e.g., fatigue_logic.py, usage_vacuums.py, defensive_contrast.py)
- Implement core analytics logic in the new service file
- Update api/services/predictor.py to import and integrate the new module
- Add new parameters/context to evaluate_edge or related functions
- Update API response to include new analytics outputs

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
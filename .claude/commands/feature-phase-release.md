---
name: feature-phase-release
description: Workflow command scaffold for feature-phase-release in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-release

Use this workflow when working on **feature-phase-release** in `mework`.

## Goal

Implements a new feature or major enhancement as part of a named 'Phase' (e.g., Phase 105: xbh_per_game + SLG as TB/power prop features), typically involving coordinated changes across multiple core pipeline files.

## Common Files

- `fangraphs_layer.py`
- `prop_enrichment_layer.py`
- `calibration_layer.py`
- `sportsbook_reference_layer.py`
- `line_comparator.py`
- `base_rate_model.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Edit or add logic in one or more of: fangraphs_layer.py, prop_enrichment_layer.py, calibration_layer.py, sportsbook_reference_layer.py, line_comparator.py, base_rate_model.py
- Update or wire up orchestration logic in tasklets.py
- Sometimes update orchestrator.py for API or pipeline wiring
- Commit with a 'Phase XX:' message summarizing the change

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
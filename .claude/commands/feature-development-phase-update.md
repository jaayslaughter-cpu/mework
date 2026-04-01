---
name: feature-development-phase-update
description: Workflow command scaffold for feature-development-phase-update in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-development-phase-update

Use this workflow when working on **feature-development-phase-update** in `mework`.

## Goal

Implements a new feature or phase, typically involving updates to core processing logic and feature vectors.

## Common Files

- `fangraphs_layer.py`
- `prop_enrichment_layer.py`
- `sportsbook_reference_layer.py`
- `calibration_layer.py`
- `DiscordAlertService.py`
- `tasklets.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Edit or add logic in one or more of: fangraphs_layer.py, prop_enrichment_layer.py, sportsbook_reference_layer.py, calibration_layer.py, DiscordAlertService.py
- Update tasklets.py to wire in new logic, features, or bugfixes
- Commit changes with a message referencing the phase/feature

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
---
name: feature-phase-development-or-bugfix
description: Workflow command scaffold for feature-phase-development-or-bugfix in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-development-or-bugfix

Use this workflow when working on **feature-phase-development-or-bugfix** in `mework`.

## Goal

Implements a new feature, bugfix, or pipeline fix in the main codebase, typically involving updates to core Python files related to data processing or enrichment.

## Common Files

- `fangraphs_layer.py`
- `prop_enrichment_layer.py`
- `tasklets.py`
- `sportsbook_reference_layer.py`
- `line_comparator.py`
- `calibration_layer.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Modify one or more of the following Python files: fangraphs_layer.py, prop_enrichment_layer.py, tasklets.py, sportsbook_reference_layer.py, line_comparator.py, calibration_layer.py, DiscordAlertService.py
- Commit changes with a descriptive message indicating the phase, feature, or fix
- Optionally merge from main branch before or after changes
- Merge pull request into main

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
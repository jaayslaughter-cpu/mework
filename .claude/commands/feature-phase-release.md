---
name: feature-phase-release
description: Workflow command scaffold for feature-phase-release in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-release

Use this workflow when working on **feature-phase-release** in `mework`.

## Goal

Implements a new feature or phase, often labeled as 'Phase XX', involving coordinated changes across core logic, enrichment, and orchestration layers.

## Common Files

- `tasklets.py`
- `prop_enrichment_layer.py`
- `fangraphs_layer.py`
- `orchestrator.py`
- `calibration_layer.py`
- `sportsbook_reference_layer.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Update or add logic in tasklets.py for new feature/phase.
- Update or add logic in prop_enrichment_layer.py and/or fangraphs_layer.py for data enrichment or feature vectors.
- Update orchestrator.py to wire up new feature or endpoint.
- Optionally update calibration_layer.py or sportsbook_reference_layer.py if odds or calibration logic is involved.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
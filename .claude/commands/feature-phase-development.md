---
name: feature-phase-development
description: Workflow command scaffold for feature-phase-development in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-development

Use this workflow when working on **feature-phase-development** in `mework`.

## Goal

Implements a new feature phase, typically involving multiple related enhancements or fixes across the pipeline. Each phase is tracked with a number and usually includes enrichment, tasklets, and sometimes other layers.

## Common Files

- `fangraphs_layer.py`
- `prop_enrichment_layer.py`
- `tasklets.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Edit or enhance fangraphs_layer.py and/or prop_enrichment_layer.py to add new features or data sources.
- Update tasklets.py to wire in new logic or processing steps.
- Optionally modify related files (e.g., calibration_layer.py, line_comparator.py) if the phase requires.
- Commit with a message referencing the phase number and a summary of changes.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.